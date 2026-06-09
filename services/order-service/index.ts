import express, { Request, Response } from 'express';
import { Pool } from 'pg';
import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import { v4 as uuidv4 } from 'uuid';
import { log, retry, updateSagaState, isDuplicateEvent } from '../shared-utils';

const PORT = process.env.PORT || 3001;
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'kafka:9092').split(',');
const DB_HOST = process.env.DB_HOST || 'postgres';
const DB_USER = process.env.DB_USER || 'postgres';
const DB_PASSWORD = process.env.DB_PASSWORD || 'postgres';
const DB_NAME = process.env.DB_NAME || 'orders';
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';

const pool = new Pool({
  host: DB_HOST,
  user: DB_USER,
  password: DB_PASSWORD,
  database: DB_NAME,
});

const redisClient = createClient({ url: REDIS_URL });

const kafka = new Kafka({
  clientId: 'order-service',
  brokers: KAFKA_BROKERS,
});

const consumer = kafka.consumer({ groupId: 'order-service-group' });

const app = express();
app.use(express.json());

// REST Endpoint: POST /api/orders
app.post('/api/orders', async (req: Request, res: Response) => {
  const { customerId, productId, quantity, price } = req.body;

  // Validate fields (Requirement 2)
  if (
    typeof customerId !== 'string' || !customerId ||
    typeof productId !== 'string' || !productId ||
    typeof quantity !== 'number' || quantity <= 0 || !Number.isInteger(quantity) ||
    typeof price !== 'number' || price <= 0
  ) {
    log({
      level: 'warn',
      service: 'order-service',
      message: 'Invalid request payload for order creation',
      timestamp: new Date().toISOString(),
    });
    return res.status(400).json({ error: 'Validation failed' });
  }

  const orderId = uuidv4();
  const eventId = uuidv4();
  const orderCreatedEvent = {
    eventType: 'OrderCreated',
    eventId,
    orderId,
    customerId,
    productId,
    quantity,
    price,
    timestamp: new Date().toISOString(),
  };

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Dual-write order state (Requirement 3)
    await client.query(
      `INSERT INTO orders (id, customer_id, product_id, quantity, price, status)
       VALUES ($1, $2, $3, $4, $5, 'PENDING')`,
      [orderId, customerId, productId, quantity, price]
    );

    // Write to outbox (Requirement 3)
    await client.query(
      `INSERT INTO orders_outbox (event_id, aggregate_id, topic, payload, processed)
       VALUES ($1, $2, 'orders', $3, FALSE)`,
      [eventId, orderId, JSON.stringify(orderCreatedEvent)]
    );

    await client.query('COMMIT');

    // Initialize Saga state
    await updateSagaState(
      redisClient as any,
      orderId,
      'order',
      'OrderCreated',
      'SUCCESS',
      'PENDING'
    );

    log({
      level: 'info',
      service: 'order-service',
      message: `Created order ${orderId} in PENDING state`,
      timestamp: new Date().toISOString(),
      orderId,
    });

    return res.status(202).json({
      orderId,
      status: 'PENDING',
    });
  } catch (err: any) {
    await client.query('ROLLBACK');
    log({
      level: 'error',
      service: 'order-service',
      message: `Failed to create order: ${err.message}`,
      timestamp: new Date().toISOString(),
    });
    return res.status(500).json({ error: 'Internal server error' });
  } finally {
    client.release();
  }
});

// GET /api/orders (Healthcheck support)
app.get('/api/orders', async (req: Request, res: Response) => {
  try {
    const result = await pool.query('SELECT * FROM orders LIMIT 10');
    return res.status(200).json(result.rows);
  } catch (err: any) {
    return res.status(500).json({ error: err.message });
  }
});

// GET /api/sagas/:orderId
app.get('/api/sagas/:orderId', async (req: Request, res: Response) => {
  const { orderId } = req.params;
  const sagaKey = `saga:${orderId}`;

  try {
    const data = await redisClient.hGetAll(sagaKey);
    if (!data || Object.keys(data).length === 0) {
      return res.status(404).json({ error: 'Saga not found' });
    }

    return res.status(200).json({
      orderId,
      status: data.status,
      history: JSON.parse(data.history || '[]'),
    });
  } catch (err: any) {
    log({
      level: 'error',
      service: 'order-service',
      message: `Failed to fetch saga ${orderId}: ${err.message}`,
      timestamp: new Date().toISOString(),
      orderId,
    });
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/simulate/failure
app.post('/api/simulate/failure', async (req: Request, res: Response) => {
  const { service, failureRate } = req.body;

  if (service !== 'payment') {
    return res.status(400).json({ error: "Only 'payment' service is supported for now." });
  }

  const rate = parseFloat(failureRate);
  if (isNaN(rate) || rate < 0.0 || rate > 1.0) {
    return res.status(400).json({ error: 'failureRate must be a number between 0.0 and 1.0.' });
  }

  try {
    await redisClient.set('payment:failureRate', rate.toString());
    log({
      level: 'info',
      service: 'order-service',
      message: `Set payment failure rate simulation to ${rate}`,
      timestamp: new Date().toISOString(),
    });
    return res.status(200).json({
      service: 'payment',
      failureRate: rate,
      updated: true,
    });
  } catch (err: any) {
    log({
      level: 'error',
      service: 'order-service',
      message: `Failed to update simulation in Redis: ${err.message}`,
      timestamp: new Date().toISOString(),
    });
    return res.status(500).json({ error: 'Failed to write setting to cache.' });
  }
});

// Event processing logic
async function processEvent(topic: string, messageValue: string) {
  let event: any;
  try {
    event = JSON.parse(messageValue);
  } catch (err) {
    log({
      level: 'error',
      service: 'order-service',
      message: `Failed to parse event JSON: ${messageValue}`,
      timestamp: new Date().toISOString(),
    });
    return;
  }

  const { eventType, eventId, orderId } = event;
  if (!eventId || !orderId) return;

  // Idempotency check
  const isDuplicate = await isDuplicateEvent(redisClient as any, 'order-service', eventId);
  if (isDuplicate) {
    log({
      level: 'info',
      service: 'order-service',
      message: `Duplicate event ignored: eventId=${eventId}, topic=${topic}`,
      timestamp: new Date().toISOString(),
      orderId,
    });
    return;
  }

  log({
    level: 'info',
    service: 'order-service',
    message: `Processing event ${eventType} (topic: ${topic})`,
    timestamp: new Date().toISOString(),
    orderId,
  });

  try {
    if (eventType === 'PaymentProcessed') {
      await pool.query("UPDATE orders SET status = 'COMPLETED', updated_at = NOW() WHERE id = $1", [orderId]);
      await updateSagaState(redisClient as any, orderId, 'order', eventType, 'SUCCESS', 'COMPLETED');
    } else if (eventType === 'PaymentFailed') {
      await pool.query("UPDATE orders SET status = 'FAILED', updated_at = NOW() WHERE id = $1", [orderId]);
      await updateSagaState(redisClient as any, orderId, 'order', eventType, 'FAILURE', 'COMPENSATING');
    } else if (eventType === 'InventoryFailed') {
      await pool.query("UPDATE orders SET status = 'FAILED', updated_at = NOW() WHERE id = $1", [orderId]);
      await updateSagaState(redisClient as any, orderId, 'order', eventType, 'FAILURE', 'FAILED');
    } else if (eventType === 'StockReleased') {
      await updateSagaState(redisClient as any, orderId, 'order', eventType, 'COMPENSATING', 'FAILED');
    }
  } catch (err: any) {
    log({
      level: 'error',
      service: 'order-service',
      message: `Failed to update order status for event ${eventType}: ${err.message}`,
      timestamp: new Date().toISOString(),
      orderId,
    });
    throw err; // throw to trigger commit rollback and retry
  }
}

async function handleEvents() {
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const valueStr = message.value?.toString();
      if (!valueStr) return;
      await processEvent(topic, valueStr);
    },
  });
}

async function start() {
  // Connections retry setup
  await retry(async () => {
    await redisClient.connect();
  }, 'order-service', 'Redis connection');

  await retry(async () => {
    await pool.query('SELECT NOW()');
  }, 'order-service', 'Postgres connection');

  await retry(async () => {
    await consumer.connect();
    await consumer.subscribe({ topics: ['payments', 'inventory'], fromBeginning: true });
  }, 'order-service', 'Kafka connection');

  app.listen(PORT, () => {
    log({
      level: 'info',
      service: 'order-service',
      message: `Order Service running on port ${PORT}`,
      timestamp: new Date().toISOString(),
    });
  });

  handleEvents().catch((err) => {
    log({
      level: 'error',
      service: 'order-service',
      message: `Error in event handler loop: ${err.message}`,
      timestamp: new Date().toISOString(),
    });
  });
}

start().catch((err) => {
  log({
    level: 'error',
    service: 'order-service',
    message: `Bootstrap failed: ${err.message}`,
    timestamp: new Date().toISOString(),
  });
  process.exit(1);
});

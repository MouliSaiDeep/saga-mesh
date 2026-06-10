import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import { v4 as uuidv4 } from 'uuid';
import { log, retry, updateSagaState, isDuplicateEvent } from '../shared-utils';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'kafka:9092').split(',');
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';

const redisClient = createClient({ url: REDIS_URL });

const kafka = new Kafka({
  clientId: 'inventory-service',
  brokers: KAFKA_BROKERS,
});

const consumer = kafka.consumer({ groupId: 'inventory-service-group' });
const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 5,
});

// Seeded in-memory store
const stockStore: Record<string, number> = {
  'PROD-001': 100,
  'PROD-002': 50,
  'PROD-003': 0,
};

// Reservations map: orderId -> quantity reserved
const reservationsMap = new Map<string, { productId: string; quantity: number }>();

// Process incoming OrderCreated event
async function handleOrderCreated(orderId: string, event: any) {
  const { productId, quantity } = event;
  if (!productId || typeof quantity !== 'number') return;

  // 1. Check idempotency: if stock reservation already exists for this orderId, skip
  if (reservationsMap.has(orderId)) {
    log({
      level: 'info',
      service: 'inventory-service',
      message: `Reservation already exists for order ${orderId}, skipping.`,
      timestamp: new Date().toISOString(),
      orderId,
    });
    return;
  }

  // 2. Look up product stock
  const currentStock = stockStore[productId] || 0;

  // 3. If stock >= quantity
  if (currentStock >= quantity) {
    stockStore[productId] -= quantity;
    reservationsMap.set(orderId, { productId, quantity });

    log({
      level: 'info',
      service: 'inventory-service',
      message: `Stock reserved for order ${orderId}. Product: ${productId}, Quantity: ${quantity}. Remaining stock: ${stockStore[productId]}`,
      timestamp: new Date().toISOString(),
      orderId,
    });

    const outEvent = {
      eventType: 'InventoryReserved',
      eventId: uuidv4(),
      orderId,
      timestamp: new Date().toISOString(),
    };
    await producer.send({
      topic: 'inventory',
      messages: [{ key: orderId, value: JSON.stringify(outEvent) }],
    });

    await updateSagaState(redisClient as any, orderId, 'inventory', 'InventoryReserved', 'SUCCESS', 'PENDING');
  } else {
    // 4. If stock < quantity
    log({
      level: 'warn',
      service: 'inventory-service',
      message: `Insufficient stock for product ${productId}. Requested: ${quantity}, Available: ${currentStock}`,
      timestamp: new Date().toISOString(),
      orderId,
    });

    const outEvent = {
      eventType: 'InventoryFailed',
      eventId: uuidv4(),
      orderId,
      reason: 'OUT_OF_STOCK',
      timestamp: new Date().toISOString(),
    };
    await producer.send({
      topic: 'inventory',
      messages: [{ key: orderId, value: JSON.stringify(outEvent) }],
    });

    await updateSagaState(redisClient as any, orderId, 'inventory', 'InventoryFailed', 'FAILURE', 'FAILED');
  }
}

// Process incoming PaymentFailed compensator
async function handlePaymentFailed(orderId: string) {
  const reservation = reservationsMap.get(orderId);
  if (reservation) {
    const { productId, quantity } = reservation;
    stockStore[productId] = (stockStore[productId] || 0) + quantity;
    reservationsMap.delete(orderId);

    log({
      level: 'info',
      service: 'inventory-service',
      message: `Stock restored for order ${orderId}. Product: ${productId}, Restored: ${quantity}. New stock: ${stockStore[productId]}`,
      timestamp: new Date().toISOString(),
      orderId,
    });

    const outEvent = {
      eventType: 'StockReleased',
      eventId: uuidv4(),
      orderId,
      reason: 'PaymentFailed',
      timestamp: new Date().toISOString(),
    };
    await producer.send({
      topic: 'inventory',
      messages: [{ key: orderId, value: JSON.stringify(outEvent) }],
    });

    await updateSagaState(redisClient as any, orderId, 'inventory', 'StockReleased', 'COMPENSATING', 'FAILED');
  } else {
    log({
      level: 'warn',
      service: 'inventory-service',
      message: `No stock reservation found to release for order ${orderId}`,
      timestamp: new Date().toISOString(),
      orderId,
    });
  }
}

async function handleEvents() {
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const valueStr = message.value?.toString();
      if (!valueStr) return;

      let event: any;
      try {
        event = JSON.parse(valueStr);
      } catch (err) {
        log({
          level: 'error',
          service: 'inventory-service',
          message: `Failed to parse event JSON: ${valueStr}`,
          timestamp: new Date().toISOString(),
        });
        return;
      }

      const { eventType, eventId, orderId } = event;
      if (!eventId || !orderId) return;

      // Idempotency check
      const isDuplicate = await isDuplicateEvent(redisClient as any, 'inventory-service', eventId);
      if (isDuplicate) {
        log({
          level: 'info',
          service: 'inventory-service',
          message: `Duplicate event ignored: eventId=${eventId}, topic=${topic}`,
          timestamp: new Date().toISOString(),
          orderId,
        });
        return;
      }

      log({
        level: 'info',
        service: 'inventory-service',
        message: `Processing event ${eventType || '(unknown)'} (topic: ${topic})`,
        timestamp: new Date().toISOString(),
        orderId,
      });

      try {
        if (topic === 'orders' || eventType === 'OrderCreated') {
          await handleOrderCreated(orderId, event);
        } else if (topic === 'payments' && eventType === 'PaymentFailed') {
          await handlePaymentFailed(orderId);
        }
      } catch (err: any) {
        log({
          level: 'error',
          service: 'inventory-service',
          message: `Error processing event: ${err.message}`,
          timestamp: new Date().toISOString(),
          orderId,
        });
        throw err;
      }
    },
  });
}

async function start() {
  await retry(async () => {
    await redisClient.connect();
  }, 'inventory-service', 'Redis connection');

  await retry(async () => {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topics: ['orders', 'payments'], fromBeginning: true });
  }, 'inventory-service', 'Kafka connection');

  log({
    level: 'info',
    service: 'inventory-service',
    message: 'Inventory Service successfully started & listening to events',
    timestamp: new Date().toISOString(),
  });

  handleEvents().catch((err) => {
    log({
      level: 'error',
      service: 'inventory-service',
      message: `Error in event handler loop: ${err.message}`,
      timestamp: new Date().toISOString(),
    });
  });
}

start().catch((err) => {
  log({
    level: 'error',
    service: 'inventory-service',
    message: `Bootstrap failed: ${err.message}`,
    timestamp: new Date().toISOString(),
  });
  process.exit(1);
});

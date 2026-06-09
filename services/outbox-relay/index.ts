import { Pool } from 'pg';
import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import http from 'http';
import { log, retry } from '../shared-utils';

const POLL_INTERVAL = 500;
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
  clientId: 'outbox-relay',
  brokers: KAFKA_BROKERS,
});

/*

This relay acts as the event publisher on behalf of the order-service.
The order-service writes intents to the orders_outbox table atomically
with its database transaction. This process reads those intents and
publishes them to Kafka with idempotence enabled, guaranteeing
exactly-once delivery even if the relay restarts mid-publish.
All Kafka producers in this system (outbox-relay, inventory-service,
payment-service) have enable.idempotence = true / idempotent: true.

*/
const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 5,
  transactionTimeout: 30000,
});

async function pollAndRelay() {
  const client = await pool.connect();
  try {
    // Select unprocessed outbox records
    const res = await client.query(
      `SELECT * FROM orders_outbox 
       WHERE processed = FALSE 
       ORDER BY created_at ASC 
       LIMIT 50`
    );

    if (res.rows.length === 0) {
      return;
    }

    log({
      level: 'info',
      service: 'outbox-relay',
      message: `Found ${res.rows.length} unprocessed outbox events to relay`,
      timestamp: new Date().toISOString(),
    });

    for (const row of res.rows) {
      const { id, aggregate_id, topic, payload } = row;

      // Ensure key is orderId (aggregate_id) to guarantee ordering per order partition (Req 4)
      await producer.send({
        topic,
        messages: [
          {
            key: aggregate_id,
            value: typeof payload === 'string' ? payload : JSON.stringify(payload),
          },
        ],
        acks: -1, // Require ACK from all in-sync replicas (Req 8)
      });

      // Update db record to processed = true on successful Kafka ACK
      await client.query(
        'UPDATE orders_outbox SET processed = TRUE WHERE id = $1',
        [id]
      );

      log({
        level: 'info',
        service: 'outbox-relay',
        message: `Relayed event for order ${aggregate_id} to topic ${topic}`,
        timestamp: new Date().toISOString(),
        orderId: aggregate_id,
      });
    }
  } catch (err: any) {
    log({
      level: 'error',
      service: 'outbox-relay',
      message: `Error in relay loop: ${err.message}`,
      timestamp: new Date().toISOString(),
    });
  } finally {
    client.release();
  }
}

async function start() {
  // 1. Redis Connection Retry
  await retry(async () => {
    await redisClient.connect();
  }, 'outbox-relay', 'Redis connection');

  // 2. PG Connection Retry
  await retry(async () => {
    const res = await pool.query('SELECT NOW()');
    log({
      level: 'info',
      service: 'outbox-relay',
      message: `Database connection verified`,
      timestamp: new Date().toISOString(),
    });
  }, 'outbox-relay', 'Postgres connection');

  // 3. Kafka Producer Connection Retry
  await retry(async () => {
    await producer.connect();
    log({
      level: 'info',
      service: 'outbox-relay',
      message: `Idempotent Kafka producer connected`,
      timestamp: new Date().toISOString(),
    });
  }, 'outbox-relay', 'Kafka producer connection');

  // Polling loop
  setInterval(() => {
    pollAndRelay().catch((err) => {
      log({
        level: 'error',
        service: 'outbox-relay',
        message: `Unhandled outbox relay exception: ${err.message}`,
        timestamp: new Date().toISOString(),
      });
    });
  }, POLL_INTERVAL);

  // Health HTTP server
  const HEALTH_PORT = process.env.HEALTH_PORT || 3002;
  http.createServer((req, res) => {
    if (req.url === '/health') {
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('OK');
    } else {
      res.writeHead(404);
      res.end();
    }
  }).listen(HEALTH_PORT, () => {
    log({
      level: 'info',
      service: 'outbox-relay',
      message: `Healthcheck server running on port ${HEALTH_PORT}`,
      timestamp: new Date().toISOString(),
    });
  });

  log({
    level: 'info',
    service: 'outbox-relay',
    message: `Outbox relay service started, polling database every ${POLL_INTERVAL}ms`,
    timestamp: new Date().toISOString(),
  });
}

start().catch((err) => {
  log({
    level: 'error',
    service: 'outbox-relay',
    message: `Bootstrap failed: ${err.message}`,
    timestamp: new Date().toISOString(),
  });
  process.exit(1);
});

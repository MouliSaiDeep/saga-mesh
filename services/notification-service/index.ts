import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import { log, retry, isDuplicateEvent } from '../shared-utils';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'kafka:9092').split(',');
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';

const redisClient = createClient({ url: REDIS_URL });

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: KAFKA_BROKERS,
});

const consumer = kafka.consumer({ groupId: 'notification-service-group' });

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
          service: 'notification-service',
          message: `Failed to parse event JSON: ${valueStr}`,
          timestamp: new Date().toISOString(),
        });
        return;
      }

      const { eventType, eventId, orderId } = event;
      if (!eventId || !orderId) return;

      // Idempotency check
      const isDuplicate = await isDuplicateEvent(redisClient as any, 'notification-service', eventId);
      if (isDuplicate) {
        log({
          level: 'info',
          service: 'notification-service',
          message: `Duplicate event ignored: eventId=${eventId}, topic=${topic}`,
          timestamp: new Date().toISOString(),
          orderId,
        });
        return;
      }

      // Notification listener simulates sending emails/SMS for: PaymentProcessed, PaymentFailed, InventoryFailed
      if (
        eventType === 'PaymentProcessed' ||
        eventType === 'PaymentFailed' ||
        eventType === 'InventoryFailed'
      ) {
        log({
          level: 'info',
          service: 'notification-service',
          message: `[NOTIFICATION SIMULATOR] Event '${eventType}' received for Order ID: ${orderId}. Sending notification payload: ${JSON.stringify(event)}`,
          timestamp: new Date().toISOString(),
          orderId,
        });
      }
    },
  });
}

async function start() {
  // 1. Redis Connection Retry
  await retry(async () => {
    await redisClient.connect();
  }, 'notification-service', 'Redis connection');

  // 2. Kafka Connection Retry
  await retry(async () => {
    await consumer.connect();
    await consumer.subscribe({ topics: ['payments', 'inventory'], fromBeginning: true });
  }, 'notification-service', 'Kafka connection');

  log({
    level: 'info',
    service: 'notification-service',
    message: 'Notification Service started & listening for terminal saga events',
    timestamp: new Date().toISOString(),
  });

  handleEvents().catch((err) => {
    log({
      level: 'error',
      service: 'notification-service',
      message: `Error in event handler loop: ${err.message}`,
      timestamp: new Date().toISOString(),
    });
  });
}

start().catch((err) => {
  log({
    level: 'error',
    service: 'notification-service',
    message: `Bootstrap failed: ${err.message}`,
    timestamp: new Date().toISOString(),
  });
  process.exit(1);
});

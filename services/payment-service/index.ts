import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import { v4 as uuidv4 } from 'uuid';
import { log, retry, updateSagaState, isDuplicateEvent } from '../shared-utils';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'kafka:9092').split(',');
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';

const redisClient = createClient({ url: REDIS_URL });

const kafka = new Kafka({
  clientId: 'payment-service',
  brokers: KAFKA_BROKERS,
});

const consumer = kafka.consumer({ groupId: 'payment-service-group' });
const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 5,
});

let failureRate = 0.0;

// Poll Redis key "payment:failureRate" every 5 seconds to update in-memory failureRate (Req 9)
async function startFailureRatePolling() {
  setInterval(async () => {
    try {
      const val = await redisClient.get('payment:failureRate');
      if (val !== null) {
        const parsed = parseFloat(val);
        if (!isNaN(parsed)) {
          if (parsed !== failureRate) {
            failureRate = parsed;
            log({
              level: 'info',
              service: 'payment-service',
              message: `Updated in-memory failure rate to ${failureRate}`,
              timestamp: new Date().toISOString(),
            });
          }
        }
      }
    } catch (err: any) {
      log({
        level: 'error',
        service: 'payment-service',
        message: `Failed to poll failureRate from Redis: ${err.message}`,
        timestamp: new Date().toISOString(),
      });
    }
  }, 5000);

  // Initial read
  try {
    const val = await redisClient.get('payment:failureRate');
    if (val !== null) {
      failureRate = parseFloat(val);
    }
    log({
      level: 'info',
      service: 'payment-service',
      message: `Initial failure rate loaded: ${failureRate}`,
      timestamp: new Date().toISOString(),
    });
  } catch (e) {}
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
          service: 'payment-service',
          message: `Failed to parse event JSON: ${valueStr}`,
          timestamp: new Date().toISOString(),
        });
        return;
      }

      const { eventType, eventId, orderId } = event;
      if (!eventId || !orderId) return;

      // Idempotency check
      const isDuplicate = await isDuplicateEvent(redisClient as any, 'payment-service', eventId);
      if (isDuplicate) {
        log({
          level: 'info',
          service: 'payment-service',
          message: `Duplicate event ignored: eventId=${eventId}, topic=${topic}`,
          timestamp: new Date().toISOString(),
          orderId,
        });
        return;
      }

      log({
        level: 'info',
        service: 'payment-service',
        message: `Processing event ${eventType || '(unknown)'} (topic: ${topic})`,
        timestamp: new Date().toISOString(),
        orderId,
      });

      try {
        if (eventType === 'InventoryReserved') {
          // 1. Check failure rate. Double-check cache read per-request to be absolutely up-to-date
          try {
            const val = await redisClient.get('payment:failureRate');
            if (val !== null) {
              failureRate = parseFloat(val);
            }
          } catch (e) {}

          // 2. Generate random float
          const randomVal = Math.random();
          const isFailed = randomVal < failureRate;

          log({
            level: 'info',
            service: 'payment-service',
            message: `Evaluating payment for order ${orderId}. Random: ${randomVal.toFixed(4)}, FailureRate: ${failureRate}, Fails: ${isFailed}`,
            timestamp: new Date().toISOString(),
            orderId,
          });

          if (isFailed) {
            // 4. On failure: PaymentFailed
            const outEvent = {
              eventType: 'PaymentFailed',
              eventId: uuidv4(),
              orderId,
              reason: 'PAYMENT_DECLINED',
              timestamp: new Date().toISOString(),
            };
            await producer.send({
              topic: 'payments',
              messages: [{ key: orderId, value: JSON.stringify(outEvent) }],
            });

            // Push saga state update to Redis
            await updateSagaState(redisClient as any, orderId, 'payment', 'PaymentFailed', 'FAILURE', 'COMPENSATING');
          } else {
            // 3. On success: PaymentProcessed
            const outEvent = {
              eventType: 'PaymentProcessed',
              eventId: uuidv4(),
              orderId,
              amount: 100.0, // Mock amount or retrieve from state. The spec doesn't mandate exact value, just a number.
              timestamp: new Date().toISOString(),
            };
            await producer.send({
              topic: 'payments',
              messages: [{ key: orderId, value: JSON.stringify(outEvent) }],
            });

            // Push saga state update to Redis
            await updateSagaState(redisClient as any, orderId, 'payment', 'PaymentProcessed', 'SUCCESS', 'COMPLETED');
          }
        }
      } catch (err: any) {
        log({
          level: 'error',
          service: 'payment-service',
          message: `Error processing payment event: ${err.message}`,
          timestamp: new Date().toISOString(),
          orderId,
        });
        throw err;
      }
    },
  });
}

async function start() {
  // 1. Redis Connection Retry
  await retry(async () => {
    await redisClient.connect();
  }, 'payment-service', 'Redis connection');

  // Start polling failureRate config
  await startFailureRatePolling();

  // 2. Kafka Connection Retry
  await retry(async () => {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topics: ['inventory'], fromBeginning: true });
  }, 'payment-service', 'Kafka connection');

  log({
    level: 'info',
    service: 'payment-service',
    message: 'Payment Service successfully started & listening to events',
    timestamp: new Date().toISOString(),
  });

  handleEvents().catch((err) => {
    log({
      level: 'error',
      service: 'payment-service',
      message: `Error in event handler loop: ${err.message}`,
      timestamp: new Date().toISOString(),
    });
  });
}

start().catch((err) => {
  log({
    level: 'error',
    service: 'payment-service',
    message: `Bootstrap failed: ${err.message}`,
    timestamp: new Date().toISOString(),
  });
  process.exit(1);
});

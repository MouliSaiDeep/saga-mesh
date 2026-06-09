import { Kafka, Producer, Consumer } from 'kafkajs';
import { createClient, RedisClientType } from 'redis';
import pg from 'pg';

export interface LogPayload {
  level: 'info' | 'warn' | 'error';
  service: string;
  message: string;
  timestamp: string;
  orderId?: string;
  [key: string]: any;
}

export function log(payload: LogPayload) {
  console.log(JSON.stringify(payload));
}

export async function retry<T>(
  operation: () => Promise<T>,
  serviceName: string,
  opName: string,
  maxRetries = 10,
  baseDelay = 2000
): Promise<T> {
  let attempt = 0;
  while (true) {
    try {
      return await operation();
    } catch (err: any) {
      attempt++;
      log({
        level: 'warn',
        service: serviceName,
        message: `Failed ${opName} (attempt ${attempt}/${maxRetries}): ${err.message || err}`,
        timestamp: new Date().toISOString(),
      });
      if (attempt >= maxRetries) {
        log({
          level: 'error',
          service: serviceName,
          message: `Max retries reached for ${opName}. Exiting...`,
          timestamp: new Date().toISOString(),
        });
        throw err;
      }
      const delay = baseDelay * Math.pow(2, attempt - 1);
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

export interface SagaHistoryEntry {
  service: 'order' | 'inventory' | 'payment' | 'notification';
  event: string;
  status: 'SUCCESS' | 'FAILURE' | 'COMPENSATING';
  timestamp: string;
}

export interface SagaState {
  orderId: string;
  status: 'PENDING' | 'COMPLETED' | 'COMPENSATING' | 'FAILED';
  history: SagaHistoryEntry[];
}

export async function updateSagaState(
  redisClient: RedisClientType<any, any, any>,
  orderId: string,
  service: SagaHistoryEntry['service'],
  event: string,
  status: SagaHistoryEntry['status'],
  newSagaStatus: SagaState['status']
) {
  const sagaKey = `saga:${orderId}`;
  const data = await redisClient.hGetAll(sagaKey);
  
  let history: SagaHistoryEntry[] = [];
  if (data && data.history) {
    try {
      history = JSON.parse(data.history);
    } catch (e) {
      history = [];
    }
  }

  history.push({
    service,
    event,
    status,
    timestamp: new Date().toISOString(),
  });

  await redisClient.hSet(sagaKey, {
    status: newSagaStatus,
    history: JSON.stringify(history),
  });

  const fullState: SagaState = {
    orderId,
    status: newSagaStatus,
    history,
  };

  await redisClient.publish('saga-updates', JSON.stringify(fullState));
  
  log({
    level: 'info',
    service: 'saga-helper',
    message: `Saga state updated for ${orderId}: status=${newSagaStatus}`,
    timestamp: new Date().toISOString(),
    orderId,
  });
}

export async function isDuplicateEvent(
  redisClient: RedisClientType<any, any, any>,
  service: string,
  eventId: string
): Promise<boolean> {
  const key = `processed:${service}:${eventId}`;
  const result = await redisClient.set(key, '1', {
    EX: 86400,
    NX: true,
  });
  return result === null;
}

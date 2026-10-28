import { PoisonPillError } from '../errors/poison-pill.error';
import { StructuredLogger } from '../logging/structured-logger.service';

export interface ProcessKafkaRecordParams<T> {
  topic: string;
  partition: number;
  offset: string;
  key?: string;
  rawValue?: string;
  logger: StructuredLogger;
  consumerGroupId?: string;
  deserializer: (raw: string) => T;
  handler: (envelope: T) => Promise<Record<string, unknown> | void>;
}

/**
 * Resilient, centralized execution wrapper for Kafka message consumption.
 *
 * Guarantees:
 * 1. Single unified structured log entry per successfully processed record (no log scattering).
 * 2. Poison pill isolation: Corrupted or unparseable messages are diagnosed and skipped without killing consumer.
 * 3. Offset safety: Transient or unhandled business errors are rethrown to KafkaJS, preventing premature offset commits.
 */
export async function processKafkaRecord<T>(params: ProcessKafkaRecordParams<T>): Promise<void> {
  const {
    topic,
    partition,
    offset,
    key = '<null>',
    rawValue,
    logger,
    consumerGroupId,
    deserializer,
    handler,
  } = params;

  // 1. Guard against empty payload
  if (!rawValue) {
    logger.warn('Received empty message payload — skipping record', {
      topic,
      partition,
      offset,
      key,
      groupId: consumerGroupId,
    });
    return;
  }

  const startTime = performance.now();

  // 2. Deserialize with PoisonPill classification
  let envelope: T;
  try {
    envelope = deserializer(rawValue);
  } catch (deserErr: unknown) {
    const errorMsg = deserErr instanceof Error ? deserErr.message : String(deserErr);
    const poisonError = new PoisonPillError(`Malformed message envelope: ${errorMsg}`, {
      topic,
      partition,
      offset,
      key,
      rawValuePreview: rawValue.slice(0, 200),
      originalError: errorMsg,
    });

    logger.error('Poison pill detected — unable to deserialize message', poisonError, {
      topic,
      partition,
      offset,
      key,
      groupId: consumerGroupId,
      error: poisonError,
    });

    // Non-retriable poison pill is isolated to prevent partition blockage.
    return;
  }

  // 3. Execute business use-case handler with timing and resilience
  try {
    const outcome = await handler(envelope);
    const durationMs = Math.round((performance.now() - startTime) * 100) / 100;

    // Single unified structured log on success
    logger.info('Kafka record processed successfully', {
      topic,
      partition,
      offset,
      key,
      groupId: consumerGroupId,
      durationMs,
      ...(outcome ?? {}),
    });
  } catch (err: unknown) {
    const durationMs = Math.round((performance.now() - startTime) * 100) / 100;

    logger.error('Unhandled failure during Kafka message processing', err, {
      topic,
      partition,
      offset,
      key,
      groupId: consumerGroupId,
      durationMs,
      error: err,
    });

    // CRITICAL: Rethrow transient or unhandled error so Kafka consumer does NOT commit offset!
    throw err;
  }
}

import { PoisonPillError } from '../errors/poison-pill.error';
import { StructuredLogger } from '../logging/structured-logger.service';
import { MetricsService } from '../metrics/metrics.service';

export interface ProcessKafkaRecordParams<T> {
  topic: string;
  partition: number;
  offset: string;
  key?: string;
  rawValue?: string;
  logger: StructuredLogger;
  consumerGroupId?: string;
  metricsService?: MetricsService;
  consumerRole?: string;
  transport?: string;
  deserializer: (raw: string) => T;
  handler: (envelope: T) => Promise<Record<string, unknown> | void>;
}

/**
 * Resilient, centralized execution wrapper for Kafka message consumption.
 *
 * Guarantees:
 * 1. Single unified structured log entry per successfully processed record (no log scattering).
 * 2. Poison pill isolation: Corrupted messages fail processing without advancing offsets.
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
    metricsService,
    consumerRole = 'consumer',
    transport = 'kafka',
    deserializer,
    handler,
  } = params;

  // 1. Guard against empty payload
  if (!rawValue) {
    logger.warn('Received empty message payload — refusing offset commit', {
      topic,
      partition,
      offset,
      key,
      groupId: consumerGroupId,
    });
    metricsService?.recordProcessingFailure(consumerRole, 'empty_payload');
    throw new PoisonPillError('Empty Kafka payload');
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

    metricsService?.recordProcessingFailure(consumerRole, 'poison_pill');
    logger.error('Poison pill detected — unable to deserialize message', poisonError, {
      topic,
      partition,
      offset,
      key,
      groupId: consumerGroupId,
      error: poisonError,
    });

    // Never acknowledge a record that could not be validated.
    throw poisonError;
  }

  const eventType =
    envelope &&
    typeof envelope === 'object' &&
    'eventType' in envelope &&
    typeof (envelope as Record<string, unknown>).eventType === 'string'
      ? ((envelope as Record<string, unknown>).eventType as string)
      : 'unknown';

  metricsService?.recordDelivery(consumerRole, transport, eventType);

  // 3. Execute business use-case handler with timing and resilience
  try {
    const outcome = await handler(envelope);
    const durationSeconds = (performance.now() - startTime) / 1000;
    const durationMs = Math.round(durationSeconds * 100000) / 100;

    if (
      outcome &&
      typeof outcome === 'object' &&
      (outcome as Record<string, unknown>).isDuplicate === true
    ) {
      metricsService?.recordDuplicateDelivery(consumerRole, eventType);
    } else {
      metricsService?.recordHandlerCommit(consumerRole, eventType, durationSeconds);
    }

    if (
      envelope &&
      typeof envelope === 'object' &&
      'occurredAt' in envelope &&
      typeof (envelope as Record<string, unknown>).occurredAt === 'string'
    ) {
      const occurredAtTime = new Date(
        (envelope as Record<string, unknown>).occurredAt as string,
      ).getTime();
      if (!Number.isNaN(occurredAtTime)) {
        const ageSeconds = (Date.now() - occurredAtTime) / 1000;
        if (ageSeconds >= 0) {
          metricsService?.recordSourceToHandlerDuration(consumerRole, ageSeconds);
        }
      }
    }

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
    const durationSeconds = (performance.now() - startTime) / 1000;
    const durationMs = Math.round(durationSeconds * 100000) / 100;

    metricsService?.recordProcessingFailure(consumerRole, 'handler_error', durationSeconds);

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

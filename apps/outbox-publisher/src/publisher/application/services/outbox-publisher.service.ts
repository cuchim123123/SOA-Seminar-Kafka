import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { StructuredLogger } from '@lab/observability';
import { AppConfigService } from '../../../config/app-config.service';
import { EVENT_PUBLISHER_PORT, EventPublisherPort } from '../ports/event-publisher.port';
import {
  OUTBOX_PUBLISHER_REPOSITORY_PORT,
  OutboxPublisherRecord,
  OutboxPublisherRepositoryPort,
} from '../ports/outbox-publisher-repository.port';

export interface PublisherStats {
  readonly isRunning: boolean;
  readonly totalPublished: number;
  readonly totalErrors: number;
  readonly lastPublishedEventId: string | null;
  readonly lastPublishedAt: string | null;
  readonly currentBackoffMs: number;
}

@Injectable()
export class OutboxPublisherService implements OnModuleInit, OnModuleDestroy {
  private readonly logger: StructuredLogger;
  private isRunning = false;
  private isProcessing = false;
  private totalPublished = 0;
  private totalErrors = 0;
  private lastPublishedEventId: string | null = null;
  private lastPublishedAt: string | null = null;
  private currentBackoffMs: number;
  private loopPromise: Promise<void> | null = null;
  private sleepResolve: ((value: void | PromiseLike<void>) => void) | null = null;

  constructor(
    @Inject(AppConfigService)
    private readonly config: AppConfigService,
    @Inject(OUTBOX_PUBLISHER_REPOSITORY_PORT)
    private readonly outboxRepository: OutboxPublisherRepositoryPort,
    @Inject(EVENT_PUBLISHER_PORT)
    private readonly eventPublisher: EventPublisherPort,
  ) {
    this.logger = new StructuredLogger(OutboxPublisherService.name, {
      serviceName: 'outbox-publisher',
      format: this.config.nodeEnv === 'production' ? 'json' : 'pretty',
    });
    this.currentBackoffMs = this.config.initialBackoffMs;
  }

  async onModuleInit(): Promise<void> {
    this.logger.info('Initializing Outbox Publisher daemon service...', {
      pollIntervalMs: this.config.pollIntervalMs,
      batchSize: this.config.batchSize,
      initialBackoffMs: this.config.initialBackoffMs,
      maxBackoffMs: this.config.maxBackoffMs,
    });
    this.isRunning = true;
    this.loopPromise = this.runLoop();
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.info('Stopping Outbox Publisher daemon service...');
    this.isRunning = false;
    if (this.sleepResolve) {
      this.sleepResolve();
      this.sleepResolve = null;
    }
    if (this.loopPromise) {
      await this.loopPromise;
    }
    this.logger.info('Outbox Publisher daemon service stopped successfully', {
      totalPublished: this.totalPublished,
      totalErrors: this.totalErrors,
    });
  }

  getStats(): PublisherStats {
    return {
      isRunning: this.isRunning,
      totalPublished: this.totalPublished,
      totalErrors: this.totalErrors,
      lastPublishedEventId: this.lastPublishedEventId,
      lastPublishedAt: this.lastPublishedAt,
      currentBackoffMs: this.currentBackoffMs,
    };
  }

  /**
   * Continuous background polling loop.
   */
  private async runLoop(): Promise<void> {
    while (this.isRunning) {
      try {
        const processed = await this.drainBatch();
        if (processed === 0 && this.isRunning) {
          await this.sleep(this.config.pollIntervalMs);
        }
      } catch (err: unknown) {
        this.totalErrors++;
        const errorMessage = err instanceof Error ? err.message : String(err);
        this.logger.error('Unexpected error in publisher loop iteration', err, {
          error: errorMessage,
          backoffMs: this.currentBackoffMs,
        });
        await this.sleep(this.currentBackoffMs);
        this.currentBackoffMs = Math.min(this.currentBackoffMs * 2, this.config.maxBackoffMs);
      }
    }
  }

  /**
   * Drains a single batch of pending outbox records respecting per-customer ordering.
   * Can also be called on-demand (e.g. from tests or ops endpoints).
   */
  async drainBatch(): Promise<number> {
    if (this.isProcessing) {
      return 0;
    }
    this.isProcessing = true;

    try {
      const pendingRecords = await this.outboxRepository.findPending(this.config.batchSize);
      if (pendingRecords.length === 0) {
        return 0;
      }

      // Group records by customer (aggregateId) to ensure strict sequential processing
      const customerBatches = new Map<string, OutboxPublisherRecord[]>();
      for (const record of pendingRecords) {
        const list = customerBatches.get(record.aggregateId) ?? [];
        list.push(record);
        customerBatches.set(record.aggregateId, list);
      }

      let batchPublished = 0;

      // Process each customer's records in ascending customerSequence order
      for (const records of customerBatches.values()) {
        records.sort((a, b) => a.customerSequence - b.customerSequence);

        for (const record of records) {
          try {
            const result = await this.eventPublisher.publish(record.envelope);
            const publishedAt = new Date().toISOString();
            await this.outboxRepository.markPublished(record.eventId, publishedAt);

            this.totalPublished++;
            batchPublished++;
            this.lastPublishedEventId = record.eventId;
            this.lastPublishedAt = publishedAt;
            this.currentBackoffMs = this.config.initialBackoffMs;

            this.logger.info('Outbox record published to Kafka broker', {
              eventId: record.eventId,
              aggregateId: record.aggregateId,
              customerSequence: record.customerSequence,
              topic: result.topic,
              partition: result.partition,
              offset: result.offset,
              publishedAt,
            });
          } catch (err: unknown) {
            this.totalErrors++;
            const errorMessage = err instanceof Error ? err.message : String(err);
            await this.outboxRepository.markFailed(record.eventId, errorMessage);

            this.logger.error('Failed to publish outbox record to Kafka broker', err, {
              eventId: record.eventId,
              aggregateId: record.aggregateId,
              customerSequence: record.customerSequence,
              error: errorMessage,
            });

            // STOP processing further events for this customer to preserve strict ordering
            // Also back off on broker errors
            this.currentBackoffMs = Math.min(this.currentBackoffMs * 2, this.config.maxBackoffMs);
            return batchPublished;
          }
        }
      }

      return batchPublished;
    } finally {
      this.isProcessing = false;
    }
  }

  private async sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      this.sleepResolve = resolve;
      setTimeout(() => {
        this.sleepResolve = null;
        resolve();
      }, ms);
    });
  }
}

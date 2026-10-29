import { Inject, Injectable } from '@nestjs/common';
import { StructuredLogger } from '@lab/observability';
import {
  EVENT_PUBLISHER_PORT,
  EventPublisherPort,
} from '../../application/ports/event-publisher.port';
import {
  OUTBOX_REPOSITORY_PORT,
  OutboxRepositoryPort,
} from '../../application/ports/outbox-repository.port';

@Injectable()
export class OutboxRelayService {
  private readonly logger = new StructuredLogger(OutboxRelayService.name, {
    serviceName: 'producer-api',
  });
  private isProcessing = false;
  private hasPendingRequests = false;

  constructor(
    @Inject(OUTBOX_REPOSITORY_PORT)
    private readonly outboxRepository: OutboxRepositoryPort,
    @Inject(EVENT_PUBLISHER_PORT)
    private readonly eventPublisher: EventPublisherPort,
  ) {}

  /**
   * Scans and relays pending outbox events to Kafka broker.
   * Ensures per-customer sequential publishing.
   */
  async processPending(): Promise<number> {
    if (this.isProcessing) {
      this.hasPendingRequests = true;
      return 0;
    }
    this.isProcessing = true;

    let totalProcessed = 0;
    try {
      do {
        this.hasPendingRequests = false;
        const pendingRecords = await this.outboxRepository.findPending(100);
        if (pendingRecords.length === 0) {
          break;
        }

        for (const record of pendingRecords) {
          try {
            const result = await this.eventPublisher.publish(record.envelope);
            await this.outboxRepository.markPublished(record.eventId, new Date().toISOString());
            totalProcessed++;

            this.logger.info('Outbox event relayed successfully', {
              eventId: record.eventId,
              aggregateId: record.aggregateId,
              customerSequence: record.customerSequence,
              topic: result.topic,
              partition: result.partition,
              offset: result.offset,
            });
          } catch (err: unknown) {
            const errorMessage = err instanceof Error ? err.message : String(err);
            await this.outboxRepository.markFailed(record.eventId, errorMessage);
            this.logger.error('Failed to relay outbox event to Kafka broker', err, {
              eventId: record.eventId,
              aggregateId: record.aggregateId,
              customerSequence: record.customerSequence,
              error: errorMessage,
            });
            // Stop processing further events to preserve sequential ordering invariant
            return totalProcessed;
          }
        }
      } while (this.hasPendingRequests);
    } finally {
      this.isProcessing = false;
    }

    return totalProcessed;
  }
}

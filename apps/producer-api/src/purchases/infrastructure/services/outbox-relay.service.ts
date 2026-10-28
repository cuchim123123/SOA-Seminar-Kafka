import { Inject, Injectable, Logger } from '@nestjs/common';
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
  private readonly logger = new Logger(OutboxRelayService.name);
  private isProcessing = false;

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
    if (this.isProcessing) return 0;
    this.isProcessing = true;

    let processedCount = 0;
    try {
      const pendingRecords = await this.outboxRepository.findPending(100);

      for (const record of pendingRecords) {
        try {
          const result = await this.eventPublisher.publish(record.envelope);
          await this.outboxRepository.markPublished(record.eventId, new Date().toISOString());
          processedCount++;

          this.logger.log(
            `Relayed Outbox event [eventId=${record.eventId}, aggregateId=${record.aggregateId}, seq=${record.customerSequence}] -> topic=${result.topic}, partition=${result.partition}, offset=${result.offset}`,
          );
        } catch (err: unknown) {
          const errorMessage = err instanceof Error ? err.message : String(err);
          await this.outboxRepository.markFailed(record.eventId, errorMessage);
          this.logger.error(
            `Failed to relay Outbox event [eventId=${record.eventId}]: ${errorMessage}`,
          );
          // Stop processing further events to preserve sequential ordering invariant
          break;
        }
      }
    } finally {
      this.isProcessing = false;
    }

    return processedCount;
  }
}

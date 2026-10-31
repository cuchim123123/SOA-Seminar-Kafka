import { Injectable } from '@nestjs/common';
import { OutboxRecord, OutboxRepositoryPort } from '../../application/ports/outbox-repository.port';

/**
 * Append-only in-memory outbox used by unit/integration test harnesses.
 * Publication state is owned by Debezium CDC (ADR-017) and never modeled here.
 */
@Injectable()
export class InMemoryOutboxRepositoryAdapter implements OutboxRepositoryPort {
  private readonly outbox = new Map<string, OutboxRecord>();

  async save(record: OutboxRecord): Promise<void> {
    this.outbox.set(record.eventId, { ...record });
  }

  async findByAggregateAndSequence(
    aggregateId: string,
    customerSequence: number,
  ): Promise<OutboxRecord | null> {
    for (const record of this.outbox.values()) {
      if (record.aggregateId === aggregateId && record.customerSequence === customerSequence) {
        return { ...record };
      }
    }
    return null;
  }

  clear(): void {
    this.outbox.clear();
  }
}

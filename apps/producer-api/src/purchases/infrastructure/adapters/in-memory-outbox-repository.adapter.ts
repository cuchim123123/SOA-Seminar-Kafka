import { Injectable } from '@nestjs/common';
import { OutboxRecord, OutboxRepositoryPort } from '../../application/ports/outbox-repository.port';

@Injectable()
export class InMemoryOutboxRepositoryAdapter implements OutboxRepositoryPort {
  private readonly outbox = new Map<string, OutboxRecord>();

  async save(record: OutboxRecord): Promise<void> {
    this.outbox.set(record.eventId, { ...record });
  }

  async findPending(limit = 100): Promise<OutboxRecord[]> {
    const pending: OutboxRecord[] = [];
    for (const record of this.outbox.values()) {
      if (record.status === 'PENDING') {
        pending.push({ ...record });
        if (pending.length >= limit) break;
      }
    }
    return pending.sort((a, b) => a.customerSequence - b.customerSequence);
  }

  async markPublished(eventId: string, publishedAt: string): Promise<void> {
    const record = this.outbox.get(eventId);
    if (record) {
      this.outbox.set(eventId, {
        ...record,
        status: 'PUBLISHED',
        publishedAt,
      });
    }
  }

  async markFailed(eventId: string, error: string): Promise<void> {
    const record = this.outbox.get(eventId);
    if (record) {
      this.outbox.set(eventId, {
        ...record,
        status: 'FAILED',
        attemptCount: record.attemptCount + 1,
        lastError: error,
      });
    }
  }

  clear(): void {
    this.outbox.clear();
  }
}

import { IntegrationEventEnvelopeV1 } from '@lab/contracts';

export type OutboxStatus = 'PENDING' | 'PUBLISHED' | 'FAILED';

export interface OutboxRecord {
  readonly eventId: string;
  readonly aggregateId: string;
  readonly customerSequence: number;
  readonly eventType: string;
  readonly envelope: IntegrationEventEnvelopeV1;
  readonly status: OutboxStatus;
  readonly attemptCount: number;
  readonly createdAt: string;
  readonly publishedAt?: string;
  readonly lastError?: string;
}

export const OUTBOX_REPOSITORY_PORT = Symbol('OUTBOX_REPOSITORY_PORT');

export interface OutboxRepositoryPort {
  save(record: OutboxRecord): Promise<void>;
  findPending(limit?: number): Promise<OutboxRecord[]>;
  markPublished(eventId: string, publishedAt: string): Promise<void>;
  markFailed(eventId: string, error: string): Promise<void>;
}

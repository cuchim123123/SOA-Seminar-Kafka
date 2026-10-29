import { IntegrationEventEnvelopeV1 } from '@lab/contracts';

export interface OutboxPublisherRecord {
  readonly eventId: string;
  readonly aggregateId: string;
  readonly customerSequence: number;
  readonly eventType: 'PurchaseCompleted' | 'PurchaseInvalidated';
  readonly envelope: IntegrationEventEnvelopeV1;
  readonly status: 'PENDING' | 'PUBLISHED' | 'FAILED';
  readonly attemptCount: number;
  readonly createdAt: string;
  readonly publishedAt?: string;
}

export interface OutboxPublisherRepositoryPort {
  findPending(limit: number): Promise<OutboxPublisherRecord[]>;
  markPublished(eventId: string, publishedAt: string): Promise<void>;
  markFailed(eventId: string, error: string): Promise<void>;
}

export const OUTBOX_PUBLISHER_REPOSITORY_PORT = Symbol('OUTBOX_PUBLISHER_REPOSITORY_PORT');

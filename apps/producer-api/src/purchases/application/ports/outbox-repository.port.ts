import { IntegrationEventEnvelopeV1 } from '@lab/contracts';

/**
 * Append-only outbox record.
 *
 * Since ADR-017, Debezium CDC is the sole outbox publisher: it tails the
 * PostgreSQL WAL and never acknowledges publication back into source rows.
 * Publication lifecycle (retries, ack status) therefore lives entirely inside
 * Kafka Connect, not in this repository. Legacy `outbox_events.status`,
 * `attempt_count` and `published_at` columns remain in the database schema for
 * historical data, but no application code writes or transitions them.
 */
export interface OutboxRecord {
  readonly eventId: string;
  readonly aggregateId: string;
  readonly customerSequence: number;
  readonly eventType: string;
  readonly envelope: IntegrationEventEnvelopeV1;
  readonly createdAt: string;
}

export const OUTBOX_REPOSITORY_PORT = Symbol('OUTBOX_REPOSITORY_PORT');

export interface OutboxRepositoryPort {
  save(record: OutboxRecord): Promise<void>;
  saveMany?(records: OutboxRecord[]): Promise<void>;
  findByAggregateAndSequence(
    aggregateId: string,
    customerSequence: number,
  ): Promise<OutboxRecord | null>;
}

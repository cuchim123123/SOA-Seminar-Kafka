export const CONSUMER_IDEMPOTENCY_REPOSITORY_PORT = Symbol('CONSUMER_IDEMPOTENCY_REPOSITORY_PORT');

export interface ConsumerIdempotencyRepositoryPort {
  /**
   * Attempts to atomically record an event as processed for this consumer group.
   * Returns true if newly inserted, false if already processed (duplicate).
   */
  markProcessedIfNew(consumerName: string, eventId: string): Promise<boolean>;
}

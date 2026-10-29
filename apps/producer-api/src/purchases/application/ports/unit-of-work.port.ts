import { HttpIdempotencyRepositoryPort } from './http-idempotency-repository.port';
import { OutboxRepositoryPort } from './outbox-repository.port';
import { PurchaseRepositoryPort } from './purchase-repository.port';
import { SequenceGeneratorPort } from './sequence-generator.port';

export interface UnitOfWorkContext {
  readonly purchaseRepository: PurchaseRepositoryPort;
  readonly outboxRepository: OutboxRepositoryPort;
  readonly sequenceGenerator: SequenceGeneratorPort;
  readonly idempotencyRepository: HttpIdempotencyRepositoryPort;
}

export const UNIT_OF_WORK_PORT = Symbol('UNIT_OF_WORK_PORT');

export interface UnitOfWorkPort {
  execute<T>(work: (context: UnitOfWorkContext) => Promise<T>): Promise<T>;
}

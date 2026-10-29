import { ConsumerIdempotencyRepositoryPort } from './consumer-idempotency-repository.port';
import { LoyaltyAccountRepositoryPort } from './loyalty-account-repository.port';

export interface LoyaltyUnitOfWorkContext {
  readonly loyaltyRepository: LoyaltyAccountRepositoryPort;
  readonly idempotencyRepository: ConsumerIdempotencyRepositoryPort;
}

export const LOYALTY_UNIT_OF_WORK_PORT = Symbol('LOYALTY_UNIT_OF_WORK_PORT');

export interface LoyaltyUnitOfWorkPort {
  execute<T>(work: (context: LoyaltyUnitOfWorkContext) => Promise<T>): Promise<T>;
}

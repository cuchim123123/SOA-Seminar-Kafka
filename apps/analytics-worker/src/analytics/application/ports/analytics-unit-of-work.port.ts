import { ConsumerIdempotencyRepositoryPort } from './consumer-idempotency-repository.port';
import { SalesSummaryRepositoryPort } from './sales-summary-repository.port';

export interface AnalyticsUnitOfWorkContext {
  readonly salesSummaryRepository: SalesSummaryRepositoryPort;
  readonly idempotencyRepository: ConsumerIdempotencyRepositoryPort;
}

export const ANALYTICS_UNIT_OF_WORK_PORT = Symbol('ANALYTICS_UNIT_OF_WORK_PORT');

export interface AnalyticsUnitOfWorkPort {
  execute<T>(work: (context: AnalyticsUnitOfWorkContext) => Promise<T>): Promise<T>;
}

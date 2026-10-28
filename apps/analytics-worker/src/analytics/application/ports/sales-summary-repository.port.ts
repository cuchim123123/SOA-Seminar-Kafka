import { SalesSummary } from '../../domain/sales-summary.entity';

export const SALES_SUMMARY_REPOSITORY_PORT = Symbol('SALES_SUMMARY_REPOSITORY_PORT');

export interface SalesSummaryRepositoryPort {
  get(): Promise<SalesSummary>;
  save(summary: SalesSummary): Promise<void>;
}

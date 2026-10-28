export interface SalesSummaryReadModel {
  readonly totalRevenueCents: number;
  readonly purchaseCount: number;
  readonly uniqueCustomerCount: number;
  readonly lastEventId: string | null;
  readonly lastProcessedAt: string | null;
}

export const SALES_SUMMARY_READ_DAO_PORT = Symbol('SALES_SUMMARY_READ_DAO_PORT');

export interface SalesSummaryReadDaoPort {
  getSummary(): Promise<SalesSummaryReadModel>;
}

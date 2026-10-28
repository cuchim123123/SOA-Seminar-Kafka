export interface SalesSummaryResponseDto {
  totalRevenueCents: number;
  totalRevenueDollars: string;
  purchaseCount: number;
  uniqueCustomerCount: number;
  lastEventId: string | null;
  lastProcessedAt: string | null;
}

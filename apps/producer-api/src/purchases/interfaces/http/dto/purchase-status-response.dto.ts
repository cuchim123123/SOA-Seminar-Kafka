export interface PurchaseStatusResponseDto {
  readonly purchaseId: string;
  readonly customerId: string;
  readonly amountCents: number;
  readonly status: 'COMPLETED' | 'INVALIDATED';
  readonly customerSequence: number;
  readonly createdAt: string;
  readonly outboxStatus: 'PENDING' | 'PUBLISHED' | 'FAILED' | 'UNKNOWN';
  readonly publishedAt: string | null;
  readonly eventId: string | null;
}

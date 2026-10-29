export interface PurchaseInvalidatedResponseDto {
  readonly purchaseId: string;
  readonly customerId: string;
  readonly status: 'INVALIDATED';
  readonly customerSequence: number;
  readonly eventId: string;
  readonly reason: string;
  readonly isAlreadyInvalidated: boolean;
}

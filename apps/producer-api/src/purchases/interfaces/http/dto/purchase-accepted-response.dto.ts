export interface PurchaseAcceptedResponseDto {
  readonly purchaseId: string;
  readonly requestStatus: 'ACCEPTED';
  readonly eventId: string;
  readonly correlationId: string;
  readonly partition: number;
  readonly offset: string;
}

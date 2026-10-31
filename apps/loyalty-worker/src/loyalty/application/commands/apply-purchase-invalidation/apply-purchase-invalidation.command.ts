export interface ApplyPurchaseInvalidationInput {
  readonly eventId: string;
  readonly customerId: string;
  readonly purchaseId: string;
  readonly customerSequence: number;
  readonly correlationId: string;
  readonly reason?: string;
}

export class ApplyPurchaseInvalidationCommand {
  constructor(public readonly data: ApplyPurchaseInvalidationInput) {}
}

export interface ApplyPurchaseInvalidationResult {
  readonly eventId: string;
  readonly customerId: string;
  readonly purchaseId: string;
  readonly correctionRequired: boolean;
  readonly lastCustomerSequence: number;
  readonly isDuplicate?: boolean;
}

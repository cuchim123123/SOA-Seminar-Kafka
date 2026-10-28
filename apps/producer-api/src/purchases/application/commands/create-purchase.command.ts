export interface CreatePurchaseInput {
  readonly customerId: string;
  readonly amountCents: number;
  readonly correlationId?: string;
  readonly idempotencyKey?: string;
}

export class CreatePurchaseCommand {
  constructor(public readonly data: CreatePurchaseInput) {}
}

export interface CreatePurchaseResult {
  readonly purchaseId: string;
  readonly customerId: string;
  readonly amountCents: number;
  readonly customerSequence: number;
  readonly eventId: string;
  readonly correlationId: string;
  readonly idempotencyKey?: string;
}

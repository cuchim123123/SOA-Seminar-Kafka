export class CreatePurchaseCommand {
  constructor(
    public readonly customerId: string,
    public readonly amountCents: number,
    public readonly correlationId?: string,
    public readonly idempotencyKey?: string,
  ) {}
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

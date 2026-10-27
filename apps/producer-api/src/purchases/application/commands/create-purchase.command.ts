export class CreatePurchaseCommand {
  constructor(
    public readonly customerId: string,
    public readonly amountCents: number,
    public readonly correlationId?: string,
  ) {}
}

export interface CreatePurchaseResult {
  readonly purchaseId: string;
  readonly customerId: string;
  readonly amountCents: number;
  readonly customerSequence: number;
  readonly eventId: string;
  readonly correlationId: string;
  readonly kafkaMetadata: {
    readonly topic: string;
    readonly partition: number;
    readonly offset: string;
  };
}

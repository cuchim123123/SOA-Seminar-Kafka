export interface CreatePurchaseBatchInput {
  readonly count: number;
  readonly customerId?: string;
  readonly minAmountCents?: number;
  readonly maxAmountCents?: number;
  readonly correlationId?: string;
}

export class CreatePurchaseBatchCommand {
  constructor(public readonly data: CreatePurchaseBatchInput) {}
}

export interface BatchPurchaseItem {
  readonly purchaseId: string;
  readonly customerId: string;
  readonly amountCents: number;
  readonly customerSequence: number;
  readonly eventId: string;
}

export interface CreatePurchaseBatchResult {
  readonly totalCreated: number;
  readonly durationMs: number;
  readonly customerDistribution: Record<string, number>;
  readonly purchases: BatchPurchaseItem[];
}

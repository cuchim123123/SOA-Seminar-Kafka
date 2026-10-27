import { Tier } from '../../domain/loyalty-policy';

export class ApplyPurchaseEventCommand {
  constructor(
    public readonly eventId: string,
    public readonly customerId: string,
    public readonly purchaseId: string,
    public readonly amountCents: number,
    public readonly customerSequence: number,
    public readonly correlationId: string,
    public readonly topic: string,
    public readonly partition: number,
    public readonly offset: string,
  ) {}
}

export interface ApplyPurchaseEventResult {
  readonly eventId: string;
  readonly customerId: string;
  readonly purchaseId: string;
  readonly pointsEarned: number;
  readonly currentTier: Tier;
  readonly totalPoints: number;
  readonly totalSpendCents: number;
  readonly kafkaMetadata: {
    readonly topic: string;
    readonly partition: number;
    readonly offset: string;
  };
}

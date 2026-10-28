import { Tier } from '../../domain/loyalty-policy';

export interface ApplyPurchaseEventInput {
  readonly eventId: string;
  readonly customerId: string;
  readonly purchaseId: string;
  readonly amountCents: number;
  readonly customerSequence: number;
  readonly correlationId: string;
}

export class ApplyPurchaseEventCommand {
  constructor(public readonly data: ApplyPurchaseEventInput) {}
}

export interface ApplyPurchaseEventResult {
  readonly eventId: string;
  readonly customerId: string;
  readonly purchaseId: string;
  readonly pointsEarned: number;
  readonly currentTier: Tier;
  readonly totalPoints: number;
  readonly totalSpendCents: number;
}

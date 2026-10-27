import { Tier } from '../../domain/loyalty-policy';

export interface GetLoyaltyAccountResult {
  readonly customerId: string;
  readonly tier: Tier;
  readonly totalEligibleSpendCents: number;
  readonly pointsBalance: number;
}

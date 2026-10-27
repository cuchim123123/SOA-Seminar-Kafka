import { pointsForPurchase, Tier, tierForEligibleSpend } from './loyalty-policy';

export interface LoyaltyAccountProps {
  readonly customerId: string;
  tier: Tier;
  totalEligibleSpendCents: number;
  pointsBalance: number;
}

export interface ApplyPurchaseResult {
  readonly pointsEarned: number;
  readonly previousTier: Tier;
  readonly newTier: Tier;
  readonly totalSpendCents: number;
  readonly totalPoints: number;
}

export class LoyaltyAccount {
  private constructor(private readonly props: LoyaltyAccountProps) {}

  public static initializeNew(customerId: string): LoyaltyAccount {
    if (!customerId || customerId.trim().length === 0) {
      throw new Error('Loyalty account customerId must not be empty');
    }
    return new LoyaltyAccount({
      customerId,
      tier: 'BRONZE',
      totalEligibleSpendCents: 0,
      pointsBalance: 0,
    });
  }

  public static reconstitute(props: LoyaltyAccountProps): LoyaltyAccount {
    return new LoyaltyAccount({ ...props });
  }

  get customerId(): string {
    return this.props.customerId;
  }

  get tier(): Tier {
    return this.props.tier;
  }

  get totalEligibleSpendCents(): number {
    return this.props.totalEligibleSpendCents;
  }

  get pointsBalance(): number {
    return this.props.pointsBalance;
  }

  /**
   * Applies a purchase to the loyalty account according to domain rules:
   * 1. Points are calculated based on CURRENT tier.
   * 2. Eligible spend is incremented.
   * 3. Tier is recomputed for subsequent purchases.
   * 4. Points balance is credited.
   */
  applyPurchase(amountCents: number): ApplyPurchaseResult {
    const previousTier = this.props.tier;
    const pointsEarned = pointsForPurchase(amountCents, previousTier);

    this.props.totalEligibleSpendCents += amountCents;
    this.props.tier = tierForEligibleSpend(this.props.totalEligibleSpendCents);
    this.props.pointsBalance += pointsEarned;

    return {
      pointsEarned,
      previousTier,
      newTier: this.props.tier,
      totalSpendCents: this.props.totalEligibleSpendCents,
      totalPoints: this.props.pointsBalance,
    };
  }
}

import { pointsForPurchase, Tier, tierForEligibleSpend } from './loyalty-policy';
import {
  customerIdSchema,
  LoyaltyAccountProps,
  loyaltyAccountPropsSchema,
} from './loyalty-account.schema';

export interface ApplyPurchaseResult {
  readonly pointsEarned: number;
  readonly previousTier: Tier;
  readonly newTier: Tier;
  readonly totalSpendCents: number;
  readonly totalPoints: number;
}

export class LoyaltyAccount {
  private constructor(private readonly props: LoyaltyAccountProps) {}

  public static initializeNew(rawCustomerId: string): LoyaltyAccount {
    const parseResult = customerIdSchema.safeParse(rawCustomerId);
    if (!parseResult.success) {
      const issue = parseResult.error.errors[0];
      throw new Error(issue?.message ?? 'Loyalty account customerId must not be empty');
    }

    return new LoyaltyAccount({
      customerId: parseResult.data,
      tier: 'BRONZE',
      totalEligibleSpendCents: 0,
      pointsBalance: 0,
    });
  }

  public static reconstitute(rawProps: LoyaltyAccountProps): LoyaltyAccount {
    const parseResult = loyaltyAccountPropsSchema.safeParse(rawProps);
    if (!parseResult.success) {
      const issue = parseResult.error.errors[0];
      throw new Error(issue?.message ?? 'Invalid loyalty account properties');
    }

    return new LoyaltyAccount({ ...parseResult.data });
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

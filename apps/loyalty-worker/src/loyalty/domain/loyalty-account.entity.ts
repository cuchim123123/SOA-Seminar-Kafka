import { pointsForPurchase, Tier, tierForEligibleSpend } from './loyalty-policy';

export interface LoyaltyAccountProps {
  readonly customerId: string;
  tier: Tier;
  totalEligibleSpendCents: number;
  pointsBalance: number;
  lastCustomerSequence: number;
  correctionRequired: boolean;
}

export interface ApplyPurchaseResult {
  readonly pointsEarned: number;
  readonly previousTier: Tier;
  readonly newTier: Tier;
  readonly totalSpendCents: number;
  readonly totalPoints: number;
  readonly lastCustomerSequence: number;
}

function assertNonEmptyCustomerId(customerId: string | undefined | null): string {
  if (!customerId || customerId.trim().length === 0) {
    throw new Error('Loyalty account customerId must not be empty');
  }
  return customerId.trim();
}

export class LoyaltyAccount {
  private constructor(private readonly props: LoyaltyAccountProps) {}

  public static create(customerId: string): LoyaltyAccount {
    return new LoyaltyAccount({
      customerId: assertNonEmptyCustomerId(customerId),
      tier: 'BRONZE',
      totalEligibleSpendCents: 0,
      pointsBalance: 0,
      lastCustomerSequence: 0,
      correctionRequired: false,
    });
  }

  public static reconstitute(props: LoyaltyAccountProps): LoyaltyAccount {
    return new LoyaltyAccount({
      customerId: assertNonEmptyCustomerId(props.customerId),
      tier: props.tier,
      totalEligibleSpendCents: props.totalEligibleSpendCents,
      pointsBalance: props.pointsBalance,
      lastCustomerSequence: props.lastCustomerSequence,
      correctionRequired: props.correctionRequired,
    });
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

  get lastCustomerSequence(): number {
    return this.props.lastCustomerSequence;
  }

  get correctionRequired(): boolean {
    return this.props.correctionRequired;
  }

  markCorrectionRequired(): void {
    this.props.correctionRequired = true;
  }

  /**
   * Applies a purchase to the loyalty account according to domain rules:
   * 1. Validates strictly incrementing sequence if provided.
   * 2. Points are calculated based on CURRENT tier.
   * 3. Eligible spend is incremented.
   * 4. Tier is recomputed for subsequent purchases.
   * 5. Points balance is credited.
   * 6. Last sequence is updated.
   */
  applyPurchase(amountCents: number, sequence?: number): ApplyPurchaseResult {
    if (sequence !== undefined) {
      const expectedSequence = this.props.lastCustomerSequence + 1;
      if (sequence !== expectedSequence) {
        throw new Error(
          `Sequence gap or out-of-order event for customer ${this.props.customerId}: expected ${expectedSequence}, received ${sequence}`,
        );
      }
      this.props.lastCustomerSequence = sequence;
    }

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
      lastCustomerSequence: this.props.lastCustomerSequence,
    };
  }
}

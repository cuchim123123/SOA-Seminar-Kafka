export type Tier = 'BRONZE' | 'SILVER' | 'GOLD';

/**
 * Calculates customer loyalty tier based on cumulative eligible spend in cents.
 * Tier thresholds:
 * - >= $300.00 (30,000 cents): GOLD (3x multiplier)
 * - >= $100.00 (10,000 cents): SILVER (2x multiplier)
 * - < $100.00: BRONZE (1x multiplier)
 */
export function tierForEligibleSpend(totalCents: number): Tier {
  if (!Number.isSafeInteger(totalCents) || totalCents < 0) {
    throw new Error(`Invalid eligible spend: ${totalCents}. Must be a non-negative integer.`);
  }
  if (totalCents >= 30_000) {
    return 'GOLD';
  }
  if (totalCents >= 10_000) {
    return 'SILVER';
  }
  return 'BRONZE';
}

/**
 * Calculates loyalty points earned for an individual purchase.
 * Rule: 1 point per $10.00 (1,000 cents) multiplied by current tier multiplier.
 */
export function pointsForPurchase(amountCents: number, tier: Tier): number {
  if (!Number.isSafeInteger(amountCents) || amountCents <= 0) {
    throw new Error(`Invalid purchase amount: ${amountCents}. Must be a positive integer.`);
  }

  const multiplier: Record<Tier, number> = {
    BRONZE: 1,
    SILVER: 2,
    GOLD: 3,
  };

  return Math.floor(amountCents / 1_000) * multiplier[tier];
}

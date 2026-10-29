import { describe, expect, it } from 'vitest';
import { LoyaltyAccount } from '../../../src/loyalty/domain/loyalty-account.entity';

describe('LoyaltyAccount Domain Entity', () => {
  it('correctly calculates canonical seminar fixture (U001: $120, $200, $100 -> 82 points)', () => {
    const account = LoyaltyAccount.create('U001');
    expect(account.tier).toBe('BRONZE');
    expect(account.pointsBalance).toBe(0);
    expect(account.totalEligibleSpendCents).toBe(0);

    // Purchase 1: $120.00 (12,000 cents) evaluated at BRONZE
    const p1 = account.applyPurchase(12_000);
    expect(p1.pointsEarned).toBe(12); // 12 * 1x
    expect(p1.previousTier).toBe('BRONZE');
    expect(p1.newTier).toBe('SILVER');
    expect(p1.totalPoints).toBe(12);
    expect(p1.totalSpendCents).toBe(12_000);

    // Purchase 2: $200.00 (20,000 cents) evaluated at SILVER
    const p2 = account.applyPurchase(20_000);
    expect(p2.pointsEarned).toBe(40); // 20 * 2x
    expect(p2.previousTier).toBe('SILVER');
    expect(p2.newTier).toBe('GOLD');
    expect(p2.totalPoints).toBe(52); // 12 + 40
    expect(p2.totalSpendCents).toBe(32_000);

    // Purchase 3: $100.00 (10,000 cents) evaluated at GOLD
    const p3 = account.applyPurchase(10_000);
    expect(p3.pointsEarned).toBe(30); // 10 * 3x
    expect(p3.previousTier).toBe('GOLD');
    expect(p3.newTier).toBe('GOLD');
    expect(p3.totalPoints).toBe(82); // 52 + 30
    expect(p3.totalSpendCents).toBe(42_000);

    // Final balance
    expect(account.pointsBalance).toBe(82);
    expect(account.tier).toBe('GOLD');
  });

  it('marks correctionRequired and updates lastCustomerSequence on applyInvalidation', () => {
    const account = LoyaltyAccount.create('U001');
    account.applyPurchase(12_000, 1);
    expect(account.correctionRequired).toBe(false);
    expect(account.lastCustomerSequence).toBe(1);

    account.applyInvalidation(2);
    expect(account.correctionRequired).toBe(true);
    expect(account.lastCustomerSequence).toBe(2);
    // Points and tier should remain untouched
    expect(account.pointsBalance).toBe(12);
    expect(account.tier).toBe('SILVER');
  });

  it('rejects out of order customer sequence on applyInvalidation', () => {
    const account = LoyaltyAccount.create('U001');
    account.applyPurchase(12_000, 1);

    expect(() => account.applyInvalidation(3)).toThrowError(
      /Sequence gap or out-of-order event for customer U001: expected 2, received 3/,
    );
  });

  it('rejects empty customerId on initialization', () => {
    expect(() => LoyaltyAccount.create('')).toThrowError(/customerId must not be empty/);
    expect(() => LoyaltyAccount.create('   ')).toThrowError(/customerId must not be empty/);
  });
});

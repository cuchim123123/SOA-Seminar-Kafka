import { describe, expect, it } from 'vitest';
import {
  pointsForPurchase,
  tierForEligibleSpend,
} from '../../../src/loyalty/domain/loyalty-policy';

describe('Loyalty Policy Domain Functions', () => {
  describe('tierForEligibleSpend', () => {
    it('returns BRONZE for spend below $100.00 (10,000 cents)', () => {
      expect(tierForEligibleSpend(0)).toBe('BRONZE');
      expect(tierForEligibleSpend(5_000)).toBe('BRONZE');
      expect(tierForEligibleSpend(9_999)).toBe('BRONZE');
    });

    it('returns SILVER for spend between $100.00 and $299.99', () => {
      expect(tierForEligibleSpend(10_000)).toBe('SILVER');
      expect(tierForEligibleSpend(25_000)).toBe('SILVER');
      expect(tierForEligibleSpend(29_999)).toBe('SILVER');
    });

    it('returns GOLD for spend of $300.00 (30,000 cents) or higher', () => {
      expect(tierForEligibleSpend(30_000)).toBe('GOLD');
      expect(tierForEligibleSpend(100_000)).toBe('GOLD');
    });

    it('rejects invalid or negative spend', () => {
      expect(() => tierForEligibleSpend(-1)).toThrowError(/Invalid eligible spend/);
      expect(() => tierForEligibleSpend(12.34)).toThrowError(/Invalid eligible spend/);
      expect(() => tierForEligibleSpend(NaN)).toThrowError(/Invalid eligible spend/);
    });
  });

  describe('pointsForPurchase', () => {
    it('calculates 1 point per 1,000 cents for BRONZE tier', () => {
      expect(pointsForPurchase(999, 'BRONZE')).toBe(0);
      expect(pointsForPurchase(1_000, 'BRONZE')).toBe(1);
      expect(pointsForPurchase(2_500, 'BRONZE')).toBe(2);
      expect(pointsForPurchase(10_000, 'BRONZE')).toBe(10);
    });

    it('calculates 2 points per 1,000 cents for SILVER tier', () => {
      expect(pointsForPurchase(1_000, 'SILVER')).toBe(2);
      expect(pointsForPurchase(2_500, 'SILVER')).toBe(4);
      expect(pointsForPurchase(10_000, 'SILVER')).toBe(20);
    });

    it('calculates 3 points per 1,000 cents for GOLD tier', () => {
      expect(pointsForPurchase(1_000, 'GOLD')).toBe(3);
      expect(pointsForPurchase(2_500, 'GOLD')).toBe(6);
      expect(pointsForPurchase(10_000, 'GOLD')).toBe(30);
    });

    it('rejects non-positive or non-integer amounts', () => {
      expect(() => pointsForPurchase(0, 'BRONZE')).toThrowError(/Invalid purchase amount/);
      expect(() => pointsForPurchase(-500, 'BRONZE')).toThrowError(/Invalid purchase amount/);
      expect(() => pointsForPurchase(15.5, 'BRONZE')).toThrowError(/Invalid purchase amount/);
    });
  });
});

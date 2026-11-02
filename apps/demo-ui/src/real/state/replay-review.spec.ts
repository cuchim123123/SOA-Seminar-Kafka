import { describe, expect, it } from 'vitest';
import type { CustomerComparisonDto } from '../../api/client';
import { expectedPromotion, mapReview } from './replay-review';

const comparison = (live: number | null, staged: number): CustomerComparisonDto => ({
  runId: 'run',
  customerId: 'customer',
  live:
    live === null
      ? null
      : {
          points: 1,
          tier: 'BRONZE',
          eligibleSpendCents: 1000,
          lastCustomerSequence: live,
          correctionRequired: true,
        },
  staged: {
    points: 1,
    tier: 'BRONZE',
    eligibleSpendCents: 1000,
    lastCustomerSequence: staged,
    scannedEventCount: 1,
  },
  discrepancy: { pointsDifference: 0, spendDifferenceCents: 0, tierChanged: false },
});
describe('global replay review', () => {
  it('mirrors existing scope and sequence eligibility including unchanged values', () => {
    expect(expectedPromotion(comparison(null, 2))).toBe('create');
    expect(expectedPromotion(comparison(2, 2))).toBe('update');
    expect(expectedPromotion(comparison(1, 2))).toBe('update');
    expect(expectedPromotion(comparison(3, 2))).toBe('skip');
    expect(() => expectedPromotion({ ...comparison(1, 2), staged: null })).toThrow();
    expect(() => expectedPromotion(comparison(Number.MAX_SAFE_INTEGER + 1, 2))).toThrow();
  });
  it('reads every customer in stable order with at most four concurrent reads', async () => {
    let active = 0;
    let peak = 0;
    const input = Array.from({ length: 37 }, (_, i) => i);
    const output = await mapReview(input, async (value) => {
      active++;
      peak = Math.max(peak, active);
      await Promise.resolve();
      active--;
      return value * 2;
    });
    expect(output).toEqual(input.map((value) => value * 2));
    expect(peak).toBeLessThanOrEqual(4);
    await expect(
      mapReview([1, 2], async () => {
        throw new Error('unavailable');
      }),
    ).rejects.toThrow('unavailable');
  });
});

import type { CustomerComparisonDto } from '../../api/client';

export function expectedPromotion(row: CustomerComparisonDto): 'create' | 'update' | 'skip' {
  if (!row.staged) throw new Error(`Missing staged comparison for ${row.customerId}`);
  for (const state of [row.live, row.staged]) {
    if (
      state &&
      (!Number.isSafeInteger(state.points) || !Number.isSafeInteger(state.eligibleSpendCents))
    )
      throw new Error('Financial values exceed the read API precision. Review unavailable.');
  }
  const sequence = (value: number | string | undefined) => {
    if (value === undefined || (typeof value === 'number' && !Number.isSafeInteger(value)))
      throw new Error('Sequence is missing or exceeds the read API precision. Review unavailable.');
    return BigInt(value);
  };
  const staged = sequence(row.staged.lastCustomerSequence);
  if (!row.live) return 'create';
  return sequence(row.live.lastCustomerSequence) > staged ? 'skip' : 'update';
}

export async function mapReview<T, R>(items: T[], load: (item: T) => Promise<R>): Promise<R[]> {
  const results = new Array<R>(items.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(4, items.length) }, async () => {
      while (next < items.length) {
        const index = next++;
        results[index] = await load(items[index]);
      }
    }),
  );
  return results;
}

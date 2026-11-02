import { expect, it } from 'vitest';
import { readPurchaseIntent, intentStorageKey } from './purchase-intent';

it('recovers the same customer intent and rejects damaged or inaccessible storage', () => {
  const key = intentStorageKey('customer');
  const values = new Map([[key, JSON.stringify({ amount: '10.00', key: 'same-intent' })]]);
  const storage = { getItem: (name: string) => values.get(name) ?? null };
  expect(readPurchaseIntent(storage, 'customer')).toEqual({ amount: '10.00', key: 'same-intent' });
  expect(readPurchaseIntent(storage, 'other')).toBeNull();
  values.set(key, 'bad json');
  expect(readPurchaseIntent(storage, 'customer')).toBeNull();
  values.set(key, JSON.stringify({ amount: '10', key: '' }));
  expect(readPurchaseIntent(storage, 'customer')).toBeNull();
  expect(
    readPurchaseIntent(
      {
        getItem: () => {
          throw new Error('blocked');
        },
      },
      'customer',
    ),
  ).toBeNull();
});

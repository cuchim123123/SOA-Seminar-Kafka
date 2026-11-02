import { expect, it } from 'vitest';
import type { PurchaseHistoryItem } from '../../api/business';
import { customerEvents } from './customer-events';

it('orders separate completion/invalidation identities without numeric sequence precision loss', () => {
  const item: PurchaseHistoryItem = {
    purchaseId: 'p',
    customerId: 'c',
    amountCents: '100',
    status: 'INVALIDATED',
    createdAt: '',
    completionSequence: '9007199254740992',
    completionEventId: 'original',
    invalidationSequence: '9007199254740993',
    invalidationEventId: 'invalidated',
    invalidationReason: 'DEMO_INVALIDATION',
  };
  const rows = customerEvents([
    {
      ...item,
      purchaseId: 'later',
      completionSequence: '9007199254740994',
      completionEventId: 'later',
      invalidationSequence: null,
    },
    item,
  ]);
  expect(rows.map((row) => row.eventId)).toEqual(['original', 'invalidated', 'later']);
});

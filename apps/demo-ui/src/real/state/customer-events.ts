import type { PurchaseHistoryItem } from '../../api/business';

export function customerEvents(items: PurchaseHistoryItem[]) {
  return items
    .flatMap((item) => [
      {
        purchaseId: item.purchaseId,
        type: 'PurchaseCompleted',
        eventId: item.completionEventId,
        sequence: item.completionSequence,
      },
      ...(item.invalidationSequence
        ? [
            {
              purchaseId: item.purchaseId,
              type: 'PurchaseInvalidated',
              eventId: item.invalidationEventId,
              sequence: item.invalidationSequence,
            },
          ]
        : []),
    ])
    .sort((a, b) =>
      BigInt(a.sequence) < BigInt(b.sequence)
        ? -1
        : BigInt(a.sequence) > BigInt(b.sequence)
          ? 1
          : 0,
    );
}

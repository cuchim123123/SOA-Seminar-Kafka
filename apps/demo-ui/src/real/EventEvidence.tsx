import { useState } from 'react';
import { businessApi } from '../api/business';
import { useLiveResource } from './hooks/useLiveResource';
import { PurchaseEvidence } from './PurchaseEvidence';
import { CustomerEventSequence } from './CustomerEventSequence';
import { routeHash } from './state/routes';
import type { WorkspaceRoute } from './state/routes';

export function EventEvidence({ route }: { route: WorkspaceRoute }) {
  const [purchaseId, setPurchaseId] = useState(route.purchaseId);
  const evidence = useLiveResource('producer', `evidence:${route.purchaseId}`, (signal) =>
    route.purchaseId
      ? businessApi.listPurchases({ purchaseId: route.purchaseId, limit: 1 }, signal)
      : Promise.resolve({ items: [], nextCursor: null }),
  );
  return (
    <>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          window.location.hash = routeHash({ ...route, purchaseId: purchaseId.trim() });
        }}
      >
        <label>
          Purchase ID{' '}
          <input
            value={purchaseId}
            onChange={(event) => setPurchaseId(event.target.value)}
            required
          />
        </label>
        <button>Inspect purchase</button>
      </form>
      <p>
        Push: {evidence.connection} · {evidence.stale ? 'May be stale' : 'Connected'}
      </p>
      {evidence.error && <p role="alert">Unavailable: {evidence.error}</p>}
      {evidence.loading && <p>Loading…</p>}
      {evidence.data?.items.map((item) => (
        <PurchaseEvidence key={item.purchaseId} item={item} refresh={evidence.refresh} />
      ))}
      {route.purchaseId && evidence.data?.items.length === 0 && <p>No persisted purchase found.</p>}
      {evidence.data?.items[0] && (
        <CustomerEventSequence
          key={evidence.data.items[0].customerId}
          customerId={evidence.data.items[0].customerId}
          route={route}
        />
      )}
    </>
  );
}

import { useState } from 'react';
import { businessApi } from '../api/business';
import { useLiveResource } from './hooks/useLiveResource';
import { customerEvents } from './state/customer-events';
import { routeHash } from './state/routes';
import type { WorkspaceRoute } from './state/routes';

export function CustomerEventSequence({
  customerId,
  route,
}: {
  customerId: string;
  route: WorkspaceRoute;
}) {
  const [cursor, setCursor] = useState<string | undefined>();
  const history = useLiveResource('producer', `sequence:${customerId}:${cursor ?? ''}`, (signal) =>
    businessApi.listPurchases({ customerId, cursor, limit: 25 }, signal),
  );
  return (
    <article>
      <h2>Source event sequence for {customerId}</h2>
      <p>
        Events from this page of persisted purchases, ordered by allocated customer sequence. This
        is a bounded source view, not a global Kafka sequence or complete delivery log. Projection
        sequence alone does not prove each event receipt, particularly after cutover.
      </p>
      <p>
        Push: {history.connection} · {history.stale ? 'May be stale' : 'Connected'} · Observed:{' '}
        {history.observedAt ?? 'Not observed'}
      </p>
      {history.error && <p role="alert">Unavailable: {history.error}</p>}
      {history.loading && <p>Loading…</p>}
      <div className="real-table">
        <table>
          <thead>
            <tr>
              <th>Customer sequence</th>
              <th>Type</th>
              <th>Event ID</th>
              <th>Purchase</th>
            </tr>
          </thead>
          <tbody>
            {customerEvents(history.data?.items ?? []).map((row) => (
              <tr key={`${row.purchaseId}:${row.type}`}>
                <td>{row.sequence}</td>
                <td>{row.type}</td>
                <td>{row.eventId ?? 'Not observed'}</td>
                <td>
                  <a href={routeHash({ ...route, purchaseId: row.purchaseId })}>{row.purchaseId}</a>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <button onClick={() => setCursor(undefined)}>Latest purchases</button>
      <button
        disabled={!history.data?.nextCursor || history.loading || !!history.error}
        onClick={() => setCursor(history.data?.nextCursor ?? undefined)}
      >
        Older purchases
      </button>
      <button onClick={history.refresh}>Refresh source sequence</button>
    </article>
  );
}

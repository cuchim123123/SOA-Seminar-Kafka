import { useState } from 'react';
import { PurchaseEvidence } from './PurchaseEvidence';
import { api } from '../api/client';
import { ApiError } from '../api/http';
import { businessApi } from '../api/business';
import { useLiveResource } from './hooks/useLiveResource';
import { parseAmountCents } from './state/money';
import { routeHash } from './state/routes';
import type { WorkspaceRoute } from './state/routes';

export function Purchases({ route }: { route: WorkspaceRoute }) {
  const [amount, setAmount] = useState('100.00');
  const [key, setKey] = useState<string>(() => crypto.randomUUID());
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState('');
  const [cursor, setCursor] = useState<string | undefined>();
  const history = useLiveResource('producer', `${route.customerId}:${cursor ?? ''}`, (signal) =>
    businessApi.listPurchases(
      { customerId: route.customerId || undefined, cursor, limit: 25 },
      signal,
    ),
  );
  const detail = useLiveResource('producer', `purchase:${route.purchaseId}`, (signal) =>
    route.purchaseId ? businessApi.getPurchase(route.purchaseId, signal) : Promise.resolve(null),
  );
  const evidence = useLiveResource('producer', `evidence:${route.purchaseId}`, (signal) =>
    route.purchaseId
      ? businessApi.listPurchases({ purchaseId: route.purchaseId, limit: 1 }, signal)
      : Promise.resolve({ items: [], nextCursor: null }),
  );
  return (
    <>
      <article>
        <h2>Create a purchase</h2>
        <p>Selected customer: {route.customerId || 'Select a customer above first.'}</p>
        <form
          onSubmit={async (event) => {
            event.preventDefault();
            if (busy || !route.customerId) return;
            let amountCents: number;
            try {
              amountCents = parseAmountCents(amount);
            } catch (error) {
              setResult(error instanceof Error ? error.message : String(error));
              return;
            }
            setBusy(true);
            try {
              const accepted = await api.createPurchase(route.customerId, amountCents, key.trim());
              setResult(
                `Source accepted purchase ${accepted.purchaseId}, event ${accepted.eventId}. Downstream processing is observed separately.`,
              );
              window.location.hash = routeHash({ ...route, purchaseId: accepted.purchaseId });
              history.refresh();
            } catch (error) {
              setResult(
                error instanceof ApiError
                  ? `HTTP ${error.status}: ${error.message}`
                  : `Outcome may be unknown: ${error instanceof Error ? error.message : String(error)}. Retry with the same key and body.`,
              );
            } finally {
              setBusy(false);
            }
          }}
        >
          <label>
            Amount (currency units){' '}
            <input
              value={amount}
              onChange={(e) => setAmount(e.target.value)}
              disabled={busy}
              inputMode="decimal"
              required
            />
          </label>
          <label>
            Idempotency key{' '}
            <input value={key} onChange={(e) => setKey(e.target.value)} disabled={busy} required />
          </label>
          <button disabled={busy || !route.customerId || !key.trim()}>
            Submit / retry same intent
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={() => {
              setKey(crypto.randomUUID());
              setResult('');
            }}
          >
            Start new intent
          </button>
        </form>
        <p>
          The key stays unchanged after submission. Same key and body returns the original purchase;
          a changed body returns HTTP409. Start a new intent only for a new purchase. Retain the key
          before leaving this page if an outcome is uncertain.
        </p>
        <p role="status">{result}</p>
      </article>
      <article>
        <h2>Persisted purchase history</h2>
        <p>
          Push: {history.connection} · {history.stale ? 'May be stale' : 'Connected'}
        </p>
        {history.error && <p role="alert">Unavailable: {history.error}</p>}
        <div className="real-table">
          <table>
            <thead>
              <tr>
                <th>Purchase</th>
                <th>Customer</th>
                <th>Amount (cents)</th>
                <th>Source status</th>
                <th>Completion sequence</th>
              </tr>
            </thead>
            <tbody>
              {history.data?.items.map((item) => (
                <tr key={item.purchaseId}>
                  <td>
                    <a href={routeHash({ ...route, purchaseId: item.purchaseId })}>
                      {item.purchaseId}
                    </a>
                  </td>
                  <td>{item.customerId}</td>
                  <td>{item.amountCents}</td>
                  <td>{item.status}</td>
                  <td>{item.completionSequence}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {history.loading && <p>Loading…</p>}
        {history.data?.items.length === 0 && <p>No matching persisted purchases.</p>}
        <button onClick={() => setCursor(undefined)}>Latest page</button>
        <button
          disabled={!history.data?.nextCursor || history.loading || !!history.error}
          onClick={() => setCursor(history.data?.nextCursor ?? undefined)}
        >
          Older purchases
        </button>
        <button onClick={history.refresh}>Refresh history</button>
      </article>
      {route.purchaseId && (
        <article>
          <h2>Canonical source detail</h2>
          {detail.error && <p role="alert">Unavailable: {detail.error}</p>}
          {detail.data && (
            <dl>
              {Object.entries(detail.data).map(([name, value]) => (
                <div key={name}>
                  <dt>{name}</dt>
                  <dd>{value === null ? 'Not observed' : String(value)}</dd>
                </div>
              ))}
            </dl>
          )}
          <p>Source acceptance and outbox state do not prove downstream processing.</p>
          <button onClick={detail.refresh}>Refresh detail</button>
        </article>
      )}
      {route.purchaseId &&
        (() => {
          const item = evidence.data?.items.find((row) => row.purchaseId === route.purchaseId);
          return item ? (
            <PurchaseEvidence
              key={item.purchaseId}
              item={item}
              refresh={() => {
                history.refresh();
                detail.refresh();
                evidence.refresh();
              }}
            />
          ) : (
            <p>
              {evidence.error
                ? `Evidence unavailable: ${evidence.error}`
                : evidence.loading
                  ? 'Loading event evidence…'
                  : 'No persisted evidence found for this purchase.'}
            </p>
          );
        })()}
    </>
  );
}

import { useState } from 'react';
import { api } from '../api/client';
import type { PurchaseBatchResponse } from '../api/client';
import { ApiError } from '../api/http';
import { parseAmountCents } from './state/money';
import { routeHash } from './state/routes';
import type { WorkspaceRoute } from './state/routes';

export function PurchaseBurst({ route, refresh }: { route: WorkspaceRoute; refresh(): void }) {
  const [count, setCount] = useState('5');
  const [amount, setAmount] = useState('10.00');
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<PurchaseBatchResponse | null>(null);
  const [message, setMessage] = useState('');
  return (
    <article>
      <h2>Same-customer burst</h2>
      <p>
        Customer: {route.customerId || 'Select a customer first.'} · Each purchase uses the same
        fixed amount. Allocated customer sequences demonstrate source ordering; Kafka ordering is
        partition-scoped.
      </p>
      <form
        onSubmit={async (event) => {
          event.preventDefault();
          if (busy || !route.customerId) return;
          let cents: number;
          const size = Number(count);
          try {
            cents = parseAmountCents(amount);
            if (
              cents < 100 ||
              cents > 10000000 ||
              !Number.isInteger(size) ||
              size < 1 ||
              size > 500
            )
              throw new Error('Use 1–500 purchases and an amount from 1.00 to 100,000.00.');
          } catch (error) {
            setMessage(error instanceof Error ? error.message : String(error));
            return;
          }
          setBusy(true);
          setResult(null);
          setMessage('Request in progress…');
          try {
            const response = await api.createPurchaseBatch({
              customerId: route.customerId,
              count: size,
              minAmountCents: cents,
              maxAmountCents: cents,
            });
            setResult(response);
            setMessage(
              `Source created ${response.totalCreated} purchases. Observe consumer receipts separately.`,
            );
            refresh();
          } catch (error) {
            setMessage(
              error instanceof ApiError
                ? `HTTP ${error.status}: ${error.message}`
                : 'Outcome unknown. Inspect persisted history before submitting another batch.',
            );
          } finally {
            setBusy(false);
          }
        }}
      >
        <label>
          Purchase count{' '}
          <input
            type="number"
            min="1"
            max="500"
            step="1"
            value={count}
            disabled={busy}
            onChange={(e) => setCount(e.target.value)}
            required
          />
        </label>
        <label>
          Fixed amount{' '}
          <input
            inputMode="decimal"
            value={amount}
            disabled={busy}
            onChange={(e) => setAmount(e.target.value)}
            required
          />
        </label>
        <button disabled={busy || !route.customerId}>Create burst</button>
      </form>
      <p>
        Batch requests have no idempotency key. Every submission is a new batch; an uncertain
        outcome is unsafe to automatically retry.
      </p>
      <p role="status">{message}</p>
      {result && (
        <div className="real-table">
          <table>
            <thead>
              <tr>
                <th>Purchase</th>
                <th>Allocated sequence</th>
                <th>Event</th>
              </tr>
            </thead>
            <tbody>
              {result.purchases.map((item) => (
                <tr key={item.purchaseId}>
                  <td>
                    <a
                      href={routeHash({
                        ...route,
                        page: 'events',
                        real: true,
                        purchaseId: item.purchaseId,
                      })}
                    >
                      {item.purchaseId}
                    </a>
                  </td>
                  <td>{item.customerSequence}</td>
                  <td>{item.eventId}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </article>
  );
}

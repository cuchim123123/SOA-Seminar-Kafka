import { useState } from 'react';
import { businessApi } from '../api/business';
import type { PurchaseHistoryItem } from '../api/business';
import { request, ApiError } from '../api/http';
import { useLiveResource } from './hooks/useLiveResource';

function Receipt({ context, eventId }: { context: 'loyalty' | 'analytics'; eventId: string }) {
  const resource = useLiveResource(context, `receipt:${eventId}`, (signal) =>
    businessApi.getReceipts(context, [eventId], signal),
  );
  const receipt = resource.data?.receipts.find((item) => item.eventId === eventId);
  return (
    <div>
      <strong>{context}</strong>:{' '}
      {resource.error
        ? `Unavailable: ${resource.error}`
        : resource.loading && !resource.data
          ? 'Loading…'
          : receipt
            ? `Receipt observed at ${receipt.processedAt}`
            : 'Not yet observed'}
      <p>
        Push: {resource.connection} · {resource.stale ? 'May be stale' : 'Connected'} · Observed:{' '}
        {resource.observedAt ?? 'Not yet observed'}
      </p>
      <button onClick={resource.refresh}>Refresh {context} receipt</button>
    </div>
  );
}

export function PurchaseEvidence({
  item,
  refresh,
}: {
  item: PurchaseHistoryItem;
  refresh(): void;
}) {
  const [reason, setReason] = useState('DEMO_INVALIDATION');
  const [confirmed, setConfirmed] = useState(false);
  const [busy, setBusy] = useState(false);
  const [outcome, setOutcome] = useState('');
  return (
    <article>
      <h2>Persisted event evidence</h2>
      <p>
        Purchase {item.purchaseId} · Customer {item.customerId}
      </p>
      <h3>Original completion</h3>
      <p>
        Event: {item.completionEventId ?? 'Unavailable'} · Sequence: {item.completionSequence}
      </p>
      {item.completionEventId && (
        <>
          <Receipt context="loyalty" eventId={item.completionEventId} />
          <Receipt context="analytics" eventId={item.completionEventId} />
        </>
      )}
      <h3>Invalidation</h3>
      {item.invalidationEventId ? (
        <>
          <p>
            Event: {item.invalidationEventId} · Sequence: {item.invalidationSequence} · Reason:{' '}
            {item.invalidationReason}
          </p>
          <Receipt context="loyalty" eventId={item.invalidationEventId} />
          <p>
            Analytics does not apply invalidations by design. Gross sales totals remain unchanged.
          </p>
        </>
      ) : (
        <p>No invalidation event observed in source history.</p>
      )}
      <p>
        A receipt proves the handler database transaction committed. Its later Kafka offset commit,
        partition and offset are not exposed here. An absent receipt is not proof of processing
        failure.
      </p>
      <form
        onSubmit={async (event) => {
          event.preventDefault();
          if (!confirmed || busy || !reason.trim()) return;
          setBusy(true);
          try {
            const result = await request<{ eventId: string; isAlreadyInvalidated: boolean }>(
              `/api/producer/api/v1/purchases/${encodeURIComponent(item.purchaseId)}/invalidate`,
              { method: 'POST', body: JSON.stringify({ reason: reason.trim() }) },
            );
            setOutcome(
              result.isAlreadyInvalidated
                ? `Already invalidated; existing event ${result.eventId}.`
                : `Invalidation accepted: ${result.eventId}. Await the Loyalty receipt and correction flag before replay.`,
            );
            refresh();
          } catch (error) {
            setOutcome(
              error instanceof ApiError
                ? `HTTP ${error.status}: ${error.message}`
                : 'Outcome unknown. Refresh source evidence before retrying.',
            );
          } finally {
            setBusy(false);
            setConfirmed(false);
          }
        }}
      >
        <label>
          Invalidation reason{' '}
          <select
            value={reason}
            disabled={busy}
            onChange={(e) => {
              setReason(e.target.value);
              setConfirmed(false);
            }}
            required
          >
            <option value="DEMO_INVALIDATION">Demo invalidation</option>
            <option value="FRAUD_CONFIRMED">Fraud confirmed (synthetic fixture)</option>
          </select>
        </label>
        <p>
          Invalidation marks this source purchase invalid and asks Loyalty to require correction. It
          does not immediately subtract points or change Analytics. Replay and global promotion are
          separate actions.
        </p>
        <label>
          <input
            type="checkbox"
            checked={confirmed}
            disabled={busy}
            onChange={(e) => setConfirmed(e.target.checked)}
          />
          I confirm invalidation of purchase {item.purchaseId}.
        </label>
        <button disabled={!confirmed || busy || !reason.trim()}>
          {busy
            ? 'Submitting…'
            : item.status === 'INVALIDATED'
              ? 'Confirm repeated invalidation'
              : 'Confirm invalidation'}
        </button>
      </form>
      <p role="status">{outcome}</p>
    </article>
  );
}

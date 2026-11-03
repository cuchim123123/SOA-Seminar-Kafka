import { useEffect, useRef, useState } from 'react';
import { useActionLifetime } from './hooks/useActionLifetime';
import { businessApi } from '../api/business';
import { api } from '../api/client';
import type {
  CustomerComparisonDto,
  ReplayRunDto,
  StagingCustomerRecord,
  CutoverResult,
} from '../api/client';
import { ApiError, request } from '../api/http';
import { useLiveResource } from './hooks/useLiveResource';
import { changeSubscriptions } from './state/change-subscriptions';
import { expectedPromotion, mapReview } from './state/replay-review';
import { routeHash } from './state/routes';
import { parseRoute } from './state/routes';
import type { WorkspaceRoute } from './state/routes';

export function Recovery({ route }: { route: WorkspaceRoute }) {
  const captureAction = useActionLifetime();
  const stillViewingRun = (runId: string) => {
    const current = parseRoute(window.location.hash);
    return current.real && current.page === 'recovery' && current.runId === runId;
  };
  const [runInput, setRunInput] = useState(route.runId);
  const [intent, setIntent] = useState<string>(() => crypto.randomUUID());
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');
  const [opened, setOpened] = useState(false);
  const [ack, setAck] = useState(false);
  const validReview = useRef(false);
  const [loyaltyConnection, setLoyaltyConnection] = useState('connecting');
  const base = `/api/replay/api/v1/replay/runs/${encodeURIComponent(route.runId)}`;
  const barrier = useLiveResource('loyalty', `barrier:${route.purchaseId}`, async (signal) => {
    if (!route.purchaseId) return false;
    const source = await businessApi.listPurchases(
      { purchaseId: route.purchaseId, limit: 1 },
      signal,
    );
    const item = source.items[0];
    if (!item?.invalidationEventId || !item.invalidationSequence) return false;
    const [receipts, account] = await Promise.all([
      businessApi.getReceipts('loyalty', [item.invalidationEventId], signal),
      api.getLoyaltyAccount(item.customerId, signal),
    ]);
    return (
      !!account?.correctionRequired &&
      account.lastCustomerSequence !== undefined &&
      BigInt(account.lastCustomerSequence) >= BigInt(item.invalidationSequence) &&
      receipts.receipts.some((row) => row.eventId === item.invalidationEventId)
    );
  });
  const review = useLiveResource('replay', `review:${route.runId}`, async (signal) => {
    validReview.current = false;
    setAck(false);
    if (!route.runId) return null;
    const run = await request<ReplayRunDto>(base, { signal });
    const staged = await request<StagingCustomerRecord[]>(`${base}/staging`, { signal });
    const rows = await mapReview(staged, (item) =>
      request<CustomerComparisonDto>(`${base}/comparison/${encodeURIComponent(item.customerId)}`, {
        signal,
      }),
    );
    for (const [index, row] of rows.entries()) {
      if (row.runId !== route.runId || row.customerId !== staged[index].customerId || !row.staged)
        throw new Error('Comparison identity does not match staging. Review incomplete.');
      expectedPromotion(row);
    }
    if (signal.aborted) throw new Error('Review superseded.');
    if (new Set(rows.map((row) => row.customerId)).size !== staged.length)
      throw new Error('Staging review contains duplicate or missing customers.');
    validReview.current = true;
    return { run, rows };
  });
  const refresh = useRef(review.refresh);
  refresh.current = review.refresh;
  useEffect(
    () =>
      changeSubscriptions.subscribe('loyalty', {
        changed: () => {
          validReview.current = false;
          setAck(false);
          refresh.current();
        },
        connection: (state) => {
          setLoyaltyConnection(state);
          if (state !== 'live') {
            validReview.current = false;
            setAck(false);
          }
        },
      }),
    [route.runId],
  );
  useEffect(() => {
    setOpened(false);
    setAck(false);
    setRunInput(route.runId);
  }, [route.runId]);
  const rows = review.data?.rows ?? [];
  const selected = rows.find((row) => row.customerId === route.customerId);
  const eligible = rows.filter((row) => expectedPromotion(row) !== 'skip').length;
  const ready =
    !!route.runId &&
    review.data?.run.status === 'COMPLETED' &&
    rows.length > 0 &&
    eligible > 0 &&
    !review.loading &&
    !review.error &&
    !review.stale &&
    loyaltyConnection === 'live' &&
    validReview.current;
  return (
    <>
      <article>
        <h2>Start a replay</h2>
        <p>
          Selected purchase:{' '}
          {route.purchaseId || 'Select and invalidate a purchase in Purchases first.'}
        </p>
        <p>
          Invalidation barrier:{' '}
          {barrier.error
            ? `Unavailable: ${barrier.error}`
            : barrier.data
              ? 'Loyalty receipt, correction flag and sequence observed'
              : 'Not yet observed'}
          . {barrier.stale && 'May be stale.'}
        </p>
        <div className="real-actions">
          <button onClick={barrier.refresh}>Refresh barrier</button>
          <button
            className="real-primary"
            disabled={busy || !!route.runId || !barrier.data || barrier.stale || barrier.loading}
            onClick={async () => {
              const isCurrent = captureAction();
              setBusy(true);
              setMessage(
                'Replay request in progress. The server scan continues if you leave this page.',
              );
              window.location.hash = routeHash({ ...route, runId: intent });
              try {
                await api.startReplay({ runId: intent });
                if (!isCurrent() || !stillViewingRun(intent)) return;
                setMessage('Replay request completed; read persisted run and staging.');
              } catch (error) {
                if (!isCurrent() || !stillViewingRun(intent)) return;
                setMessage(
                  `Replay request failed or outcome unknown: ${error instanceof Error ? error.message : String(error)}. Recover the retained run ID; do not start a duplicate.`,
                );
              } finally {
                setBusy(false);
                if (isCurrent() && stillViewingRun(intent)) refresh.current();
              }
            }}
          >
            Start bounded replay
          </button>
          {route.runId && (
            <button
              disabled={busy}
              onClick={() => {
                setIntent(crypto.randomUUID());
                window.location.hash = routeHash({ ...route, runId: '' });
              }}
            >
              Prepare new run
            </button>
          )}
        </div>
        {route.runId && <p>Selected run: {route.runId}</p>}
        <p role="status">{message}</p>
      </article>
      <article>
        <h2>Recover an existing run</h2>
        <form
          onSubmit={(event) => {
            event.preventDefault();
            window.location.hash = routeHash({ ...route, runId: runInput.trim() });
            review.refresh();
          }}
        >
          <label>
            Recover run by ID{' '}
            <input
              value={runInput}
              disabled={busy}
              onChange={(e) => setRunInput(e.target.value)}
              required
            />
          </label>
          <button disabled={busy}>Read persisted run</button>
        </form>
        <p>
          A transient404 during startup can precede persistence. Refresh this ID; do not
          automatically launch another replay. Scanned count is persisted state, not continuous
          progress.
        </p>
      </article>
      <article>
        <h2>Persisted replay state</h2>
        <p>
          Push: {review.connection} · Loyalty push: {loyaltyConnection} · Observed:{' '}
          {review.observedAt ?? 'Not observed'} · {review.stale ? 'May be stale' : 'Connected'}
        </p>
        {review.error && <p role="alert">Incomplete/unavailable review: {review.error}</p>}
        {review.loading && <p>Reading run and every staged customer…</p>}
        {review.data && (
          <>
            <p>
              Run {route.runId}: {review.data.run.status} · Scanned: {review.data.run.scannedCount}
            </p>
            <p>{review.data.run.errorMessage}</p>
            {review.data.run.status === 'CUTOVER_COMPLETED' && (
              <p>This run is already applied. No new promotion is requested.</p>
            )}
          </>
        )}
        <button
          onClick={() => {
            validReview.current = false;
            setAck(false);
            review.refresh();
          }}
        >
          Refresh complete review
        </button>
      </article>
      <article>
        <h2>Selected-customer correction preview</h2>
        <p>This filter does not limit global cutover scope.</p>
        {selected ? (
          <Comparison row={selected} />
        ) : (
          <p>No selected-customer staging comparison observed for this run.</p>
        )}
      </article>
      <article className="real-cutover">
        <h2>Global cutover for this replay run</h2>
        <p>
          All staged customers: {rows.length} · Expected eligible: {eligible} · Expected skipped:{' '}
          {rows.length - eligible}. Unchanged eligible values are still promoted and their
          correction flag cleared.
        </p>
        {rows.map((row) => (
          <Comparison key={row.customerId} row={row} />
        ))}
        <p>
          Preview is observational. Live writes may occur before execution; the backend rechecks
          sequence eligibility under the approved locks. Analytics remains unchanged. Actual
          execution returns only an applied count, not per-customer outcomes.
        </p>
        <button
          disabled={busy || !ready}
          onClick={() => {
            setOpened(true);
            setAck(false);
            validReview.current = false;
            review.refresh();
          }}
        >
          Open global confirmation and refresh review
        </button>
        {opened && (
          <section aria-label="Global cutover confirmation">
            <p>
              Apply this replay run globally to all eligible staged customers. The selected customer
              does not limit this action.
            </p>
            <label>
              <input
                type="checkbox"
                disabled={!ready || busy}
                checked={ack && ready}
                onChange={(e) => setAck(e.target.checked)}
              />
              I reviewed all {rows.length} staged customers for run {route.runId} and acknowledge
              global promotion.
            </label>
            <button
              disabled={!ack || !ready || busy}
              onClick={async () => {
                if (!validReview.current || !ack || !ready) return;
                const isCurrent = captureAction();
                validReview.current = false;
                setAck(false);
                setBusy(true);
                try {
                  const result = await request<CutoverResult>(`${base}/cutover`, {
                    method: 'POST',
                  });
                  if (!isCurrent() || !stillViewingRun(route.runId)) return;
                  setMessage(
                    `Server returned ${result.status}; applied customers: ${result.appliedCustomersCount}. Repeated completed cutover can return zero. Refreshed comparisons are post-action observations, not a per-customer execution audit.`,
                  );
                } catch (error) {
                  if (!isCurrent() || !stillViewingRun(route.runId)) return;
                  setMessage(
                    error instanceof ApiError && error.status >= 400 && error.status < 500
                      ? `Cutover rejected (HTTP ${error.status}): ${error.message}. Refresh the persisted run before retrying.`
                      : `Cutover failed or outcome unknown: ${error instanceof Error ? error.message : String(error)}. Read persisted run before retrying.`,
                  );
                } finally {
                  setBusy(false);
                  if (isCurrent() && stillViewingRun(route.runId)) {
                    setOpened(false);
                    review.refresh();
                    barrier.refresh();
                  }
                }
              }}
            >
              Confirm global cutover
            </button>
          </section>
        )}
      </article>
    </>
  );
}

function Comparison({ row }: { row: CustomerComparisonDto }) {
  return (
    <div>
      <h3>
        {row.customerId} — expected {expectedPromotion(row)}
      </h3>
      <div className="real-table">
        <table>
          <thead>
            <tr>
              <th>State</th>
              <th>Points</th>
              <th>Tier</th>
              <th>Eligible cents</th>
              <th>Sequence</th>
              <th>Correction required</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <th>Live</th>
              <td>{row.live?.points ?? 'No account'}</td>
              <td>{row.live?.tier ?? '—'}</td>
              <td>{row.live?.eligibleSpendCents ?? '—'}</td>
              <td>{row.live?.lastCustomerSequence ?? '—'}</td>
              <td>{row.live ? String(row.live.correctionRequired) : '—'}</td>
            </tr>
            <tr>
              <th>Staged</th>
              <td>{row.staged?.points}</td>
              <td>{row.staged?.tier}</td>
              <td>{row.staged?.eligibleSpendCents}</td>
              <td>{row.staged?.lastCustomerSequence}</td>
              <td>false if promoted</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

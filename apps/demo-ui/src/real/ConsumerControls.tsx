import { useRef, useState } from 'react';
import { api } from '../api/client';
import { request } from '../api/http';
import type { ConsumerStatusDto } from '../api/client';
import { useLiveResource } from './hooks/useLiveResource';

function WorkerControls({
  context,
  registerRefresh,
}: {
  context: 'loyalty' | 'analytics';
  registerRefresh: (refresh: () => void) => void;
}) {
  const base = `/api/${context}/api/v1/${context}/consumer`;
  const status = useLiveResource(context, 'controls', (signal) =>
    request<ConsumerStatusDto>(`${base}/status`, { signal }),
  );
  const groupId = status.data?.groupId;
  const topic = status.data?.topic;
  const lag = useLiveResource(
    null,
    JSON.stringify([groupId, topic]),
    (signal) =>
      groupId && topic ? api.getConsumerLag(groupId, topic, signal) : Promise.resolve(undefined),
    !!groupId && !!topic,
  );
  registerRefresh(() => {
    status.refresh();
    lag.refresh();
  });
  const [delay, setDelay] = useState('0');
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');
  const mutate = async (action: string, body?: object) => {
    if (busy || status.stale || status.loading) return;
    setBusy(true);
    try {
      await request(`${base}/${action}`, {
        method: 'POST',
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
      setMessage(`${action} accepted. Refreshing actual worker state.`);
    } catch (error) {
      setMessage(
        `Action failed or outcome unknown: ${error instanceof Error ? error.message : String(error)}. Read current status before retrying.`,
      );
    } finally {
      status.refresh();
      setBusy(false);
    }
  };
  return (
    <article className="consumer-card">
      <h2>{context === 'loyalty' ? 'Loyalty' : 'Analytics'} consumer</h2>
      <p>
        Status feed: {status.connection} · {status.stale ? 'May be stale' : 'Current'} · Observed:{' '}
        {status.observedAt ?? 'Not yet observed'}
      </p>
      {status.error && <p role="alert">Unavailable: {status.error}</p>}
      {status.data && (
        <>
          <p>
            Group: {status.data.groupId} · Topic: {status.data.topic}
          </p>
          <p>
            {status.data.isPaused ? 'Paused' : 'Not globally paused'} · Delay:{' '}
            {status.data.processingDelayMs ?? 'Unknown'} ms
          </p>
          <p>Paused partitions: {status.data.pausedPartitions?.join(', ') || 'None reported'}</p>
          <div className="consumer-evidence">
            <strong>Kafka group lag: {lag.data?.totalLag ?? '—'}</strong>
            <p>{lag.loading ? 'Reading broker offsets…' : 'Snapshot of committed offsets'}</p>
            {lag.observedAt && (
              <p>
                Observed: {lag.observedAt}
                {lag.error && ' · May be stale'}
              </p>
            )}
            {lag.error && <p role="alert">Lag read failed: {lag.error}</p>}
          </div>
          <button
            className="real-primary"
            disabled={busy || status.stale || status.loading}
            onClick={() =>
              void mutate(
                status.data?.isPaused || status.data?.pausedPartitions?.length ? 'resume' : 'pause',
              )
            }
          >
            {status.data.isPaused || status.data.pausedPartitions?.length ? 'Resume' : 'Pause'}{' '}
            {context}
          </button>
          <form
            onSubmit={(event) => {
              event.preventDefault();
              const value = Number(delay);
              if (Number.isInteger(value) && value >= 0 && value <= 30000)
                void mutate('delay', { delayMs: value });
            }}
          >
            <label>
              Processing delay (ms){' '}
              <input
                type="number"
                min="0"
                max="30000"
                step="1"
                value={delay}
                disabled={busy}
                onChange={(event) => setDelay(event.target.value)}
                required
              />
            </label>
            <button disabled={busy || status.stale || status.loading}>Set delay</button>
          </form>
        </>
      )}
      <p role="status">{message}</p>
    </article>
  );
}

export function ConsumerControls() {
  const refresh = useRef<Partial<Record<'loyalty' | 'analytics', () => void>>>({});
  return (
    <>
      <p>
        These controls affect the running application workers. Pause one group to demonstrate
        independent progress, then restore it. An in-flight handler may finish after pause. These
        are application controls, not separate deployed replicas or broker administration.
      </p>
      <div>
        <button onClick={() => Object.values(refresh.current).forEach((read) => read())}>
          Refresh evidence
        </button>
      </div>
      <div className="real-columns">
        <WorkerControls
          context="loyalty"
          registerRefresh={(read) => {
            refresh.current.loyalty = read;
          }}
        />
        <WorkerControls
          context="analytics"
          registerRefresh={(read) => {
            refresh.current.analytics = read;
          }}
        />
      </div>
      <p>
        Real poison injection and DLQ redrive are unavailable. Use existing logs and Grafana for
        aggregate retry/duplicate evidence.
      </p>
    </>
  );
}

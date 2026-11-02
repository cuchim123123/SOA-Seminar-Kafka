import { useState } from 'react';
import { request } from '../api/http';
import type { ConsumerStatusDto } from '../api/client';
import { useLiveResource } from './hooks/useLiveResource';

function WorkerControls({ context }: { context: 'loyalty' | 'analytics' }) {
  const base = `/api/${context}/api/v1/${context}/consumer`;
  const status = useLiveResource(context, 'controls', (signal) =>
    request<ConsumerStatusDto>(`${base}/status`, { signal }),
  );
  const [delay, setDelay] = useState('0');
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');
  const mutate = async (action: string, body?: object) => {
    if (busy) return;
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
    <article>
      <h2>{context} consumer</h2>
      <p>
        Push: {status.connection} · {status.stale ? 'May be stale' : 'Connected'} · Observed:{' '}
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
          <button disabled={busy || status.stale} onClick={() => void mutate('pause')}>
            Pause {context}
          </button>
          <button disabled={busy || status.stale} onClick={() => void mutate('resume')}>
            Resume {context}
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
            <button disabled={busy || status.stale}>Set delay</button>
          </form>
        </>
      )}
      <button onClick={status.refresh}>Refresh status</button>
      <p role="status">{message}</p>
    </article>
  );
}

export function ConsumerControls() {
  return (
    <>
      <h2>Independent consumer controls</h2>
      <p>
        These controls affect the running application workers. Pause one group to demonstrate
        independent progress, then restore it. An in-flight handler may finish after pause. These
        are application controls, not separate deployed replicas or broker administration.
      </p>
      <div className="real-columns">
        <WorkerControls context="loyalty" />
        <WorkerControls context="analytics" />
      </div>
      <p>
        Real poison injection and DLQ redrive are unavailable. Use existing logs and Grafana for
        aggregate retry/duplicate evidence.
      </p>
    </>
  );
}

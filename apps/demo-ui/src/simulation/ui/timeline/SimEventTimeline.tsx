import { useId, useState } from 'react';
import type { SimEvent } from '../../domain/events';
import { filterEvents } from '../../domain/event-filter';

export function SimEventTimeline({
  events,
  total = events.length,
}: {
  events: SimEvent[];
  total?: number;
}) {
  const [open, setOpen] = useState(false);
  const [type, setType] = useState(''),
    [group, setGroup] = useState(''),
    [query, setQuery] = useState('');
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const panelId = useId();
  const types = [...new Set(events.map((event) => event.type))].sort();
  const groups = [
    ...new Set(events.map((event) => String(event.metadata?.groupId ?? '')).filter(Boolean)),
  ].sort();
  const selected = events.find((event) => event.id === selectedId);
  const visible = filterEvents(events, type, group, query);
  return (
    <section className="sim-evidence" aria-label="Simulation evidence">
      <button
        className="sim-evidence-toggle"
        aria-expanded={open}
        aria-controls={panelId}
        onClick={() => setOpen(!open)}
      >
        Evidence{' '}
        <span>
          {events.length} retained / {total} recorded
        </span>
        <span>{open ? 'Hide' : 'Show'}</span>
      </button>
      {open && (
        <div id={panelId} className="sim-evidence-body">
          <div className="sim-evidence-filters">
            <label>
              Type
              <select value={type} onChange={(event) => setType(event.target.value)}>
                <option value="">All types</option>
                {[...new Set([...types, ...(type ? [type] : [])])].map((item) => (
                  <option key={item}>{item}</option>
                ))}
              </select>
            </label>
            <label>
              Group
              <select value={group} onChange={(event) => setGroup(event.target.value)}>
                <option value="">All groups</option>
                {[...new Set([...groups, ...(group ? [group] : [])])].map((item) => (
                  <option key={item}>{item}</option>
                ))}
              </select>
            </label>
            <input
              aria-label="Find event evidence"
              placeholder="Message ID, key or description"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
          </div>
          <div className="sim-evidence-columns">
            <div className="sim-evidence-list">
              {visible.map((event) => (
                <button
                  key={event.id}
                  aria-pressed={selectedId === event.id}
                  onClick={() => setSelectedId(event.id)}
                >
                  <time>{(event.scheduledTime / 1000).toFixed(3)} s</time>
                  <strong>{event.type}</strong>
                  <span>{event.description}</span>
                </button>
              ))}
              {visible.length === 0 && (
                <p className="sim-muted">
                  {events.length
                    ? 'No retained events match these filters.'
                    : 'Send an event, then Run or Step to inspect its journey.'}
                </p>
              )}
            </div>
            <div className="sim-evidence-detail">
              {selected ? (
                <>
                  <strong>{selected.type}</strong>
                  <p>{selected.description}</p>
                  <dl>
                    <dt>Event / message</dt>
                    <dd>
                      {selected.id}
                      <br />
                      {selected.message?.id ?? 'No message'}
                    </dd>
                    <dt>Group · topic · partition · record offset</dt>
                    <dd>
                      {String(selected.metadata?.groupId ?? '—')} ·{' '}
                      {String(selected.metadata?.topic ?? '—')} ·{' '}
                      {String(selected.metadata?.partitionId ?? '—')} ·{' '}
                      {String(selected.metadata?.offset ?? '—')}
                    </dd>
                  </dl>
                  <p className="sim-muted">
                    Record offset N is distinct from committed next offset N+1. Group offsets are
                    independent.
                  </p>
                  <pre>
                    {JSON.stringify(selected.message?.payload ?? selected.metadata ?? {}, null, 2)}
                  </pre>
                  <button
                    onClick={() => setQuery(selected.message?.id ?? '')}
                    disabled={!selected.message}
                  >
                    Show this message's retained journey
                  </button>
                </>
              ) : (
                <p className="sim-muted">
                  {selectedId
                    ? 'This event is outside the retained window. Select another event.'
                    : 'Select an event to inspect its message and processing evidence.'}
                </p>
              )}
              <p className="sim-muted">
                Only the latest 100 events are retained. Filters and message journeys cover this
                window, not a complete audit history.
              </p>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}

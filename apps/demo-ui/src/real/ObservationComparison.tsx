import { useState } from 'react';

type Values = Record<string, string | number | null>;

/** Two successful read observations; not a persisted audit ledger or event attribution. */
export function ObservationComparison({
  values,
  observedAt,
  unavailable,
}: {
  values: Values;
  observedAt: string | null;
  unavailable: boolean;
}) {
  const [before, setBefore] = useState<{ values: Values; at: string } | null>(null);
  return (
    <section aria-label="Before and after observations">
      <button
        disabled={unavailable || !observedAt}
        onClick={() => {
          if (observedAt && !unavailable) setBefore({ values: { ...values }, at: observedAt });
        }}
      >
        Capture before snapshot
      </button>
      {before && (
        <>
          <p>
            Browser observations: before {before.at}; current {observedAt ?? 'Unavailable'}.{' '}
            {unavailable && 'Current state may be stale.'} Changes may include other purchases or
            replay; this is not an event audit.
          </p>
          <div className="real-table">
            <table>
              <thead>
                <tr>
                  <th>Field</th>
                  <th>Before</th>
                  <th>Current</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(values).map(([name, value]) => (
                  <tr key={name}>
                    <th>{name}</th>
                    <td>{before.values[name] ?? 'Not observed'}</td>
                    <td>{value ?? 'Not observed'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <button onClick={() => setBefore(null)}>Clear snapshot</button>
        </>
      )}
    </section>
  );
}

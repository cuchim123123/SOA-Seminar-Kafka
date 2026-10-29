import React from 'react';

export interface LogEntry {
  id: string;
  timestamp: string;
  customerId: string;
  customerSequence?: number;
  purchaseId: string;
  status: string;
}

interface LiveEventLogProps {
  entries: LogEntry[];
  onClear: () => void;
}

export const LiveEventLog: React.FC<LiveEventLogProps> = ({ entries, onClear }) => {
  return (
    <section className="cyber-card" style={{ padding: '20px 24px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '8px' }}>
        <div>
          <h2 style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ color: 'var(--color-emerald)' }}>≡</span>
            Live Event Stream &amp; Outbox Relay Audit Log
          </h2>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem', marginTop: '2px' }}>
            Real-time feed of events processed through PostgreSQL transactional outbox and published to Apache Kafka.
          </p>
        </div>

        <button
          onClick={onClear}
          className="btn-cyber btn-outline"
          disabled={entries.length === 0}
        >
          Clear Log
        </button>
      </div>

      {entries.length === 0 ? (
        <div style={{ padding: '24px', textAlign: 'center', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)', fontSize: '0.825rem', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)' }}>
          No events recorded in current browser session. Use "Emit Purchase" or "Burst 5x" above to trigger live streaming traffic.
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
          {entries.slice(0, 10).map((item) => (
            <div
              key={item.id}
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                padding: '8px 12px',
                background: '#ffffff',
                border: '1px solid var(--border-subtle)',
                borderRadius: 'var(--radius-sm)',
                fontFamily: 'var(--font-mono)',
                fontSize: '0.75rem',
                flexWrap: 'wrap',
                gap: '8px',
                boxShadow: 'var(--shadow-sm)',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                <span style={{ color: 'var(--text-muted)' }}>{item.timestamp}</span>
                {item.customerSequence != null && (
                  <span className="badge-mono badge-violet">Seq #{item.customerSequence}</span>
                )}
                <span style={{ color: 'var(--color-cyan)', fontWeight: 600 }}>{item.customerId}</span>
                <span style={{ color: 'var(--text-body)' }}>Purchase: {item.purchaseId.slice(0, 14)}...</span>
              </div>

              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span className="badge-mono badge-emerald">{item.status}</span>
                <span style={{ color: 'var(--text-muted)', fontSize: '0.7rem' }}>Event ID: {item.id.slice(0, 12)}...</span>
              </div>
            </div>
          ))}
          {entries.length > 10 && (
            <div style={{ textAlign: 'center', fontSize: '0.75rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)', marginTop: '4px' }}>
              Showing latest 10 of {entries.length} events
            </div>
          )}
        </div>
      )}
    </section>
  );
};

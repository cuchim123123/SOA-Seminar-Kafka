import React from 'react';

export interface LogEntry {
  id: string;
  timestamp: string;
  customerId: string;
  customerSequence: number;
  purchaseId: string;
  status: string;
}

interface LiveEventLogProps {
  entries: LogEntry[];
  onClear: () => void;
}

export const LiveEventLog: React.FC<LiveEventLogProps> = ({ entries, onClear }) => {
  return (
    <section className="cyber-card" style={{ padding: '24px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '8px' }}>
        <div>
          <h2 style={{ fontSize: '1.15rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ color: 'var(--color-emerald)' }}>≡</span>
            LIVE EVENT STREAM &amp; OUTBOX RELAY AUDIT LOG
          </h2>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem' }}>
            Real-time feed of events processed through the transactional outbox and published to Apache Kafka.
          </p>
        </div>

        <button
          onClick={onClear}
          className="btn-cyber btn-outline"
          style={{ padding: '4px 10px', fontSize: '0.75rem' }}
          disabled={entries.length === 0}
        >
          Clear Log
        </button>
      </div>

      {entries.length === 0 ? (
        <div style={{ padding: '32px', textAlign: 'center', color: 'var(--text-dim)', fontFamily: 'var(--font-mono)', fontSize: '0.825rem' }}>
          No events recorded in current browser session. Use "Emit Purchase" or "Burst 5x" to trigger live stream traffic.
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', maxHeight: '280px', overflowY: 'auto' }}>
          {entries.map((item) => (
            <div
              key={item.id}
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                padding: '8px 12px',
                background: 'rgba(0,0,0,0.25)',
                border: '1px solid var(--border-subtle)',
                borderRadius: 'var(--radius-sm)',
                fontFamily: 'var(--font-mono)',
                fontSize: '0.75rem',
                flexWrap: 'wrap',
                gap: '8px',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                <span style={{ color: 'var(--text-dim)' }}>{item.timestamp}</span>
                <span className="badge-mono badge-violet">Seq #{item.customerSequence}</span>
                <span style={{ color: 'var(--color-cyan)', fontWeight: 600 }}>{item.customerId}</span>
                <span style={{ color: 'var(--text-muted)' }}>Purchase: {item.purchaseId.slice(0, 14)}...</span>
              </div>

              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span className="badge-mono badge-emerald">{item.status}</span>
                <span style={{ color: 'var(--text-dim)', fontSize: '0.7rem' }}>Event ID: {item.id.slice(0, 12)}...</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
};

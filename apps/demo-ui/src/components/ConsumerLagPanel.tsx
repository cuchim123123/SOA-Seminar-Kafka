import React, { useState } from 'react';
import { api } from '../api/client';
import type { ConsumerGroupLag, ConsumerStatusDto } from '../api/client';

interface ConsumerLagPanelProps {
  loyaltyLag: ConsumerGroupLag | null;
  analyticsLag: ConsumerGroupLag | null;
  analyticsStatus: ConsumerStatusDto | null;
  onStatusChanged: () => void;
}

export const ConsumerLagPanel: React.FC<ConsumerLagPanelProps> = ({
  loyaltyLag,
  analyticsLag,
  analyticsStatus,
  onStatusChanged,
}) => {
  const [toggling, setToggling] = useState<boolean>(false);
  const isPaused = analyticsStatus?.isPaused ?? false;

  const handleToggleConsumer = async () => {
    setToggling(true);
    try {
      if (isPaused) {
        await api.resumeAnalytics();
      } else {
        await api.pauseAnalytics();
      }
      onStatusChanged();
    } catch (err) {
      alert(`Consumer control error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setToggling(false);
    }
  };

  return (
    <section className="cyber-card" style={{ padding: '20px 24px', marginBottom: '20px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '8px' }}>
        <div>
          <h2 style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ color: 'var(--color-emerald)' }}>◷</span>
            Independent Consumer Groups &amp; Lag Telemetry
          </h2>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem', marginTop: '2px' }}>
            Real-time lag calculated from <code style={{ color: 'var(--color-cyan)', fontFamily: 'var(--font-mono)' }}>logEndOffset - committedOffset</code> per partition. Groups maintain completely independent offsets.
          </p>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <span className="badge-mono badge-cyan">Topic: purchase.events</span>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '16px' }}>
        
        {/* Consumer Group 1: Loyalty-v1 */}
        <div
          style={{
            padding: '16px',
            borderRadius: 'var(--radius-md)',
            background: '#ffffff',
            border: '1px solid var(--border-subtle)',
            boxShadow: 'var(--shadow-sm)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: 'var(--color-emerald)', display: 'inline-block' }} />
              <span style={{ fontWeight: 700, fontSize: '0.925rem', color: 'var(--text-main)', fontFamily: 'var(--font-mono)' }}>
                loyalty-v1
              </span>
            </div>
            <span className="badge-mono badge-emerald">RUNNING</span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px', padding: '8px 12px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)' }}>
            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>Total Unprocessed Lag:</span>
            <span style={{ fontSize: '1.1rem', fontWeight: 800, color: 'var(--color-emerald)', fontFamily: 'var(--font-mono)' }}>
              {loyaltyLag?.totalLag || '0'} <span style={{ fontSize: '0.75rem', fontWeight: 400, color: 'var(--text-muted)' }}>records</span>
            </span>
          </div>

          {/* Per-Partition Lag Breakdown */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
            {loyaltyLag?.partitions.map((pl) => {
              const lagNum = Number(pl.lag || '0');
              return (
                <div key={pl.partition} style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--text-body)', marginBottom: '2px' }}>
                    <span>P#{pl.partition} (Committed: {pl.committedOffset ?? 'Uncommitted'})</span>
                    <span style={{ color: lagNum > 0 ? 'var(--color-amber)' : 'var(--text-muted)', fontWeight: 600 }}>
                      Lag: {pl.lag}
                    </span>
                  </div>
                  <div style={{ height: '4px', background: '#f1f5f9', borderRadius: '2px', overflow: 'hidden' }}>
                    <div
                      style={{
                        height: '100%',
                        width: `${Math.min(100, lagNum * 15)}%`,
                        background: lagNum > 0 ? '#d97706' : '#059669',
                        transition: 'width 0.3s ease',
                      }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Consumer Group 2: Analytics-v1 (With Live Pause/Resume Control) */}
        <div
          style={{
            padding: '16px',
            borderRadius: 'var(--radius-md)',
            background: isPaused ? '#fffbeb' : '#ffffff',
            border: isPaused ? '1px solid #fde68a' : '1px solid var(--border-subtle)',
            boxShadow: 'var(--shadow-sm)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span
                style={{
                  width: '8px',
                  height: '8px',
                  borderRadius: '50%',
                  background: isPaused ? 'var(--color-amber)' : 'var(--color-cyan)',
                  display: 'inline-block',
                }}
              />
              <span style={{ fontWeight: 700, fontSize: '0.925rem', color: 'var(--text-main)', fontFamily: 'var(--font-mono)' }}>
                analytics-v1
              </span>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span className={`badge-mono ${isPaused ? 'badge-amber' : 'badge-cyan'}`}>
                {isPaused ? 'PAUSED' : 'RUNNING'}
              </span>

              <button
                onClick={handleToggleConsumer}
                disabled={toggling}
                className={`btn-cyber ${isPaused ? 'btn-emerald' : 'btn-amber'}`}
                style={{ padding: '4px 10px', fontSize: '0.75rem' }}
                title={isPaused ? 'Resume consumer to drain lag' : 'Pause consumer to observe lag accumulation'}
              >
                {toggling ? '...' : isPaused ? '▶ Resume Consumer' : '⏸ Pause Consumer'}
              </button>
            </div>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px', padding: '8px 12px', background: isPaused ? '#fef3c7' : 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)' }}>
            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>Total Unprocessed Lag:</span>
            <span style={{ fontSize: '1.1rem', fontWeight: 800, color: isPaused ? '#b45309' : '#0369a1', fontFamily: 'var(--font-mono)' }}>
              {analyticsLag?.totalLag || '0'} <span style={{ fontSize: '0.75rem', fontWeight: 400, color: 'var(--text-muted)' }}>records</span>
            </span>
          </div>

          {/* Per-Partition Lag Breakdown */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
            {analyticsLag?.partitions.map((pl) => {
              const lagNum = Number(pl.lag || '0');
              return (
                <div key={pl.partition} style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--text-body)', marginBottom: '2px' }}>
                    <span>P#{pl.partition} (Committed: {pl.committedOffset ?? 'Uncommitted'})</span>
                    <span style={{ color: lagNum > 0 ? '#b45309' : 'var(--text-muted)', fontWeight: 600 }}>
                      Lag: {pl.lag}
                    </span>
                  </div>
                  <div style={{ height: '4px', background: '#f1f5f9', borderRadius: '2px', overflow: 'hidden' }}>
                    <div
                      style={{
                        height: '100%',
                        width: `${Math.min(100, lagNum * 15)}%`,
                        background: lagNum > 0 ? '#d97706' : '#0284c7',
                        transition: 'width 0.3s ease',
                      }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </section>
  );
};

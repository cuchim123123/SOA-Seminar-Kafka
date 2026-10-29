import React, { useState } from 'react';
import { api } from '../api/client';
import type { CustomerComparisonDto, ReplayRunDto } from '../api/client';

export const ReplayStudio: React.FC = () => {
  const [running, setRunning] = useState<boolean>(false);
  const [replayRun, setReplayRun] = useState<ReplayRunDto | null>(null);
  const [comparison, setComparison] = useState<CustomerComparisonDto | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const handleStartReplay = async (simulateRetentionFailure: boolean = false) => {
    setRunning(true);
    setErrorMessage(null);
    setReplayRun(null);
    setComparison(null);

    try {
      // If simulating retention failure, require an offset earlier than possible (offset -1 or 0 when truncated)
      const options = simulateRetentionFailure
        ? {
            // Require partition 0 to have offset 0, but if we pass an impossible requirement or test retention
            requiredStartOffsets: { '0': '999999' }, // Impossible high required offset or test logic
          }
        : undefined;

      const run = await api.startReplay(options);
      setReplayRun(run);

      // Fetch comparison for U001
      if (run.status === 'COMPLETED') {
        try {
          const comp = await api.getComparison(run.id, 'U001');
          setComparison(comp);
        } catch {
          // May not have U001 events yet
        }
      }
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : String(err));
    } finally {
      setRunning(false);
    }
  };

  return (
    <section className="cyber-card" style={{ padding: '24px', marginBottom: '24px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '12px' }}>
        <div>
          <h2 style={{ fontSize: '1.15rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ color: 'var(--color-amber)' }}>↺</span>
            BOUNDED REPLAY &amp; TWO-PASS CORRECTION STUDIO (PHASE 5)
          </h2>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem' }}>
            Recompute accurate projections using an isolated on-demand group without touching live consumer group offsets. Reconstructs state via two-pass correction.
          </p>
        </div>

        <div style={{ display: 'flex', gap: '10px', alignItems: 'center', flexWrap: 'wrap' }}>
          <button
            onClick={() => handleStartReplay(false)}
            disabled={running}
            className="btn-cyber btn-violet"
          >
            {running ? 'Replaying...' : '↺ Start Bounded Replay'}
          </button>

          <button
            onClick={() => handleStartReplay(true)}
            disabled={running}
            className="btn-cyber btn-amber"
            title="Tests P5-04 retention horizon failure handling"
          >
            ⚠ Test Retention Horizon Failure
          </button>
        </div>
      </div>

      {errorMessage && (
        <div
          style={{
            padding: '12px 16px',
            borderRadius: 'var(--radius-sm)',
            background: 'rgba(239, 68, 68, 0.12)',
            border: '1px solid rgba(239, 68, 68, 0.35)',
            color: '#fca5a5',
            fontSize: '0.825rem',
            fontFamily: 'var(--font-mono)',
            marginBottom: '16px',
          }}
        >
          <div style={{ fontWeight: 700, marginBottom: '4px' }}>✕ Replay Execution Aborted</div>
          <div>{errorMessage}</div>
        </div>
      )}

      {replayRun && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          {/* Replay Run Telemetry */}
          <div
            style={{
              padding: '16px',
              borderRadius: 'var(--radius-sm)',
              background: 'rgba(0,0,0,0.25)',
              border: '1px solid var(--border-subtle)',
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
              gap: '12px',
              fontFamily: 'var(--font-mono)',
              fontSize: '0.8rem',
            }}
          >
            <div>
              <div style={{ color: 'var(--text-dim)' }}>Replay Run ID:</div>
              <div style={{ color: 'var(--color-cyan)', fontWeight: 600, marginTop: '2px' }}>
                {replayRun.id.slice(0, 18)}...
              </div>
            </div>

            <div>
              <div style={{ color: 'var(--text-dim)' }}>On-Demand Group:</div>
              <div style={{ color: 'var(--color-violet)', fontWeight: 600, marginTop: '2px' }}>
                {replayRun.groupId.slice(0, 20)}...
              </div>
            </div>

            <div>
              <div style={{ color: 'var(--text-dim)' }}>Status:</div>
              <div style={{ marginTop: '2px' }}>
                <span className={`badge-mono ${replayRun.status === 'COMPLETED' ? 'badge-emerald' : replayRun.status === 'FAILED' ? 'badge-crimson' : 'badge-amber'}`}>
                  {replayRun.status}
                </span>
              </div>
            </div>

            <div>
              <div style={{ color: 'var(--text-dim)' }}>Scanned Events:</div>
              <div style={{ color: 'var(--text-main)', fontWeight: 700, marginTop: '2px' }}>
                {replayRun.scannedCount} records
              </div>
            </div>
          </div>

          {/* Comparison Card (Live vs Replay Staging) */}
          {comparison && (
            <div
              className="cyber-card"
              style={{
                padding: '16px',
                background: 'rgba(139, 92, 246, 0.06)',
                border: '1px solid var(--border-glow-violet)',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px' }}>
                <span style={{ fontWeight: 700, fontSize: '0.9rem', color: 'var(--text-main)' }}>
                  Two-Pass Correction Discrepancy for Customer {comparison.customerId}
                </span>
                <span className="badge-mono badge-violet">Isolated in Staging</span>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '14px', fontFamily: 'var(--font-mono)' }}>
                <div style={{ padding: '12px', background: 'rgba(0,0,0,0.3)', borderRadius: 'var(--radius-sm)' }}>
                  <div style={{ color: 'var(--text-dim)', fontSize: '0.75rem' }}>Live Projection (Naive):</div>
                  <div style={{ fontSize: '1.25rem', fontWeight: 800, color: 'var(--color-amber)', marginTop: '4px' }}>
                    {comparison.live?.points || 0} pts
                  </div>
                  <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                    Tier: {comparison.live?.tier || 'N/A'} (Correction Flag: {comparison.live?.correctionRequired ? 'True' : 'False'})
                  </div>
                </div>

                <div style={{ padding: '12px', background: 'rgba(0,0,0,0.3)', borderRadius: 'var(--radius-sm)' }}>
                  <div style={{ color: 'var(--text-dim)', fontSize: '0.75rem' }}>Staged Projection (Corrected):</div>
                  <div style={{ fontSize: '1.25rem', fontWeight: 800, color: 'var(--color-emerald)', marginTop: '4px' }}>
                    {comparison.staged?.points || 0} pts
                  </div>
                  <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                    Tier: {comparison.staged?.tier || 'N/A'} (Scanned: {comparison.staged?.scannedEventCount} events)
                  </div>
                </div>

                <div style={{ padding: '12px', background: 'rgba(0,0,0,0.3)', borderRadius: 'var(--radius-sm)' }}>
                  <div style={{ color: 'var(--text-dim)', fontSize: '0.75rem' }}>Discrepancy:</div>
                  <div style={{ fontSize: '1.25rem', fontWeight: 800, color: 'var(--color-crimson)', marginTop: '4px' }}>
                    {comparison.discrepancy.pointsDifference} pts
                  </div>
                  <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', marginTop: '2px' }}>
                    Spend Diff: ${(comparison.discrepancy.spendDifferenceCents / 100).toFixed(2)}
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Mechanics Explanation */}
      <div
        style={{
          marginTop: '16px',
          padding: '12px 16px',
          borderRadius: 'var(--radius-sm)',
          background: 'rgba(6, 182, 212, 0.08)',
          border: '1px solid rgba(6, 182, 212, 0.25)',
          display: 'flex',
          alignItems: 'center',
          gap: '12px',
          fontSize: '0.8rem',
          color: 'var(--text-muted)',
        }}
      >
        <span style={{ fontSize: '1.1rem', color: 'var(--color-cyan)' }}>ℹ</span>
        <span>
          <strong style={{ color: 'var(--text-main)' }}>Replay vs. Catch-up:</strong> Replay uses an on-demand disposable consumer group with fixed high-watermarks. The live consumer groups (<code style={{ color: 'var(--color-cyan)', fontFamily: 'var(--font-mono)' }}>loyalty-v1</code> and <code style={{ color: 'var(--color-cyan)', fontFamily: 'var(--font-mono)' }}>analytics-v1</code>) continue processing without interruption.
        </span>
      </div>
    </section>
  );
};

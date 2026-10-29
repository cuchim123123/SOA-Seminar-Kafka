import React, { useState } from 'react';
import { api } from '../api/client';
import type { CustomerComparisonDto, ReplayRunDto, StagingCustomerRecord } from '../api/client';

export const ReplayStudio: React.FC = () => {
  const [running, setRunning] = useState<boolean>(false);
  const [replayRun, setReplayRun] = useState<ReplayRunDto | null>(null);
  const [comparison, setComparison] = useState<CustomerComparisonDto | null>(null);
  const [targetCustomer, setTargetCustomer] = useState<string>('U001');
  const [stagingRecords, setStagingRecords] = useState<StagingCustomerRecord[]>([]);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [comparing, setComparing] = useState<boolean>(false);

  const fetchCustomerComparison = async (runId: string, customerId: string) => {
    setComparing(true);
    try {
      const comp = await api.getComparison(runId, customerId);
      setComparison(comp);
    } catch {
      setComparison(null);
    } finally {
      setComparing(false);
    }
  };

  const handleStartReplay = async (simulateRetentionFailure: boolean = false) => {
    setRunning(true);
    setErrorMessage(null);
    setReplayRun(null);
    setComparison(null);
    setStagingRecords([]);

    try {
      const options = simulateRetentionFailure
        ? {
            requiredStartOffsets: { '0': '999999' },
          }
        : undefined;

      const run = await api.startReplay(options);
      setReplayRun(run);

      if (run.status === 'COMPLETED') {
        // Fetch comparison for target customer
        await fetchCustomerComparison(run.id, targetCustomer.trim() || 'U001');

        // Fetch top staging records
        try {
          const records = await api.getStagingRecords(run.id);
          setStagingRecords(records.slice(0, 15));
        } catch {
          // ignore
        }
      }
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : String(err));
    } finally {
      setRunning(false);
    }
  };

  const handleSelectCustomer = async (cust: string) => {
    setTargetCustomer(cust);
    if (replayRun?.id) {
      await fetchCustomerComparison(replayRun.id, cust);
    }
  };

  return (
    <section className="cyber-card" style={{ padding: '20px 24px', marginBottom: '20px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '12px' }}>
        <div>
          <h2 style={{ fontSize: '1.15rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ color: 'var(--color-amber)' }}>↺</span>
            Bounded Replay &amp; Two-Pass Correction Studio (Phase 5)
          </h2>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem', marginTop: '3px' }}>
            Recompute accurate projections using an isolated on-demand group without touching live consumer group offsets. Reconstructs state via two-pass correction.
          </p>
        </div>

        <div style={{ display: 'flex', gap: '8px', alignItems: 'center', flexWrap: 'wrap' }}>
          <button
            type="button"
            onClick={() => handleStartReplay(false)}
            disabled={running}
            className="btn-cyber btn-violet"
            style={{
              background: 'linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%)',
              color: '#ffffff',
              border: 'none',
              borderRadius: 'var(--radius-sm)',
              padding: '8px 16px',
              fontSize: '0.825rem',
              fontWeight: 600,
              cursor: running ? 'not-allowed' : 'pointer',
              boxShadow: 'var(--shadow-sm)',
            }}
          >
            {running ? 'Replaying Kafka Topic...' : '↺ Start Bounded Replay'}
          </button>

          <button
            type="button"
            onClick={() => handleStartReplay(true)}
            disabled={running}
            className="btn-cyber btn-amber"
            title="Tests P5-04 retention horizon failure handling"
            style={{
              background: '#fffbeb',
              color: '#92400e',
              border: '1px solid #fde68a',
              borderRadius: 'var(--radius-sm)',
              padding: '8px 14px',
              fontSize: '0.825rem',
              fontWeight: 600,
              cursor: running ? 'not-allowed' : 'pointer',
            }}
          >
            ⚠ Test Retention Failure
          </button>
        </div>
      </div>

      {errorMessage && (
        <div
          style={{
            padding: '12px 14px',
            borderRadius: 'var(--radius-sm)',
            background: 'var(--color-crimson-bg)',
            border: '1px solid var(--color-crimson-border)',
            color: '#991b1b',
            fontSize: '0.825rem',
            fontFamily: 'var(--font-mono)',
            marginBottom: '16px',
          }}
        >
          <div style={{ fontWeight: 700, marginBottom: '2px' }}>✕ Replay Execution Aborted</div>
          <div>{errorMessage}</div>
        </div>
      )}

      {replayRun && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          {/* Replay Run Telemetry */}
          <div
            style={{
              padding: '14px 18px',
              borderRadius: 'var(--radius-md)',
              background: 'var(--bg-subtle)',
              border: '1px solid var(--border-subtle)',
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
              gap: '12px',
              fontFamily: 'var(--font-mono)',
              fontSize: '0.8rem',
            }}
          >
            <div>
              <div style={{ color: 'var(--text-muted)' }}>Replay Run ID:</div>
              <div style={{ color: 'var(--color-cyan)', fontWeight: 600, marginTop: '2px' }}>
                {replayRun.id.slice(0, 18)}...
              </div>
            </div>

            <div>
              <div style={{ color: 'var(--text-muted)' }}>On-Demand Group:</div>
              <div style={{ color: 'var(--color-violet)', fontWeight: 600, marginTop: '2px' }}>
                {replayRun.groupId.slice(0, 22)}...
              </div>
            </div>

            <div>
              <div style={{ color: 'var(--text-muted)' }}>Status:</div>
              <div style={{ marginTop: '2px' }}>
                <span className={`badge-mono ${replayRun.status === 'COMPLETED' ? 'badge-emerald' : replayRun.status === 'FAILED' ? 'badge-crimson' : 'badge-amber'}`}>
                  {replayRun.status}
                </span>
              </div>
            </div>

            <div>
              <div style={{ color: 'var(--text-muted)' }}>Total Records Scanned:</div>
              <div style={{ color: 'var(--text-main)', fontWeight: 700, marginTop: '2px' }}>
                {replayRun.scannedCount} events
              </div>
            </div>
          </div>

          {/* Customer Selection & Comparison Inspector */}
          <div
            style={{
              padding: '16px 20px',
              borderRadius: 'var(--radius-md)',
              background: '#f5f3ff',
              border: '1px solid #ddd6fe',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '10px' }}>
              <div>
                <span style={{ fontWeight: 700, fontSize: '0.95rem', color: '#4c1d95' }}>
                  Two-Pass Correction Discrepancy Inspector
                </span>
                <p style={{ fontSize: '0.785rem', color: '#6d28d9', marginTop: '2px' }}>
                  Comparing live projection against replayed state reconstructed from Kafka historical events.
                </p>
              </div>

              {/* Customer Selector */}
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <label style={{ fontSize: '0.75rem', fontWeight: 600, color: '#5b21b6' }}>
                  Customer:
                </label>
                <div style={{ display: 'flex', gap: '4px' }}>
                  {['U001', 'U002', 'VIP_001'].map((c) => (
                    <button
                      key={c}
                      type="button"
                      onClick={() => handleSelectCustomer(c)}
                      style={{
                        padding: '3px 8px',
                        fontSize: '0.75rem',
                        fontFamily: 'var(--font-mono)',
                        borderRadius: 'var(--radius-sm)',
                        border: '1px solid #c4b5fd',
                        background: targetCustomer === c ? '#7c3aed' : '#ffffff',
                        color: targetCustomer === c ? '#ffffff' : '#5b21b6',
                        cursor: 'pointer',
                        fontWeight: 600,
                      }}
                    >
                      {c}
                    </button>
                  ))}
                </div>
                <input
                  type="text"
                  value={targetCustomer}
                  onChange={(e) => setTargetCustomer(e.target.value)}
                  placeholder="Other Customer ID..."
                  style={{
                    padding: '4px 8px',
                    borderRadius: 'var(--radius-sm)',
                    border: '1px solid #c4b5fd',
                    fontFamily: 'var(--font-mono)',
                    fontSize: '0.785rem',
                    width: '120px',
                    background: '#ffffff',
                  }}
                />
                <button
                  type="button"
                  onClick={() => fetchCustomerComparison(replayRun.id, targetCustomer.trim())}
                  disabled={comparing}
                  style={{
                    padding: '4px 10px',
                    borderRadius: 'var(--radius-sm)',
                    border: '1px solid #7c3aed',
                    background: '#7c3aed',
                    color: '#ffffff',
                    fontSize: '0.75rem',
                    fontWeight: 600,
                    cursor: 'pointer',
                  }}
                >
                  {comparing ? '...' : 'Compare'}
                </button>
              </div>
            </div>

            {comparison ? (
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '12px', fontFamily: 'var(--font-mono)' }}>
                {/* Live State */}
                <div style={{ padding: '14px', background: '#ffffff', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
                  <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', fontWeight: 600 }}>
                    Live DB Projection (Customer {comparison.customerId}):
                  </div>
                  <div style={{ fontSize: '1.4rem', fontWeight: 800, color: 'var(--color-amber)', marginTop: '4px' }}>
                    {comparison.live?.points ?? 0} pts
                  </div>
                  <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '4px' }}>
                    Tier: <strong style={{ color: 'var(--text-main)' }}>{comparison.live?.tier || 'N/A'}</strong>
                  </div>
                  <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '2px' }}>
                    Correction Flag: <strong style={{ color: comparison.live?.correctionRequired ? 'var(--color-crimson)' : 'var(--color-emerald)' }}>{comparison.live?.correctionRequired ? 'TRUE' : 'FALSE'}</strong>
                  </div>
                  <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '2px' }}>
                    Last Seq: #{comparison.live?.lastCustomerSequence ?? 0}
                  </div>
                </div>

                {/* Staged State */}
                <div style={{ padding: '14px', background: '#ffffff', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
                  <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', fontWeight: 600 }}>
                    Staged Projection (Replayed from Kafka):
                  </div>
                  <div style={{ fontSize: '1.4rem', fontWeight: 800, color: 'var(--color-emerald)', marginTop: '4px' }}>
                    {comparison.staged?.points ?? 0} pts
                  </div>
                  <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '4px' }}>
                    Tier: <strong style={{ color: 'var(--text-main)' }}>{comparison.staged?.tier || 'N/A'}</strong>
                  </div>
                  <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '2px' }}>
                    Scanned Events: <strong>{comparison.staged?.scannedEventCount ?? 0}</strong>
                  </div>
                  <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '2px' }}>
                    Replayed Seq: #{comparison.staged?.lastCustomerSequence ?? 0}
                  </div>
                </div>

                {/* Discrepancy */}
                <div style={{ padding: '14px', background: '#ffffff', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
                  <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', fontWeight: 600 }}>
                    Discrepancy:
                  </div>
                  <div style={{ fontSize: '1.4rem', fontWeight: 800, color: comparison.discrepancy.pointsDifference !== 0 ? 'var(--color-crimson)' : 'var(--color-emerald)', marginTop: '4px' }}>
                    {comparison.discrepancy.pointsDifference > 0 ? `+${comparison.discrepancy.pointsDifference}` : comparison.discrepancy.pointsDifference} pts
                  </div>
                  <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '4px' }}>
                    Spend Delta: ${(comparison.discrepancy.spendDifferenceCents / 100).toFixed(2)}
                  </div>
                  <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '2px' }}>
                    Tier Changed: <strong style={{ color: comparison.discrepancy.tierChanged ? 'var(--color-amber)' : 'var(--color-emerald)' }}>{comparison.discrepancy.tierChanged ? 'YES' : 'NO'}</strong>
                  </div>
                </div>
              </div>
            ) : (
              <div style={{ padding: '18px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '0.8rem', background: '#ffffff', borderRadius: 'var(--radius-sm)' }}>
                No events found for customer <strong>{targetCustomer}</strong> in this replay staging run. Select another customer above.
              </div>
            )}
          </div>

          {/* Staged Customers Table */}
          {stagingRecords.length > 0 && (
            <div style={{ background: '#ffffff', borderRadius: 'var(--radius-md)', border: '1px solid var(--border-subtle)', padding: '14px 18px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
                <span style={{ fontSize: '0.85rem', fontWeight: 700, color: 'var(--text-main)' }}>
                  Staged Customers in this Replay Run (Click to inspect):
                </span>
                <span className="badge-mono badge-cyan">{stagingRecords.length} Customers</span>
              </div>

              <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                {stagingRecords.map((r) => (
                  <button
                    key={r.customerId}
                    type="button"
                    onClick={() => handleSelectCustomer(r.customerId)}
                    style={{
                      padding: '5px 10px',
                      borderRadius: 'var(--radius-sm)',
                      border: targetCustomer === r.customerId ? '1px solid #7c3aed' : '1px solid var(--border-medium)',
                      background: targetCustomer === r.customerId ? '#f5f3ff' : '#ffffff',
                      color: targetCustomer === r.customerId ? '#7c3aed' : 'var(--text-body)',
                      fontSize: '0.75rem',
                      fontFamily: 'var(--font-mono)',
                      cursor: 'pointer',
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: '6px',
                    }}
                  >
                    <strong>{r.customerId}</strong>
                    <span style={{ color: 'var(--text-dim)', fontSize: '0.7rem' }}>({r.points} pts)</span>
                  </button>
                ))}
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
          background: 'var(--color-cyan-bg)',
          border: '1px solid var(--color-cyan-border)',
          display: 'flex',
          alignItems: 'center',
          gap: '10px',
          fontSize: '0.8rem',
          color: '#0369a1',
        }}
      >
        <span style={{ fontSize: '1.1rem', color: 'var(--color-cyan)' }}>ℹ</span>
        <span>
          <strong style={{ color: '#0f172a' }}>Replay Isolation Invariant:</strong> Replay uses an on-demand disposable consumer group with fixed high-watermarks. The live consumer groups (<code style={{ color: '#0369a1', fontFamily: 'var(--font-mono)', fontWeight: 600 }}>loyalty-v1</code> and <code style={{ color: '#0369a1', fontFamily: 'var(--font-mono)', fontWeight: 600 }}>analytics-v1</code>) continue processing in real-time without pause, lag impact, or offset disruption.
        </span>
      </div>
    </section>
  );
};

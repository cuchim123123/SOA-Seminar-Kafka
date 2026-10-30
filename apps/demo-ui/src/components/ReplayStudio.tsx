import React, { useState } from 'react';
import { api } from '../api/client';
import type { CustomerComparisonDto, ReplayRunDto, StagingCustomerRecord } from '../api/client';
import { ReplayComparisonCard } from './replay/ReplayComparisonCard';
import { ReplayStagingTable } from './replay/ReplayStagingTable';

export interface ReplayStudioProps {
  onCutoverCompleted?: () => void;
}

export const ReplayStudio: React.FC<ReplayStudioProps> = ({ onCutoverCompleted }) => {
  const [running, setRunning] = useState<boolean>(false);
  const [replayRun, setReplayRun] = useState<ReplayRunDto | null>(null);
  const [comparison, setComparison] = useState<CustomerComparisonDto | null>(null);
  const [targetCustomer, setTargetCustomer] = useState<string>('U001');
  const [stagingRecords, setStagingRecords] = useState<StagingCustomerRecord[]>([]);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [comparing, setComparing] = useState<boolean>(false);
  const [isCuttingOver, setIsCuttingOver] = useState<boolean>(false);
  const [cutoverSuccess, setCutoverSuccess] = useState<string | null>(null);

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
    setCutoverSuccess(null);
    setReplayRun(null);
    setComparison(null);
    setStagingRecords([]);

    try {
      const options = simulateRetentionFailure
        ? { requiredStartOffsets: { '0': '999999' } }
        : undefined;

      const run = await api.startReplay(options);
      setReplayRun(run);

      if (run.status === 'COMPLETED' || run.status === 'CUTOVER_COMPLETED') {
        await fetchCustomerComparison(run.id, targetCustomer.trim() || 'U001');
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

  const handleCutover = async () => {
    if (!replayRun) return;
    setIsCuttingOver(true);
    setCutoverSuccess(null);
    try {
      const res = await api.cutoverReplayRun(replayRun.id);
      setCutoverSuccess(
        `✓ Controlled Cutover Completed! Promoted ${res.appliedCustomersCount} customer records to live database. Live projection is now fully updated and reconciled!`,
      );
      await fetchCustomerComparison(replayRun.id, targetCustomer.trim() || 'U001');
      if (onCutoverCompleted) onCutoverCompleted();
    } catch (err) {
      alert(`Cutover failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setIsCuttingOver(false);
    }
  };

  return (
    <section className="cyber-card" style={{ padding: '20px 24px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '12px' }}>
        <div>
          <h2 style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ color: 'var(--color-primary)' }}>🔄</span>
            Phase 6: Two-Pass Bounded Replay Studio
          </h2>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem', marginTop: '2px' }}>
            Recompute accurate projections from Kafka historical immutable events without taking the live loyalty consumer offline.
          </p>
        </div>

        <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
          <button
            onClick={() => handleStartReplay(false)}
            disabled={running}
            className="btn-cyber btn-violet"
          >
            {running ? 'Replaying Stream...' : '▶ Start Bounded Replay'}
          </button>

          <button
            onClick={() => handleStartReplay(true)}
            disabled={running}
            className="btn-cyber btn-rose"
            title="Simulate insufficient retention by requesting a start offset that has already been pruned"
          >
            Simulate Retention Failure
          </button>
        </div>
      </div>

      {errorMessage && (
        <div style={{ padding: '12px 16px', borderRadius: 'var(--radius-sm)', background: '#fef2f2', border: '1px solid #fecaca', color: '#991b1b', fontSize: '0.825rem', marginBottom: '16px' }}>
          <strong>Error:</strong> {errorMessage}
        </div>
      )}

      {replayRun && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          {/* Metadata Card */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '12px', padding: '14px 16px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)', fontSize: '0.785rem', fontFamily: 'var(--font-mono)' }}>
            <div>
              <div style={{ color: 'var(--text-muted)' }}>Replay Run ID:</div>
              <div style={{ color: 'var(--text-main)', fontWeight: 600, marginTop: '2px' }}>{replayRun.id.slice(0, 18)}...</div>
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
              <div style={{ color: 'var(--text-main)', fontWeight: 700, marginTop: '2px' }}>{replayRun.scannedCount} events</div>
            </div>
          </div>

          {/* Comparison Discrepancy Card */}
          <ReplayComparisonCard
            comparison={comparison}
            targetCustomer={targetCustomer}
            setTargetCustomer={setTargetCustomer}
            comparing={comparing}
            onFetchComparison={(c) => fetchCustomerComparison(replayRun.id, c)}
            isCuttingOver={isCuttingOver}
            cutoverSuccess={cutoverSuccess}
            onCutover={handleCutover}
          />

          {/* Staging Records Table */}
          <ReplayStagingTable records={stagingRecords} />
        </div>
      )}
    </section>
  );
};

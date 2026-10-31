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
  const [retentionDetails, setRetentionDetails] = useState<string | null>(null);
  const [comparing, setComparing] = useState<boolean>(false);
  const [isCuttingOver, setIsCuttingOver] = useState<boolean>(false);
  const [cutoverSuccess, setCutoverSuccess] = useState<string | null>(null);
  const [demoStepMessage, setDemoStepMessage] = useState<string | null>(null);
  const [isDemoRunning, setIsDemoRunning] = useState<boolean>(false);

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
    setRetentionDetails(null);
    setCutoverSuccess(null);
    setReplayRun(null);
    setComparison(null);
    setStagingRecords([]);

    try {
      const options = simulateRetentionFailure ? { simulateRetentionFailure: true } : undefined;
      const run = await api.startReplay(options);
      setReplayRun(run);

      if (run.status === 'COMPLETED' || run.status === 'CUTOVER_COMPLETED') {
        await fetchCustomerComparison(run.id, targetCustomer.trim() || 'U001');
        try {
          const records = await api.getStagingRecords(run.id);
          setStagingRecords(records.slice(0, 15));
        } catch {
          // non-blocking staging fetch
        }
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setErrorMessage(msg);
      if (simulateRetentionFailure || msg.includes('InsufficientRetentionError') || msg.includes('retained history')) {
        setRetentionDetails(
          'Kafka broker log cleanup (retention.ms / deleteTopicRecords) pruned earliest records on Partition 0. Replay scanner halted explicitly to prevent building state from incomplete historical event logs.',
        );
      }
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

  // 1-Click End-to-End Stream Correction Demo
  const handleRunFullCorrectionDemo = async () => {
    setIsDemoRunning(true);
    const barrierTimeoutMs = 30000;
    setDemoStepMessage('Step 1/5: Emitting new purchase for U001 ($150)...');
    try {
      // 1. Emit purchase
      const purchaseRes = await api.createPurchase('U001', 15000);

      // 2. Invalidate; the assigned invalidation sequence gates the delivery barrier
      setDemoStepMessage(`Step 2/5: Invalidating purchase ${purchaseRes.purchaseId.slice(0, 12)}... (returning item)`);
      await new Promise((r) => setTimeout(r, 800));
      const invalidation = await api.invalidatePurchase(purchaseRes.purchaseId, 'CUSTOMER_RETURN_DEMO');

      // Barrier: wait until the live loyalty consumer has processed every event
      // through the invalidation sequence (proves Debezium published them to Kafka)
      // before capturing replay high-watermarks.
      setDemoStepMessage('Step 2.5/5: Waiting for live consumer to process events through CDC...');
      await api.waitForLoyaltyProcessed('U001', invalidation.customerSequence, barrierTimeoutMs);

      // 3. Trigger Bounded Replay
      setDemoStepMessage('Step 3/5: Running Two-Pass Bounded Replay (capturing high-watermarks)...');
      await new Promise((r) => setTimeout(r, 800));
      const run = await api.startReplay();
      setReplayRun(run);

      // 4. Fetch Staged Comparison
      setDemoStepMessage('Step 4/5: Computing discrepancy between live projection and replay staging...');
      await fetchCustomerComparison(run.id, 'U001');
      const records = await api.getStagingRecords(run.id).catch(() => []);
      setStagingRecords(records.slice(0, 15));

      setDemoStepMessage('Step 5/5: ✓ Invalidation discovered! Review the discrepancy below and execute Cutover.');
    } catch (err) {
      setDemoStepMessage(`✕ Demo workflow halted: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setIsDemoRunning(false);
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {/* 1. Header & Replay Workflow Stepper */}
      <div
        className="card-cyber"
        style={{
          background: 'linear-gradient(135deg, rgba(17, 24, 39, 0.95), rgba(30, 27, 75, 0.95))',
          border: '1px solid rgba(139, 92, 246, 0.3)',
          boxShadow: '0 0 25px rgba(139, 92, 246, 0.15)',
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '16px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '6px' }}>
              <span style={{ fontSize: '1.4rem' }}>↺</span>
              <h2 style={{ margin: 0, fontSize: '1.25rem', fontWeight: 800, color: '#f3f4f6', letterSpacing: '-0.02em' }}>
                Two-Pass Visual Bounded Replay Studio
              </h2>
              <span className="badge-mono badge-violet">Phase 4 Active</span>
            </div>
            <p style={{ margin: 0, fontSize: '0.85rem', color: '#9ca3af', maxWidth: '850px', lineHeight: 1.5 }}>
              Recompute corrected business state from Kafka historical events without mutating or taking live consumer groups offline.
              Pass 1 discovers invalidations; Pass 2 reconstructs true customer points and tiers in staging tables before controlled cutover.
            </p>
          </div>

          <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
            <button
              type="button"
              onClick={handleRunFullCorrectionDemo}
              disabled={isDemoRunning || running}
              className="btn-cyber btn-primary"
              style={{ fontSize: '0.8rem', padding: '8px 14px', fontWeight: 700 }}
            >
              {isDemoRunning ? 'Running Demo...' : '🚀 1-Click Correction Demo'}
            </button>
            <button
              type="button"
              onClick={() => handleStartReplay(false)}
              disabled={running || isDemoRunning}
              className="btn-cyber btn-violet"
              style={{ fontSize: '0.8rem', padding: '8px 14px', fontWeight: 700 }}
            >
              {running ? 'Replaying...' : '▶ Start Bounded Replay'}
            </button>
            <button
              type="button"
              onClick={() => handleStartReplay(true)}
              disabled={running || isDemoRunning}
              className="btn-cyber btn-rose"
              style={{ fontSize: '0.8rem', padding: '8px 14px' }}
              title="Demonstrate InsufficientRetentionError handling when Kafka broker retention has pruned history"
            >
              Simulate Retention Failure
            </button>
          </div>
        </div>

        {demoStepMessage && (
          <div
            style={{
              marginTop: '16px',
              padding: '10px 14px',
              borderRadius: 'var(--radius-sm)',
              background: demoStepMessage.startsWith('✕') ? 'rgba(239, 68, 68, 0.15)' : 'rgba(99, 102, 241, 0.15)',
              border: `1px solid ${demoStepMessage.startsWith('✕') ? '#ef4444' : 'var(--color-primary)'}`,
              color: demoStepMessage.startsWith('✕') ? '#fca5a5' : '#c7d2fe',
              fontFamily: 'var(--font-mono)',
              fontSize: '0.8rem',
            }}
          >
            {demoStepMessage}
          </div>
        )}

        {/* Replay Architectural Workflow Stepper */}
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))',
            gap: '8px',
            marginTop: '18px',
            paddingTop: '16px',
            borderTop: '1px solid rgba(255, 255, 255, 0.1)',
          }}
        >
          <div style={{ padding: '8px 10px', background: 'rgba(255, 255, 255, 0.04)', borderRadius: 'var(--radius-sm)' }}>
            <div style={{ fontSize: '0.675rem', color: '#a78bfa', fontWeight: 700 }}>1. WATERMARK PIN</div>
            <div style={{ fontSize: '0.725rem', color: '#e2e8f0', marginTop: '2px' }}>
              Captures HWM on 10 partitions; spawns ephemeral <code style={{ fontFamily: 'var(--font-mono)' }}>replay-&lt;uuid&gt;</code>.
            </div>
          </div>
          <div style={{ padding: '8px 10px', background: 'rgba(255, 255, 255, 0.04)', borderRadius: 'var(--radius-sm)' }}>
            <div style={{ fontSize: '0.675rem', color: '#38bdf8', fontWeight: 700 }}>2. PASS 1: DISCOVERY</div>
            <div style={{ fontSize: '0.725rem', color: '#e2e8f0', marginTop: '2px' }}>
              Scans historical log to collect all <code style={{ fontFamily: 'var(--font-mono)' }}>PurchaseInvalidated</code> events.
            </div>
          </div>
          <div style={{ padding: '8px 10px', background: 'rgba(255, 255, 255, 0.04)', borderRadius: 'var(--radius-sm)' }}>
            <div style={{ fontSize: '0.675rem', color: '#34d399', fontWeight: 700 }}>3. PASS 2: RECONSTRUCT</div>
            <div style={{ fontSize: '0.725rem', color: '#e2e8f0', marginTop: '2px' }}>
              Pure domain math recalculates true points &amp; tiers, omitting invalidated purchases.
            </div>
          </div>
          <div style={{ padding: '8px 10px', background: 'rgba(255, 255, 255, 0.04)', borderRadius: 'var(--radius-sm)' }}>
            <div style={{ fontSize: '0.675rem', color: '#fbbf24', fontWeight: 700 }}>4. STAGING ISOLATION</div>
            <div style={{ fontSize: '0.725rem', color: '#e2e8f0', marginTop: '2px' }}>
              Saves to <code style={{ fontFamily: 'var(--font-mono)' }}>recomputed_loyalty_accounts</code>. Live DB remains untouched.
            </div>
          </div>
          <div style={{ padding: '8px 10px', background: 'rgba(255, 255, 255, 0.04)', borderRadius: 'var(--radius-sm)' }}>
            <div style={{ fontSize: '0.675rem', color: '#f472b6', fontWeight: 700 }}>5. CONTROLLED CUTOVER</div>
            <div style={{ fontSize: '0.725rem', color: '#e2e8f0', marginTop: '2px' }}>
              Explicit confirmation promotes staging to live without repeating external side effects.
            </div>
          </div>
        </div>
      </div>

      {/* 2. Rebalance Isolation Diagnostic Banner */}
      <div
        style={{
          padding: '12px 16px',
          borderRadius: 'var(--radius-md)',
          background: '#f0fdf4',
          border: '1px solid #bbf7d0',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          flexWrap: 'wrap',
          gap: '12px',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <span style={{ fontSize: '1.2rem' }}>🛡️</span>
          <div>
            <div style={{ fontSize: '0.825rem', fontWeight: 700, color: '#166534' }}>
              Kafka Rebalance &amp; Offset Isolation Guarantee
            </div>
            <div style={{ fontSize: '0.75rem', color: '#15803d' }}>
              Bounded Replay uses on-demand consumer groups with independent metadata coordinators. Live groups{' '}
              <code style={{ fontFamily: 'var(--font-mono)', fontWeight: 600 }}>loyalty-v1</code> and{' '}
              <code style={{ fontFamily: 'var(--font-mono)', fontWeight: 600 }}>analytics-v1</code> experience 0 rebalances and 0 lag interruption.
            </div>
          </div>
        </div>
        <span className="badge-mono badge-emerald">ZERO REBALANCE IMPACT</span>
      </div>

      {/* 3. Retention Truncation Alert */}
      {errorMessage && (
        <div
          style={{
            padding: '14px 18px',
            borderRadius: 'var(--radius-md)',
            background: '#fef2f2',
            border: '2px solid #ef4444',
            color: '#991b1b',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
            <span style={{ fontSize: '1.2rem' }}>⚠️</span>
            <strong style={{ fontSize: '0.9rem' }}>Kafka Retention Horizon Diagnostic:</strong>
            <span className="badge-mono badge-crimson">InsufficientRetentionError</span>
          </div>
          <div style={{ fontFamily: 'var(--font-mono)', fontSize: '0.775rem', marginBottom: '8px' }}>
            {errorMessage}
          </div>
          {retentionDetails && (
            <p style={{ margin: 0, fontSize: '0.75rem', color: '#b91c1c', lineHeight: 1.4 }}>
              {retentionDetails}
            </p>
          )}
        </div>
      )}

      {/* 4. Active Replay Run Results & Discrepancy Diff */}
      {replayRun && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          {/* Metadata Grid */}
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
              gap: '12px',
              padding: '14px 16px',
              background: 'var(--bg-subtle)',
              borderRadius: 'var(--radius-sm)',
              fontSize: '0.785rem',
              fontFamily: 'var(--font-mono)',
              border: '1px solid var(--border-subtle)',
            }}
          >
            <div>
              <div style={{ color: 'var(--text-muted)' }}>Replay Group ID:</div>
              <div style={{ color: 'var(--text-main)', fontWeight: 600, marginTop: '2px' }}>
                {replayRun.groupId.slice(0, 24)}...
              </div>
            </div>
            <div>
              <div style={{ color: 'var(--text-muted)' }}>Status:</div>
              <div style={{ marginTop: '2px' }}>
                <span
                  className={`badge-mono ${
                    replayRun.status === 'COMPLETED'
                      ? 'badge-emerald'
                      : replayRun.status === 'CUTOVER_COMPLETED'
                        ? 'badge-cyan'
                        : replayRun.status === 'FAILED'
                          ? 'badge-crimson'
                          : 'badge-amber'
                  }`}
                >
                  {replayRun.status}
                </span>
              </div>
            </div>
            <div>
              <div style={{ color: 'var(--text-muted)' }}>Events Scanned:</div>
              <div style={{ color: 'var(--text-main)', fontWeight: 700, marginTop: '2px' }}>
                {replayRun.scannedCount} events
              </div>
            </div>
            <div>
              <div style={{ color: 'var(--text-muted)' }}>Partition Bounds:</div>
              <div style={{ color: 'var(--color-primary)', fontWeight: 600, marginTop: '2px' }}>
                10 Partitions (0..HWM)
              </div>
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
    </div>
  );
};

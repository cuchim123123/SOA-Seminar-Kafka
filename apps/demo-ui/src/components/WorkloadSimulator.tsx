import React, { useState, useEffect, useRef, useCallback } from 'react';
import { api } from '../api/client';
import type {
  TopicPartitionMetadata,
  ConsumerGroupLag,
  ConsumerStatusDto,
  PurchaseCreatedResponse,
} from '../api/client';

interface WorkloadSimulatorProps {
  topicMetadata: TopicPartitionMetadata | null;
  loyaltyLag: ConsumerGroupLag | null;
  analyticsLag: ConsumerGroupLag | null;
  loyaltyStatus: ConsumerStatusDto | null;
  analyticsStatus: ConsumerStatusDto | null;
  onRefreshTelemetry: () => Promise<void>;
  onEventEmitted?: (event: PurchaseCreatedResponse) => void;
}

interface WorkloadStats {
  totalSent: number;
  totalSuccess: number;
  totalFailed: number;
  durationSeconds: number;
  currentRate: number;
  partitionHits: Record<number, number>;
  customerHits: Record<string, number>;
}

export const WorkloadSimulator: React.FC<WorkloadSimulatorProps> = ({
  topicMetadata,
  loyaltyLag,
  analyticsLag,
  loyaltyStatus,
  analyticsStatus,
  onRefreshTelemetry,
  onEventEmitted,
}) => {
  // Workload settings
  const [distributionMode, setDistributionMode] = useState<'UNIFORM' | 'SKEWED' | 'EXTREME'>('UNIFORM');
  const [hotKeyCustomer, setHotKeyCustomer] = useState<string>('U001');
  const [rateLimit, setRateLimit] = useState<number>(5); // msgs per second
  const [isRunningContinuous, setIsRunningContinuous] = useState<boolean>(false);
  const [isExecutingBurst, setIsExecutingBurst] = useState<boolean>(false);

  // Chaos settings
  const [analyticsDelay, setAnalyticsDelay] = useState<number>(
    analyticsStatus?.processingDelayMs ?? 0,
  );
  const [poisonKey, setPoisonKey] = useState<string>('corrupt_key_demo');
  const [isInjectingPoison, setIsInjectingPoison] = useState<boolean>(false);
  const [actionStatus, setActionStatus] = useState<string | null>(null);

  // Statistics tracker
  const [stats, setStats] = useState<WorkloadStats>({
    totalSent: 0,
    totalSuccess: 0,
    totalFailed: 0,
    durationSeconds: 0,
    currentRate: 0,
    partitionHits: {},
    customerHits: {},
  });

  const continuousTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const secondCounterRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const sentInCurrentSecRef = useRef<number>(0);

  // Customer pool
  const customerPool = ['U001', 'U002', 'U003', 'U004', 'U005', 'U006', 'U007', 'U008', 'U009', 'U010'];

  // Select customer key based on active distribution policy
  const pickCustomer = useCallback((): string => {
    if (distributionMode === 'EXTREME') {
      return hotKeyCustomer;
    }
    if (distributionMode === 'SKEWED') {
      // 85% chance for hot key, 15% random among remaining
      if (Math.random() < 0.85) {
        return hotKeyCustomer;
      }
      const others = customerPool.filter((c) => c !== hotKeyCustomer);
      return others[Math.floor(Math.random() * others.length)];
    }
    // UNIFORM
    return customerPool[Math.floor(Math.random() * customerPool.length)];
  }, [distributionMode, hotKeyCustomer]);

  // Execute a single synthetic purchase
  const sendSinglePurchase = useCallback(
    async (customerId: string, amountDollars: number = 100) => {
      const amountCents = Math.round(amountDollars * 100);
      const idempotencyKey = `idem_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;

      try {
        const res = await api.createPurchase(customerId, amountCents, idempotencyKey);
        sentInCurrentSecRef.current += 1;

        setStats((prev) => {
          const newCustHits = { ...prev.customerHits };
          newCustHits[customerId] = (newCustHits[customerId] || 0) + 1;
          return {
            ...prev,
            totalSent: prev.totalSent + 1,
            totalSuccess: prev.totalSuccess + 1,
            customerHits: newCustHits,
          };
        });

        if (onEventEmitted) {
          onEventEmitted(res);
        }
        return true;
      } catch {
        setStats((prev) => ({
          ...prev,
          totalSent: prev.totalSent + 1,
          totalFailed: prev.totalFailed + 1,
        }));
        return false;
      }
    },
    [onEventEmitted],
  );

  // Continuous traffic generator loop
  useEffect(() => {
    if (isRunningContinuous) {
      const intervalMs = Math.max(40, Math.floor(1000 / rateLimit));

      continuousTimerRef.current = setInterval(() => {
        const cust = pickCustomer();
        const randAmount = Math.floor(Math.random() * 200) + 20;
        sendSinglePurchase(cust, randAmount);
      }, intervalMs);

      secondCounterRef.current = setInterval(() => {
        setStats((prev) => ({
          ...prev,
          durationSeconds: prev.durationSeconds + 1,
          currentRate: sentInCurrentSecRef.current,
        }));
        sentInCurrentSecRef.current = 0;
        onRefreshTelemetry();
      }, 1000);
    } else {
      if (continuousTimerRef.current) clearInterval(continuousTimerRef.current);
      if (secondCounterRef.current) clearInterval(secondCounterRef.current);
      setStats((prev) => ({ ...prev, currentRate: 0 }));
    }

    return () => {
      if (continuousTimerRef.current) clearInterval(continuousTimerRef.current);
      if (secondCounterRef.current) clearInterval(secondCounterRef.current);
    };
  }, [isRunningContinuous, rateLimit, pickCustomer, sendSinglePurchase, onRefreshTelemetry]);

  // Burst workload handler
  const handleTriggerBurst = async (count: number) => {
    setIsExecutingBurst(true);
    setActionStatus(`Executing burst of ${count} events (${distributionMode} profile)...`);

    const startTime = Date.now();
    let successes = 0;
    let failures = 0;

    for (let i = 0; i < count; i++) {
      const cust = pickCustomer();
      const randAmount = Math.floor(Math.random() * 250) + 15;
      const ok = await sendSinglePurchase(cust, randAmount);
      if (ok) successes++;
      else failures++;
    }

    const elapsed = Date.now() - startTime;
    setActionStatus(
      `✓ Burst complete: ${successes} emitted, ${failures} failed in ${elapsed}ms (${Math.round((count / (elapsed / 1000)) * 10) / 10} evt/s). Debezium CDC is publishing from the outbox...`,
    );
    setIsExecutingBurst(false);

    // Refresh telemetry after outbox processing
    setTimeout(async () => {
      await onRefreshTelemetry();
    }, 600);
  };

  // Chaos: Induce consumer delay
  const handleApplyConsumerDelay = async (delayMs: number) => {
    try {
      setAnalyticsDelay(delayMs);
      await api.setAnalyticsConsumerDelay(delayMs);
      setActionStatus(
        `⏱️ Analytics consumer delay updated to ${delayMs}ms. Processing will queue up lag under traffic!`,
      );
      await onRefreshTelemetry();
    } catch (err) {
      setActionStatus(`✕ Failed to update consumer delay: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  // Chaos: Pause / Resume consumer
  const handleToggleAnalyticsPause = async () => {
    const isPaused = analyticsStatus?.isPaused ?? false;
    try {
      if (isPaused) {
        await api.resumeAnalytics();
        setActionStatus('▶️ Analytics consumer resumed. Draining accumulated lag from Kafka partitions...');
      } else {
        await api.pauseAnalytics();
        setActionStatus('⏸️ Analytics consumer paused. Partitions will now accumulate real lag as events arrive!');
      }
      await onRefreshTelemetry();
    } catch (err) {
      setActionStatus(`✕ Consumer toggle failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  // Chaos: Inject poison pill
  const handleInjectPoison = async () => {
    setIsInjectingPoison(true);
    try {
      const res = await api.injectPoisonPill({ key: poisonKey.trim() || undefined });
      setActionStatus(
        `☠️ Poison Pill injected into partition ${res.partition}, offset ${res.offset} (Key: "${res.key}"). Kafka consumer caught PoisonPillError and safely committed N+1!`,
      );
      await onRefreshTelemetry();
    } catch (err) {
      setActionStatus(`✕ Poison injection failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setIsInjectingPoison(false);
    }
  };

  // Reset simulator statistics
  const handleResetStats = () => {
    setStats({
      totalSent: 0,
      totalSuccess: 0,
      totalFailed: 0,
      durationSeconds: 0,
      currentRate: 0,
      partitionHits: {},
      customerHits: {},
    });
    setActionStatus('Simulator session statistics reset.');
  };

  const partitions = topicMetadata?.partitions ?? [];
  const totalPartitions = partitions.length || 10;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {/* 1. Header Overview & Control Banner */}
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
              <span style={{ fontSize: '1.4rem' }}>⚡</span>
              <h2 style={{ margin: 0, fontSize: '1.25rem', fontWeight: 800, color: '#f3f4f6', letterSpacing: '-0.02em' }}>
                Workload Generation, Chaos &amp; Partition Skew Simulator
              </h2>
              <span className="badge-mono badge-violet">Phase 3 Active</span>
            </div>
            <p style={{ margin: 0, fontSize: '0.85rem', color: '#9ca3af', maxWidth: '850px', lineHeight: 1.5 }}>
              Generate synthetic multi-customer workloads to observe real-time Apache Kafka partition distributions,
              murmur2 key hashing invariants, and consumer lag accumulation under controlled traffic profiles.
            </p>
          </div>

          <div style={{ display: 'flex', gap: '8px' }}>
            <button
              type="button"
              onClick={handleResetStats}
              className="btn-cyber btn-outline"
              style={{ fontSize: '0.75rem', padding: '6px 12px' }}
            >
              Reset Session Counters
            </button>
          </div>
        </div>

        {actionStatus && (
          <div
            style={{
              marginTop: '16px',
              padding: '10px 14px',
              borderRadius: 'var(--radius-sm)',
              background: actionStatus.startsWith('✕') ? 'rgba(239, 68, 68, 0.15)' : 'rgba(16, 185, 129, 0.15)',
              border: `1px solid ${actionStatus.startsWith('✕') ? '#ef4444' : '#10b981'}`,
              color: actionStatus.startsWith('✕') ? '#fca5a5' : '#a7f3d0',
              fontFamily: 'var(--font-mono)',
              fontSize: '0.8rem',
            }}
          >
            {actionStatus}
          </div>
        )}
      </div>

      {/* 2. Main Simulation Grid: Workload Config & Live Rate Meters */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: '20px' }}>
        {/* Panel A: Traffic Profile & Distribution */}
        <div className="card-cyber">
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '14px' }}>
            <span style={{ fontSize: '1.1rem', color: 'var(--color-primary)' }}>🎯</span>
            <h3 style={{ margin: 0, fontSize: '0.95rem', fontWeight: 700, color: 'var(--text-body)' }}>
              1. Customer Key Distribution Profile
            </h3>
          </div>

          <p style={{ fontSize: '0.775rem', color: 'var(--text-muted)', marginBottom: '16px' }}>
            Kafka strictly guarantees ordering only <em>within a partition</em>. Keys are mapped deterministically via{' '}
            <code style={{ fontFamily: 'var(--font-mono)', color: 'var(--color-accent)' }}>murmur2(customerId) % {totalPartitions}</code>.
          </p>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '10px', marginBottom: '18px' }}>
            <label
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '10px',
                padding: '10px 14px',
                borderRadius: 'var(--radius-sm)',
                background: distributionMode === 'UNIFORM' ? 'rgba(99, 102, 241, 0.12)' : 'var(--bg-elevated)',
                border: `1px solid ${distributionMode === 'UNIFORM' ? 'var(--color-primary)' : 'var(--border-subtle)'}`,
                cursor: 'pointer',
              }}
            >
              <input
                type="radio"
                name="distribution"
                checked={distributionMode === 'UNIFORM'}
                onChange={() => setDistributionMode('UNIFORM')}
              />
              <div>
                <div style={{ fontWeight: 600, fontSize: '0.825rem', color: 'var(--text-body)' }}>
                  Uniform Distribution (Balanced)
                </div>
                <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)' }}>
                  Equal probability across 10 customers (U001..U010). Disperses records evenly across all {totalPartitions} partitions.
                </div>
              </div>
            </label>

            <label
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '10px',
                padding: '10px 14px',
                borderRadius: 'var(--radius-sm)',
                background: distributionMode === 'SKEWED' ? 'rgba(245, 158, 11, 0.12)' : 'var(--bg-elevated)',
                border: `1px solid ${distributionMode === 'SKEWED' ? '#f59e0b' : 'var(--border-subtle)'}`,
                cursor: 'pointer',
              }}
            >
              <input
                type="radio"
                name="distribution"
                checked={distributionMode === 'SKEWED'}
                onChange={() => setDistributionMode('SKEWED')}
              />
              <div>
                <div style={{ fontWeight: 600, fontSize: '0.825rem', color: '#b45309' }}>
                  Hot-Key Partition Skew (85% to Hot Key)
                </div>
                <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)' }}>
                  85% of traffic sent to {hotKeyCustomer}, 15% across remaining keys. Visualizes partition skew and hot-spotting!
                </div>
              </div>
            </label>

            <label
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '10px',
                padding: '10px 14px',
                borderRadius: 'var(--radius-sm)',
                background: distributionMode === 'EXTREME' ? 'rgba(239, 68, 68, 0.12)' : 'var(--bg-elevated)',
                border: `1px solid ${distributionMode === 'EXTREME' ? '#ef4444' : 'var(--border-subtle)'}`,
                cursor: 'pointer',
              }}
            >
              <input
                type="radio"
                name="distribution"
                checked={distributionMode === 'EXTREME'}
                onChange={() => setDistributionMode('EXTREME')}
              />
              <div>
                <div style={{ fontWeight: 600, fontSize: '0.825rem', color: '#b91c1c' }}>
                  Single Key Monopolization (100% {hotKeyCustomer})
                </div>
                <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)' }}>
                  All records sent to one customer. Proves 100% of records land on a single partition without ordering gaps.
                </div>
              </div>
            </label>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <span style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-muted)' }}>Hot Customer Key:</span>
            <select
              value={hotKeyCustomer}
              onChange={(e) => setHotKeyCustomer(e.target.value)}
              style={{
                padding: '6px 12px',
                borderRadius: 'var(--radius-sm)',
                border: '1px solid var(--border-medium)',
                fontFamily: 'var(--font-mono)',
                fontSize: '0.8rem',
                background: '#ffffff',
              }}
            >
              {customerPool.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Panel B: Traffic Generator Engines (Continuous & Burst) */}
        <div className="card-cyber">
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '14px' }}>
            <span style={{ fontSize: '1.1rem', color: 'var(--color-secondary)' }}>⚙️</span>
            <h3 style={{ margin: 0, fontSize: '0.95rem', fontWeight: 700, color: 'var(--text-body)' }}>
              2. Generator Controls &amp; Rate Meter
            </h3>
          </div>

          {/* Continuous Generator */}
          <div
            style={{
              padding: '14px',
              borderRadius: 'var(--radius-sm)',
              background: 'var(--bg-elevated)',
              border: '1px solid var(--border-subtle)',
              marginBottom: '16px',
            }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <span style={{ fontSize: '0.825rem', fontWeight: 600 }}>Continuous Workload Generator</span>
              <span className={`badge-mono ${isRunningContinuous ? 'badge-emerald' : 'badge-gray'}`}>
                {isRunningContinuous ? 'TRANSMITTING' : 'IDLE'}
              </span>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '12px' }}>
              <span style={{ fontSize: '0.775rem', color: 'var(--text-muted)', minWidth: '90px' }}>
                Rate: <strong>{rateLimit} msg/s</strong>
              </span>
              <input
                type="range"
                min="1"
                max="25"
                step="1"
                value={rateLimit}
                onChange={(e) => setRateLimit(Number(e.target.value))}
                style={{ flex: 1 }}
              />
            </div>

            <button
              type="button"
              onClick={() => setIsRunningContinuous(!isRunningContinuous)}
              className={`btn-cyber ${isRunningContinuous ? 'btn-amber' : 'btn-primary'}`}
              style={{ width: '100%', padding: '8px 16px', fontSize: '0.85rem' }}
            >
              {isRunningContinuous ? '⏸ Stop Continuous Workload' : '▶ Start Continuous Traffic'}
            </button>
          </div>

          {/* Burst Generator */}
          <div>
            <div style={{ fontSize: '0.825rem', fontWeight: 600, marginBottom: '8px' }}>
              Instant Workload Bursts:
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '8px' }}>
              <button
                type="button"
                disabled={isExecutingBurst}
                onClick={() => handleTriggerBurst(5)}
                className="btn-cyber btn-outline"
                style={{ padding: '8px 4px', fontSize: '0.75rem', fontWeight: 700 }}
              >
                +5 Burst
              </button>
              <button
                type="button"
                disabled={isExecutingBurst}
                onClick={() => handleTriggerBurst(10)}
                className="btn-cyber btn-outline"
                style={{ padding: '8px 4px', fontSize: '0.75rem', fontWeight: 700 }}
              >
                +10 Burst
              </button>
              <button
                type="button"
                disabled={isExecutingBurst}
                onClick={() => handleTriggerBurst(25)}
                className="btn-cyber btn-outline"
                style={{ padding: '8px 4px', fontSize: '0.75rem', fontWeight: 700 }}
              >
                +25 Workload
              </button>
              <button
                type="button"
                disabled={isExecutingBurst}
                onClick={() => handleTriggerBurst(50)}
                className="btn-cyber btn-outline"
                style={{ padding: '8px 4px', fontSize: '0.75rem', fontWeight: 700 }}
              >
                +50 Stress
              </button>
            </div>
          </div>

          {/* Live Metrics Grid */}
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(3, 1fr)',
              gap: '8px',
              marginTop: '16px',
              textAlign: 'center',
            }}
          >
            <div style={{ padding: '8px', background: 'var(--bg-elevated)', borderRadius: 'var(--radius-sm)' }}>
              <div style={{ fontSize: '0.675rem', color: 'var(--text-muted)' }}>ACHIEVED RATE</div>
              <div style={{ fontSize: '1.1rem', fontWeight: 800, color: 'var(--color-primary)' }}>
                {stats.currentRate} <span style={{ fontSize: '0.7rem' }}>evt/s</span>
              </div>
            </div>
            <div style={{ padding: '8px', background: 'var(--bg-elevated)', borderRadius: 'var(--radius-sm)' }}>
              <div style={{ fontSize: '0.675rem', color: 'var(--text-muted)' }}>TOTAL EMITTED</div>
              <div style={{ fontSize: '1.1rem', fontWeight: 800, color: 'var(--color-emerald)' }}>
                {stats.totalSuccess}
              </div>
            </div>
            <div style={{ padding: '8px', background: 'var(--bg-elevated)', borderRadius: 'var(--radius-sm)' }}>
              <div style={{ fontSize: '0.675rem', color: 'var(--text-muted)' }}>SESSION TIME</div>
              <div style={{ fontSize: '1.1rem', fontWeight: 800, color: 'var(--text-body)' }}>
                {stats.durationSeconds}s
              </div>
            </div>
          </div>
        </div>

        {/* Panel C: Chaos & Failure Injection Deck */}
        <div className="card-cyber">
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '14px' }}>
            <span style={{ fontSize: '1.1rem', color: '#ef4444' }}>☠️</span>
            <h3 style={{ margin: 0, fontSize: '0.95rem', fontWeight: 700, color: 'var(--text-body)' }}>
              3. Chaos &amp; Consumer Lag Experiments
            </h3>
          </div>

          {/* Consumer Lag Delay Slider */}
          <div
            style={{
              padding: '12px',
              borderRadius: 'var(--radius-sm)',
              background: '#fef2f2',
              border: '1px solid #fee2e2',
              marginBottom: '14px',
            }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
              <span style={{ fontSize: '0.8rem', fontWeight: 700, color: '#991b1b' }}>
                Analytics Worker Processing Delay
              </span>
              <span className="badge-mono badge-rose">{analyticsDelay}ms delay</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '8px' }}>
              <input
                type="range"
                min="0"
                max="1500"
                step="50"
                value={analyticsDelay}
                onChange={(e) => setAnalyticsDelay(Number(e.target.value))}
                style={{ flex: 1 }}
              />
              <button
                type="button"
                onClick={() => handleApplyConsumerDelay(analyticsDelay)}
                className="btn-cyber btn-rose"
                style={{ padding: '4px 10px', fontSize: '0.725rem' }}
              >
                Apply
              </button>
            </div>
            <div style={{ display: 'flex', gap: '6px' }}>
              {[0, 100, 300, 750].map((ms) => (
                <button
                  key={ms}
                  type="button"
                  onClick={() => handleApplyConsumerDelay(ms)}
                  className="btn-cyber btn-outline"
                  style={{ flex: 1, padding: '3px', fontSize: '0.675rem' }}
                >
                  {ms === 0 ? '0ms (Fast)' : `${ms}ms`}
                </button>
              ))}
            </div>
          </div>

          {/* Lag Accumulator Action Buttons */}
          <div style={{ display: 'flex', gap: '8px', marginBottom: '14px' }}>
            <button
              type="button"
              onClick={handleToggleAnalyticsPause}
              className={`btn-cyber ${analyticsStatus?.isPaused ? 'btn-emerald' : 'btn-amber'}`}
              style={{ flex: 1, padding: '8px', fontSize: '0.775rem', fontWeight: 700 }}
            >
              {analyticsStatus?.isPaused ? '▶ Resume Analytics Lag' : '⏸ Pause Analytics Lag'}
            </button>
            <button
              type="button"
              onClick={async () => {
                try {
                  await api.checkCdc();
                  await onRefreshTelemetry();
                  setActionStatus('✓ Debezium connector and tasks are running. Delivery is asynchronous.');
                } catch (err) {
                  setActionStatus(`✕ CDC check failed: ${err instanceof Error ? err.message : String(err)}`);
                }
              }}
              className="btn-cyber btn-outline"
              style={{ padding: '8px 12px', fontSize: '0.775rem' }}
            >
              Check CDC
            </button>
          </div>

          {/* Poison Pill Ingestion */}
          <div
            style={{
              padding: '12px',
              borderRadius: 'var(--radius-sm)',
              background: '#fff1f2',
              border: '1px solid #fecdd3',
            }}
          >
            <div style={{ fontSize: '0.8rem', fontWeight: 700, color: '#9f1239', marginBottom: '6px' }}>
              Corrupt Poison Pill Injection (Non-JSON payload)
            </div>
            <div style={{ display: 'flex', gap: '8px' }}>
              <input
                type="text"
                value={poisonKey}
                onChange={(e) => setPoisonKey(e.target.value)}
                placeholder="Key..."
                style={{
                  width: '120px',
                  padding: '6px 8px',
                  borderRadius: 'var(--radius-sm)',
                  border: '1px solid #f43f5e',
                  fontSize: '0.75rem',
                  fontFamily: 'var(--font-mono)',
                  background: '#ffffff',
                }}
              />
              <button
                type="button"
                disabled={isInjectingPoison}
                onClick={handleInjectPoison}
                className="btn-cyber btn-rose"
                style={{ flex: 1, padding: '6px 10px', fontSize: '0.75rem', fontWeight: 700 }}
              >
                {isInjectingPoison ? 'Injecting...' : '☠️ Inject Poison Pill'}
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* 3. Live Partition Distribution & Lag Heatmap */}
      <div className="card-cyber">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '14px', flexWrap: 'wrap', gap: '10px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ fontSize: '1.1rem', color: 'var(--color-primary)' }}>❖</span>
              <h3 style={{ margin: 0, fontSize: '0.95rem', fontWeight: 700, color: 'var(--text-body)' }}>
                Real-Time Kafka Partition Load &amp; Consumer Lag Heatmap
              </h3>
            </div>
            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
              Live broker high-watermarks and consumer group lag across all {totalPartitions} topic partitions.
            </div>
          </div>

          <div style={{ display: 'flex', gap: '14px', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
            <span style={{ color: loyaltyStatus?.isPaused ? '#ef4444' : 'var(--color-emerald)' }}>
              ● Loyalty Lag: <strong>{loyaltyLag?.totalLag ?? '0'}</strong> ({loyaltyStatus?.isPaused ? 'PAUSED' : 'ACTIVE'})
            </span>
            <span style={{ color: analyticsStatus?.isPaused ? '#ef4444' : '#f59e0b' }}>
              ● Analytics Lag: <strong>{analyticsLag?.totalLag ?? '0'}</strong> ({analyticsStatus?.isPaused ? 'PAUSED' : 'ACTIVE'})
            </span>
          </div>
        </div>

        {/* Partition Grid Cards */}
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(130px, 1fr))',
            gap: '10px',
          }}
        >
          {partitions.map((p) => {
            const loyaltyPartLag = loyaltyLag?.partitions.find((l) => l.partition === p.partition);
            const analyticsPartLag = analyticsLag?.partitions.find((l) => l.partition === p.partition);
            const aLagVal = Number(analyticsPartLag?.lag ?? 0);
            const lLagVal = Number(loyaltyPartLag?.lag ?? 0);

            const isLagging = aLagVal > 0 || lLagVal > 0;

            return (
              <div
                key={p.partition}
                style={{
                  padding: '10px',
                  borderRadius: 'var(--radius-sm)',
                  background: isLagging ? 'rgba(245, 158, 11, 0.08)' : 'var(--bg-elevated)',
                  border: `1px solid ${isLagging ? '#f59e0b' : 'var(--border-subtle)'}`,
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '4px',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span style={{ fontWeight: 700, fontSize: '0.8rem', color: 'var(--color-primary)' }}>
                    P{p.partition}
                  </span>
                  <span style={{ fontSize: '0.65rem', fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}>
                    L:{p.leader}
                  </span>
                </div>

                <div style={{ fontSize: '0.675rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                  HWM: <strong>{p.highWatermark}</strong>
                </div>

                <div style={{ fontSize: '0.675rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                  Msgs: <strong>{p.messageCount}</strong>
                </div>

                {/* Lag Indicators */}
                <div
                  style={{
                    marginTop: '6px',
                    paddingTop: '6px',
                    borderTop: '1px solid var(--border-subtle)',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '2px',
                    fontSize: '0.65rem',
                    fontFamily: 'var(--font-mono)',
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Loyalty:</span>
                    <span style={{ color: lLagVal > 0 ? '#f59e0b' : 'var(--color-emerald)', fontWeight: 600 }}>
                      {lLagVal} lag
                    </span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Analytics:</span>
                    <span style={{ color: aLagVal > 0 ? '#ef4444' : 'var(--color-emerald)', fontWeight: 700 }}>
                      {aLagVal} lag
                    </span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
};

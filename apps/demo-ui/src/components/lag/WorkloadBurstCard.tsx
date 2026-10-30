import React, { useState } from 'react';
import { api } from '../../api/client';
import type { PurchaseBatchResponse } from '../../api/client';

const BURST_PRESETS = [10, 25, 50, 100];

interface WorkloadBurstCardProps {
  onStatusChanged: () => void;
}

export const WorkloadBurstCard: React.FC<WorkloadBurstCardProps> = ({ onStatusChanged }) => {
  const [burstCount, setBurstCount] = useState<number>(25);
  const [burstCountInput, setBurstCountInput] = useState<string>('25');
  const [partitionMode, setPartitionMode] = useState<'balanced' | 'hot'>('balanced');
  const [hotCustomerId, setHotCustomerId] = useState<string>('cust_hot_spot');
  const [bursting, setBursting] = useState<boolean>(false);
  const [burstResult, setBurstResult] = useState<PurchaseBatchResponse | null>(null);
  const [burstError, setBurstError] = useState<string | null>(null);
  const [injectingPoison, setInjectingPoison] = useState<boolean>(false);
  const [poisonResult, setPoisonResult] = useState<string | null>(null);

  const handleLaunchBurst = async () => {
    const effectiveCount = Math.max(1, Math.min(500, parseInt(burstCountInput, 10) || burstCount || 25));
    setBurstCount(effectiveCount);
    setBurstCountInput(String(effectiveCount));
    setBursting(true);
    setBurstResult(null);
    setBurstError(null);
    try {
      const result = await api.createPurchaseBatch({
        count: effectiveCount,
        customerId: partitionMode === 'hot' ? hotCustomerId.trim() || 'cust_hot_spot' : undefined,
      });
      setBurstResult(result);
      onStatusChanged();

      setTimeout(onStatusChanged, 300);
      setTimeout(onStatusChanged, 800);
      setTimeout(onStatusChanged, 1500);
      setTimeout(onStatusChanged, 2500);
    } catch (err) {
      setBurstError(err instanceof Error ? err.message : String(err));
    } finally {
      setBursting(false);
    }
  };

  const handleInjectPoisonPill = async () => {
    setInjectingPoison(true);
    setPoisonResult(null);
    try {
      const key = partitionMode === 'hot' ? hotCustomerId.trim() || 'poison_hot_key' : `poison_pill_${Date.now()}`;
      const res = await api.injectPoisonPill({ key });
      setPoisonResult(
        `☠️ Injected poison pill into ${res.topic} partition ${res.partition} at offset ${res.offset} (key: "${res.key}"). Consumer isolated the invalid record and committed offset N+1 safely.`,
      );
      onStatusChanged();
      setTimeout(onStatusChanged, 400);
      setTimeout(onStatusChanged, 1200);
    } catch (err) {
      alert(`Poison pill injection failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setInjectingPoison(false);
    }
  };

  return (
    <section className="cyber-card" style={{ padding: '20px 24px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '1.1rem', color: 'var(--color-cyan)' }}>🚀</span>
          <span style={{ fontWeight: 700, fontSize: '0.95rem', color: 'var(--text-main)' }}>
            High-Throughput Workload Generator
          </span>
        </div>
        <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
          Appends directly to Transactional Outbox → KRaft Partitions
        </span>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: '16px', alignItems: 'center' }}>
        {/* Burst Count Controls */}
        <div>
          <label style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-body)', display: 'block', marginBottom: '6px' }}>
            Burst Size (Event Count):
          </label>
          <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', alignItems: 'center' }}>
            {BURST_PRESETS.map((preset) => (
              <button
                key={preset}
                type="button"
                onClick={() => {
                  setBurstCount(preset);
                  setBurstCountInput(String(preset));
                }}
                disabled={bursting}
                className={`btn-cyber ${burstCount === preset ? 'btn-cyan' : 'btn-ghost'}`}
                style={{ padding: '4px 10px', fontSize: '0.775rem' }}
              >
                {preset}
              </button>
            ))}
            <div style={{ display: 'flex', alignItems: 'center', gap: '4px', marginLeft: '4px' }}>
              <input
                type="text"
                inputMode="numeric"
                pattern="[0-9]*"
                value={burstCountInput}
                onChange={(e) => {
                  const clean = e.target.value.replace(/[^0-9]/g, '');
                  setBurstCountInput(clean);
                  const parsed = parseInt(clean, 10);
                  if (!isNaN(parsed) && parsed > 0) {
                    setBurstCount(Math.min(500, parsed));
                  }
                }}
                onBlur={() => {
                  const parsed = parseInt(burstCountInput, 10);
                  const val = isNaN(parsed) || parsed < 1 ? 1 : Math.min(500, parsed);
                  setBurstCount(val);
                  setBurstCountInput(String(val));
                }}
                disabled={bursting}
                placeholder="Count"
                style={{
                  width: '65px',
                  padding: '4px 8px',
                  fontSize: '0.8rem',
                  fontFamily: 'var(--font-mono)',
                  borderRadius: 'var(--radius-sm)',
                  border: '1px solid var(--border-subtle)',
                }}
              />
              <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>events (max 500)</span>
            </div>
          </div>
        </div>

        {/* Partitioning Strategy */}
        <div>
          <label style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-body)', display: 'block', marginBottom: '6px' }}>
            Partition Key Strategy:
          </label>
          <div style={{ display: 'flex', gap: '8px', alignItems: 'center', flexWrap: 'wrap' }}>
            <label
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                fontSize: '0.8rem',
                cursor: 'pointer',
                padding: '5px 10px',
                borderRadius: 'var(--radius-sm)',
                background: partitionMode === 'balanced' ? '#e0f2fe' : 'var(--bg-subtle)',
                border: partitionMode === 'balanced' ? '1px solid #7dd3fc' : '1px solid transparent',
                fontWeight: partitionMode === 'balanced' ? 600 : 400,
              }}
            >
              <input
                type="radio"
                name="partitionMode"
                checked={partitionMode === 'balanced'}
                onChange={() => setPartitionMode('balanced')}
                disabled={bursting}
              />
              🟢 Multi-Customer Balanced
            </label>

            <label
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                fontSize: '0.8rem',
                cursor: 'pointer',
                padding: '5px 10px',
                borderRadius: 'var(--radius-sm)',
                background: partitionMode === 'hot' ? '#ffedd5' : 'var(--bg-subtle)',
                border: partitionMode === 'hot' ? '1px solid #fed7aa' : '1px solid transparent',
                fontWeight: partitionMode === 'hot' ? 600 : 400,
              }}
            >
              <input
                type="radio"
                name="partitionMode"
                checked={partitionMode === 'hot'}
                onChange={() => setPartitionMode('hot')}
                disabled={bursting}
              />
              🔥 Custom Key (Hot Partition)
            </label>

            {partitionMode === 'hot' && (
              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                <input
                  type="text"
                  placeholder="Arbitrary Key..."
                  value={hotCustomerId}
                  onChange={(e) => setHotCustomerId(e.target.value)}
                  disabled={bursting}
                  style={{
                    padding: '4px 8px',
                    fontSize: '0.775rem',
                    fontFamily: 'var(--font-mono)',
                    width: '135px',
                    borderRadius: 'var(--radius-sm)',
                    border: '1px solid #f97316',
                  }}
                />
                <div style={{ display: 'flex', gap: '2px' }}>
                  {['cust_hot_spot', 'VIP_HOT', 'U001', 'cust_alpha'].map((preset) => (
                    <button
                      key={preset}
                      type="button"
                      onClick={() => setHotCustomerId(preset)}
                      disabled={bursting}
                      style={{
                        fontSize: '0.7rem',
                        padding: '2px 5px',
                        borderRadius: 'var(--radius-sm)',
                        border: '1px solid var(--border-subtle)',
                        background: hotCustomerId === preset ? '#fed7aa' : 'var(--bg-subtle)',
                        cursor: 'pointer',
                        fontFamily: 'var(--font-mono)',
                      }}
                    >
                      {preset}
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Action Trigger Buttons */}
        <div style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'flex-end', gap: '8px' }}>
          <button
            onClick={handleInjectPoisonPill}
            disabled={injectingPoison || bursting}
            className="btn-cyber btn-rose"
            title="Inject malformed non-JSON bytes into Kafka to prove consumer isolation and offset survival"
            style={{
              padding: '8px 14px',
              fontSize: '0.825rem',
              fontWeight: 600,
              border: '1px solid #f43f5e',
              color: '#e11d48',
              background: '#fff1f2',
            }}
          >
            {injectingPoison ? '☠️ Injecting...' : '☠️ Inject Poison Pill'}
          </button>

          <button
            onClick={handleLaunchBurst}
            disabled={bursting}
            className="btn-cyber btn-emerald"
            style={{
              padding: '8px 18px',
              fontSize: '0.875rem',
              fontWeight: 700,
              boxShadow: '0 4px 12px rgba(16, 185, 129, 0.25)',
            }}
          >
            {bursting ? '⚡ Emitting Workload Burst...' : `⚡ Fire ${burstCount} Events Burst`}
          </button>
        </div>
      </div>

      {/* Poison Pill Banner */}
      {poisonResult && (
        <div
          style={{
            marginTop: '12px',
            padding: '10px 14px',
            borderRadius: 'var(--radius-sm)',
            background: '#fff1f2',
            border: '1px solid #fecdd3',
            fontSize: '0.825rem',
            color: '#9f1239',
            fontFamily: 'var(--font-mono)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <span>{poisonResult}</span>
          <button
            type="button"
            onClick={() => setPoisonResult(null)}
            style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#9f1239', fontWeight: 700 }}
          >
            ✕
          </button>
        </div>
      )}

      {/* Burst Result Notification */}
      {burstResult && (
        <div
          style={{
            marginTop: '14px',
            padding: '12px 16px',
            borderRadius: 'var(--radius-sm)',
            background: '#ecfdf5',
            border: '1px solid #a7f3d0',
            display: 'flex',
            flexDirection: 'column',
            gap: '6px',
            fontSize: '0.825rem',
            color: '#065f46',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '8px' }}>
            <div>
              <strong style={{ fontSize: '0.875rem' }}>✓ Exactly {burstResult.totalCreated} Kafka events emitted</strong> into{' '}
              <code style={{ fontFamily: 'var(--font-mono)', background: '#d1fae5', padding: '1px 5px', borderRadius: '3px' }}>purchase.events</code>{' '}
              in <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 700 }}>{burstResult.durationMs}ms</span>
            </div>
            <span className="badge-mono badge-emerald">
              2 Consumer Groups Fan-out ({burstResult.totalCreated * 2} Deliveries)
            </span>
          </div>

          <div style={{ fontSize: '0.775rem', color: '#047857', display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
            <span>
              <strong>Customer Distribution:</strong>{' '}
              {Object.entries(burstResult.customerDistribution)
                .map(([c, n]) => `${c}: ${n}`)
                .join(', ')}
            </span>
          </div>

          <div style={{ fontSize: '0.75rem', color: '#059669', borderTop: '1px solid #a7f3d0', paddingTop: '6px', marginTop: '2px' }}>
            ℹ <strong>Kafka Lag Clarification:</strong> Each of the two consumer groups (<code>loyalty-v1</code> and <code>analytics-v1</code>) receives {burstResult.totalCreated} events. If a consumer is <strong>PAUSED</strong>, its lag counter will display <em>(prior uncommitted backlog + {burstResult.totalCreated})</em>.
          </div>
        </div>
      )}

      {burstError && (
        <div
          style={{
            marginTop: '14px',
            padding: '10px 14px',
            borderRadius: 'var(--radius-sm)',
            background: '#fef2f2',
            border: '1px solid #fecaca',
            fontSize: '0.825rem',
            color: '#991b1b',
          }}
        >
          ✕ Burst Error: {burstError}
        </div>
      )}
    </section>
  );
};

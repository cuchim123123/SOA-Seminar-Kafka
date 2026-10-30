import React, { useState } from 'react';
import { api } from '../api/client';
import type { ConsumerGroupLag, ConsumerStatusDto, PurchaseBatchResponse } from '../api/client';

interface ConsumerLagPanelProps {
  loyaltyLag: ConsumerGroupLag | null;
  analyticsLag: ConsumerGroupLag | null;
  loyaltyStatus: ConsumerStatusDto | null;
  analyticsStatus: ConsumerStatusDto | null;
  onStatusChanged: () => void;
}

const LATENCY_PRESETS = [0, 50, 200, 500, 1000];
const BURST_PRESETS = [10, 25, 50, 100, 200];

export const ConsumerLagPanel: React.FC<ConsumerLagPanelProps> = ({
  loyaltyLag,
  analyticsLag,
  loyaltyStatus,
  analyticsStatus,
  onStatusChanged,
}) => {
  // Consumer Controls state
  const [togglingLoyalty, setTogglingLoyalty] = useState<boolean>(false);
  const [togglingAnalytics, setTogglingAnalytics] = useState<boolean>(false);

  // Latency input state
  const [loyaltyDelayInput, setLoyaltyDelayInput] = useState<string>(
    String(loyaltyStatus?.processingDelayMs ?? 0),
  );
  const [analyticsDelayInput, setAnalyticsDelayInput] = useState<string>(
    String(analyticsStatus?.processingDelayMs ?? 0),
  );
  const [updatingLoyaltyDelay, setUpdatingLoyaltyDelay] = useState<boolean>(false);
  const [updatingAnalyticsDelay, setUpdatingAnalyticsDelay] = useState<boolean>(false);

  // Stress burst state
  const [burstCount, setBurstCount] = useState<number>(25);
  const [partitionMode, setPartitionMode] = useState<'balanced' | 'hot'>('balanced');
  const [hotCustomerId, setHotCustomerId] = useState<string>('cust_hot_spot');
  const [bursting, setBursting] = useState<boolean>(false);
  const [burstResult, setBurstResult] = useState<PurchaseBatchResponse | null>(null);
  const [burstError, setBurstError] = useState<string | null>(null);

  const isLoyaltyPaused = loyaltyStatus?.isPaused ?? false;
  const isAnalyticsPaused = analyticsStatus?.isPaused ?? false;

  const currentLoyaltyDelay = loyaltyStatus?.processingDelayMs ?? 0;
  const currentAnalyticsDelay = analyticsStatus?.processingDelayMs ?? 0;

  // Toggle Loyalty Pause/Resume
  const handleToggleLoyalty = async () => {
    setTogglingLoyalty(true);
    try {
      if (isLoyaltyPaused) {
        await api.resumeLoyalty();
      } else {
        await api.pauseLoyalty();
      }
      onStatusChanged();
    } catch (err) {
      alert(`Loyalty control error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setTogglingLoyalty(false);
    }
  };

  // Toggle Analytics Pause/Resume
  const handleToggleAnalytics = async () => {
    setTogglingAnalytics(true);
    try {
      if (isAnalyticsPaused) {
        await api.resumeAnalytics();
      } else {
        await api.pauseAnalytics();
      }
      onStatusChanged();
    } catch (err) {
      alert(`Analytics control error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setTogglingAnalytics(false);
    }
  };

  // Set Loyalty Delay
  const handleApplyLoyaltyDelay = async (msValue?: number) => {
    const delay = typeof msValue === 'number' ? msValue : Math.max(0, parseInt(loyaltyDelayInput, 10) || 0);
    setUpdatingLoyaltyDelay(true);
    try {
      await api.setLoyaltyConsumerDelay(delay);
      setLoyaltyDelayInput(String(delay));
      onStatusChanged();
    } catch (err) {
      alert(`Failed to set loyalty consumer latency: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setUpdatingLoyaltyDelay(false);
    }
  };

  // Set Analytics Delay
  const handleApplyAnalyticsDelay = async (msValue?: number) => {
    const delay = typeof msValue === 'number' ? msValue : Math.max(0, parseInt(analyticsDelayInput, 10) || 0);
    setUpdatingAnalyticsDelay(true);
    try {
      await api.setAnalyticsConsumerDelay(delay);
      setAnalyticsDelayInput(String(delay));
      onStatusChanged();
    } catch (err) {
      alert(`Failed to set analytics consumer latency: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setUpdatingAnalyticsDelay(false);
    }
  };

  // Launch Stress Test Workload Burst
  const handleLaunchBurst = async () => {
    setBursting(true);
    setBurstResult(null);
    setBurstError(null);
    try {
      const result = await api.createPurchaseBatch({
        count: burstCount,
        customerId: partitionMode === 'hot' ? hotCustomerId.trim() || 'cust_hot_spot' : undefined,
      });
      setBurstResult(result);
      onStatusChanged();

      // Trigger sequential refresh so lag visualizer immediately animates
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

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px', marginBottom: '24px' }}>
      {/* 1. Header & Explanation */}
      <section className="cyber-card" style={{ padding: '20px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h2 style={{ fontSize: '1.2rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ color: 'var(--color-cyan)', fontSize: '1.3rem' }}>⚡</span>
              Stress Testing &amp; Consumer Lag Telemetry Studio
            </h2>
            <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem', marginTop: '4px' }}>
              Simulate real-world consumer processing time (<code style={{ color: 'var(--color-cyan)', fontFamily: 'var(--font-mono)' }}>latency in ms</code>) and fire high-throughput workload bursts to observe lag accumulation, partition balancing, and independent consumer offset commits.
            </p>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span className="badge-mono badge-cyan">Topic: purchase.events</span>
            <span className="badge-mono badge-emerald">Manual Commit (N + 1)</span>
          </div>
        </div>
      </section>

      {/* 2. Stress Testing Workload Burst Generator */}
      <section
        className="cyber-card"
        style={{
          padding: '22px 24px',
          background: 'linear-gradient(135deg, rgba(255,255,255,0.95), rgba(240, 249, 255, 0.6))',
          border: '1px solid rgba(14, 165, 233, 0.25)',
          boxShadow: 'var(--shadow-sm)',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '8px' }}>
          <div>
            <span style={{ fontSize: '0.75rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em', color: '#0284c7' }}>
              Workload Burst Engine
            </span>
            <h3 style={{ fontSize: '1.05rem', fontWeight: 700, color: 'var(--text-main)', marginTop: '2px' }}>
              🚀 High-Throughput Event Burst Generator
            </h3>
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
                  onClick={() => setBurstCount(preset)}
                  disabled={bursting}
                  className={`btn-cyber ${burstCount === preset ? 'btn-cyan' : 'btn-ghost'}`}
                  style={{ padding: '4px 10px', fontSize: '0.775rem' }}
                >
                  {preset}
                </button>
              ))}
              <div style={{ display: 'flex', alignItems: 'center', gap: '4px', marginLeft: '4px' }}>
                <input
                  type="number"
                  min="1"
                  max="500"
                  value={burstCount}
                  onChange={(e) => setBurstCount(Math.max(1, Math.min(500, parseInt(e.target.value, 10) || 1)))}
                  disabled={bursting}
                  style={{
                    width: '65px',
                    padding: '4px 8px',
                    fontSize: '0.8rem',
                    fontFamily: 'var(--font-mono)',
                    borderRadius: 'var(--radius-sm)',
                    border: '1px solid var(--border-subtle)',
                  }}
                />
                <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>events</span>
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
                🔥 Hot Single Partition
              </label>

              {partitionMode === 'hot' && (
                <input
                  type="text"
                  placeholder="e.g. cust_hot_spot"
                  value={hotCustomerId}
                  onChange={(e) => setHotCustomerId(e.target.value)}
                  disabled={bursting}
                  style={{
                    padding: '4px 8px',
                    fontSize: '0.775rem',
                    fontFamily: 'var(--font-mono)',
                    width: '130px',
                    borderRadius: 'var(--radius-sm)',
                    border: '1px solid var(--border-subtle)',
                  }}
                />
              )}
            </div>
          </div>

          {/* Trigger Button */}
          <div style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'flex-end' }}>
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

        {/* Burst Result Notification */}
        {burstResult && (
          <div
            style={{
              marginTop: '14px',
              padding: '10px 14px',
              borderRadius: 'var(--radius-sm)',
              background: '#ecfdf5',
              border: '1px solid #a7f3d0',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              flexWrap: 'wrap',
              gap: '8px',
              fontSize: '0.825rem',
              color: '#065f46',
            }}
          >
            <div>
              <strong>✓ Successfully emitted {burstResult.totalCreated} events</strong> in{' '}
              <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 700 }}>{burstResult.durationMs}ms</span>!
              <span style={{ marginLeft: '10px', color: '#047857' }}>
                Distribution: {Object.entries(burstResult.customerDistribution).map(([c, n]) => `${c}: ${n}`).join(', ')}
              </span>
            </div>
            <div style={{ fontSize: '0.775rem', color: '#059669', fontStyle: 'italic' }}>
              💡 Watch the consumer lag bars below drain in real-time according to simulated latency!
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

      {/* 3. Side-by-Side Independent Consumer Group Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: '20px' }}>
        
        {/* ============================================================ */}
        {/* Consumer Group 1: loyalty-v1                                 */}
        {/* ============================================================ */}
        <div
          style={{
            padding: '20px',
            borderRadius: 'var(--radius-md)',
            background: isLoyaltyPaused ? '#fffbeb' : '#ffffff',
            border: isLoyaltyPaused ? '1px solid #fde68a' : '1px solid var(--border-subtle)',
            boxShadow: 'var(--shadow-sm)',
          }}
        >
          {/* Header & Status */}
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span
                style={{
                  width: '10px',
                  height: '10px',
                  borderRadius: '50%',
                  background: isLoyaltyPaused ? 'var(--color-amber)' : 'var(--color-emerald)',
                  display: 'inline-block',
                }}
              />
              <span style={{ fontWeight: 800, fontSize: '1rem', color: 'var(--text-main)', fontFamily: 'var(--font-mono)' }}>
                loyalty-v1
              </span>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span className={`badge-mono ${isLoyaltyPaused ? 'badge-amber' : 'badge-emerald'}`}>
                {isLoyaltyPaused ? 'PAUSED' : 'RUNNING'}
              </span>

              <button
                onClick={handleToggleLoyalty}
                disabled={togglingLoyalty}
                className={`btn-cyber ${isLoyaltyPaused ? 'btn-emerald' : 'btn-amber'}`}
                style={{ padding: '4px 10px', fontSize: '0.75rem' }}
                title={isLoyaltyPaused ? 'Resume consumer to drain lag' : 'Pause consumer to observe lag accumulation'}
              >
                {togglingLoyalty ? '...' : isLoyaltyPaused ? '▶ Resume' : '⏸ Pause'}
              </button>
            </div>
          </div>

          {/* Latency Simulator Section */}
          <div
            style={{
              padding: '12px 14px',
              borderRadius: 'var(--radius-sm)',
              background: currentLoyaltyDelay > 0 ? '#f0fdf4' : 'var(--bg-subtle)',
              border: currentLoyaltyDelay > 0 ? '1px solid #bbf7d0' : '1px solid var(--border-subtle)',
              marginBottom: '14px',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
              <span style={{ fontSize: '0.775rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '5px' }}>
                ⏱ Simulated Consumer Latency:
              </span>
              <span
                className={`badge-mono ${currentLoyaltyDelay > 0 ? 'badge-emerald' : 'badge-cyan'}`}
                style={{ fontSize: '0.75rem' }}
              >
                {currentLoyaltyDelay === 0 ? '0ms (Real-time)' : `${currentLoyaltyDelay}ms / msg`}
              </span>
            </div>

            {/* Direct Input & Quick Presets */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap', marginBottom: '8px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                <input
                  type="number"
                  min="0"
                  max="10000"
                  step="50"
                  value={loyaltyDelayInput}
                  onChange={(e) => setLoyaltyDelayInput(e.target.value)}
                  disabled={updatingLoyaltyDelay}
                  placeholder="ms"
                  style={{
                    width: '80px',
                    padding: '4px 8px',
                    fontSize: '0.8rem',
                    fontFamily: 'var(--font-mono)',
                    borderRadius: 'var(--radius-sm)',
                    border: '1px solid var(--border-subtle)',
                  }}
                />
                <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>ms</span>
              </div>

              <button
                type="button"
                onClick={() => handleApplyLoyaltyDelay()}
                disabled={updatingLoyaltyDelay}
                className="btn-cyber btn-cyan"
                style={{ padding: '4px 10px', fontSize: '0.75rem' }}
              >
                {updatingLoyaltyDelay ? '...' : 'Apply'}
              </button>
            </div>

            {/* Preset Buttons */}
            <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
              {LATENCY_PRESETS.map((preset) => (
                <button
                  key={preset}
                  type="button"
                  onClick={() => handleApplyLoyaltyDelay(preset)}
                  disabled={updatingLoyaltyDelay}
                  className={`btn-cyber ${currentLoyaltyDelay === preset ? 'btn-emerald' : 'btn-ghost'}`}
                  style={{ padding: '2px 8px', fontSize: '0.725rem' }}
                >
                  {preset === 0 ? '0ms (Realtime)' : `${preset}ms`}
                </button>
              ))}
            </div>
          </div>

          {/* Lag Counter Badge */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              marginBottom: '14px',
              padding: '10px 14px',
              background: isLoyaltyPaused ? '#fef3c7' : 'var(--bg-subtle)',
              borderRadius: 'var(--radius-sm)',
            }}
          >
            <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)', fontWeight: 600 }}>Total Unprocessed Lag:</span>
            <span style={{ fontSize: '1.25rem', fontWeight: 800, color: 'var(--color-emerald)', fontFamily: 'var(--font-mono)' }}>
              {loyaltyLag?.totalLag || '0'} <span style={{ fontSize: '0.75rem', fontWeight: 400, color: 'var(--text-muted)' }}>records</span>
            </span>
          </div>

          {/* Per-Partition Lag Breakdown */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            <span style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase' }}>
              Partition Offsets &amp; Backlog:
            </span>
            {loyaltyLag?.partitions.map((pl) => {
              const lagNum = Number(pl.lag || '0');
              return (
                <div key={pl.partition} style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--text-body)', marginBottom: '3px' }}>
                    <span>P#{pl.partition} (Committed: {pl.committedOffset ?? 'Uncommitted'})</span>
                    <span style={{ color: lagNum > 0 ? 'var(--color-amber)' : 'var(--text-muted)', fontWeight: 700 }}>
                      Lag: {pl.lag}
                    </span>
                  </div>
                  <div style={{ height: '6px', background: '#f1f5f9', borderRadius: '3px', overflow: 'hidden' }}>
                    <div
                      style={{
                        height: '100%',
                        width: `${Math.min(100, lagNum * 12)}%`,
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

        {/* ============================================================ */}
        {/* Consumer Group 2: analytics-v1                              */}
        {/* ============================================================ */}
        <div
          style={{
            padding: '20px',
            borderRadius: 'var(--radius-md)',
            background: isAnalyticsPaused ? '#fffbeb' : '#ffffff',
            border: isAnalyticsPaused ? '1px solid #fde68a' : '1px solid var(--border-subtle)',
            boxShadow: 'var(--shadow-sm)',
          }}
        >
          {/* Header & Status */}
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span
                style={{
                  width: '10px',
                  height: '10px',
                  borderRadius: '50%',
                  background: isAnalyticsPaused ? 'var(--color-amber)' : 'var(--color-cyan)',
                  display: 'inline-block',
                }}
              />
              <span style={{ fontWeight: 800, fontSize: '1rem', color: 'var(--text-main)', fontFamily: 'var(--font-mono)' }}>
                analytics-v1
              </span>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span className={`badge-mono ${isAnalyticsPaused ? 'badge-amber' : 'badge-cyan'}`}>
                {isAnalyticsPaused ? 'PAUSED' : 'RUNNING'}
              </span>

              <button
                onClick={handleToggleAnalytics}
                disabled={togglingAnalytics}
                className={`btn-cyber ${isAnalyticsPaused ? 'btn-emerald' : 'btn-amber'}`}
                style={{ padding: '4px 10px', fontSize: '0.75rem' }}
                title={isAnalyticsPaused ? 'Resume consumer to drain lag' : 'Pause consumer to observe lag accumulation'}
              >
                {togglingAnalytics ? '...' : isAnalyticsPaused ? '▶ Resume' : '⏸ Pause'}
              </button>
            </div>
          </div>

          {/* Latency Simulator Section */}
          <div
            style={{
              padding: '12px 14px',
              borderRadius: 'var(--radius-sm)',
              background: currentAnalyticsDelay > 0 ? '#f0f9ff' : 'var(--bg-subtle)',
              border: currentAnalyticsDelay > 0 ? '1px solid #bae6fd' : '1px solid var(--border-subtle)',
              marginBottom: '14px',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
              <span style={{ fontSize: '0.775rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '5px' }}>
                ⏱ Simulated Consumer Latency:
              </span>
              <span
                className={`badge-mono ${currentAnalyticsDelay > 0 ? 'badge-cyan' : 'badge-cyan'}`}
                style={{ fontSize: '0.75rem' }}
              >
                {currentAnalyticsDelay === 0 ? '0ms (Real-time)' : `${currentAnalyticsDelay}ms / msg`}
              </span>
            </div>

            {/* Direct Input & Quick Presets */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap', marginBottom: '8px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                <input
                  type="number"
                  min="0"
                  max="10000"
                  step="50"
                  value={analyticsDelayInput}
                  onChange={(e) => setAnalyticsDelayInput(e.target.value)}
                  disabled={updatingAnalyticsDelay}
                  placeholder="ms"
                  style={{
                    width: '80px',
                    padding: '4px 8px',
                    fontSize: '0.8rem',
                    fontFamily: 'var(--font-mono)',
                    borderRadius: 'var(--radius-sm)',
                    border: '1px solid var(--border-subtle)',
                  }}
                />
                <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>ms</span>
              </div>

              <button
                type="button"
                onClick={() => handleApplyAnalyticsDelay()}
                disabled={updatingAnalyticsDelay}
                className="btn-cyber btn-cyan"
                style={{ padding: '4px 10px', fontSize: '0.75rem' }}
              >
                {updatingAnalyticsDelay ? '...' : 'Apply'}
              </button>
            </div>

            {/* Preset Buttons */}
            <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
              {LATENCY_PRESETS.map((preset) => (
                <button
                  key={preset}
                  type="button"
                  onClick={() => handleApplyAnalyticsDelay(preset)}
                  disabled={updatingAnalyticsDelay}
                  className={`btn-cyber ${currentAnalyticsDelay === preset ? 'btn-cyan' : 'btn-ghost'}`}
                  style={{ padding: '2px 8px', fontSize: '0.725rem' }}
                >
                  {preset === 0 ? '0ms (Realtime)' : `${preset}ms`}
                </button>
              ))}
            </div>
          </div>

          {/* Lag Counter Badge */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              marginBottom: '14px',
              padding: '10px 14px',
              background: isAnalyticsPaused ? '#fef3c7' : 'var(--bg-subtle)',
              borderRadius: 'var(--radius-sm)',
            }}
          >
            <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)', fontWeight: 600 }}>Total Unprocessed Lag:</span>
            <span style={{ fontSize: '1.25rem', fontWeight: 800, color: isAnalyticsPaused ? '#b45309' : '#0369a1', fontFamily: 'var(--font-mono)' }}>
              {analyticsLag?.totalLag || '0'} <span style={{ fontSize: '0.75rem', fontWeight: 400, color: 'var(--text-muted)' }}>records</span>
            </span>
          </div>

          {/* Per-Partition Lag Breakdown */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            <span style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase' }}>
              Partition Offsets &amp; Backlog:
            </span>
            {analyticsLag?.partitions.map((pl) => {
              const lagNum = Number(pl.lag || '0');
              return (
                <div key={pl.partition} style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--text-body)', marginBottom: '3px' }}>
                    <span>P#{pl.partition} (Committed: {pl.committedOffset ?? 'Uncommitted'})</span>
                    <span style={{ color: lagNum > 0 ? '#b45309' : 'var(--text-muted)', fontWeight: 700 }}>
                      Lag: {pl.lag}
                    </span>
                  </div>
                  <div style={{ height: '6px', background: '#f1f5f9', borderRadius: '3px', overflow: 'hidden' }}>
                    <div
                      style={{
                        height: '100%',
                        width: `${Math.min(100, lagNum * 12)}%`,
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
    </div>
  );
};

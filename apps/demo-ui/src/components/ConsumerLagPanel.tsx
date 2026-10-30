import React from 'react';
import type { ConsumerGroupLag, ConsumerStatusDto } from '../api/client';
import { api } from '../api/client';
import { WorkloadBurstCard } from './lag/WorkloadBurstCard';
import { ConsumerGroupCard } from './lag/ConsumerGroupCard';

interface ConsumerLagPanelProps {
  loyaltyLag: ConsumerGroupLag | null;
  analyticsLag: ConsumerGroupLag | null;
  loyaltyStatus: ConsumerStatusDto | null;
  analyticsStatus: ConsumerStatusDto | null;
  onStatusChanged: () => void;
}

export const ConsumerLagPanel: React.FC<ConsumerLagPanelProps> = ({
  loyaltyLag,
  analyticsLag,
  loyaltyStatus,
  analyticsStatus,
  onStatusChanged,
}) => {
  // Toggle Loyalty Pause/Resume
  const handleToggleLoyalty = async () => {
    try {
      if (loyaltyStatus?.isPaused) {
        await api.resumeLoyalty();
      } else {
        await api.pauseLoyalty();
      }
      onStatusChanged();
    } catch (err) {
      alert(`Loyalty control error: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  // Toggle Analytics Pause/Resume
  const handleToggleAnalytics = async () => {
    try {
      if (analyticsStatus?.isPaused) {
        await api.resumeAnalytics();
      } else {
        await api.pauseAnalytics();
      }
      onStatusChanged();
    } catch (err) {
      alert(`Analytics control error: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  // Set Loyalty Delay
  const handleApplyLoyaltyDelay = async (delayMs: number) => {
    try {
      await api.setLoyaltyConsumerDelay(delayMs);
      onStatusChanged();
    } catch (err) {
      alert(`Failed to set loyalty consumer latency: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  // Set Analytics Delay
  const handleApplyAnalyticsDelay = async (delayMs: number) => {
    try {
      await api.setAnalyticsConsumerDelay(delayMs);
      onStatusChanged();
    } catch (err) {
      alert(`Failed to set analytics consumer latency: ${err instanceof Error ? err.message : String(err)}`);
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

      {/* 2. Workload Burst & Chaos Generator */}
      <WorkloadBurstCard onStatusChanged={onStatusChanged} />

      {/* 3. Side-by-Side Independent Consumer Group Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: '20px' }}>
        <ConsumerGroupCard
          groupId="loyalty-v1"
          databaseName="loyalty_db"
          themeColor="emerald"
          lag={loyaltyLag}
          status={loyaltyStatus}
          onTogglePause={handleToggleLoyalty}
          onApplyDelay={handleApplyLoyaltyDelay}
        />

        <ConsumerGroupCard
          groupId="analytics-v1"
          databaseName="analytics_db"
          themeColor="cyan"
          lag={analyticsLag}
          status={analyticsStatus}
          onTogglePause={handleToggleAnalytics}
          onApplyDelay={handleApplyAnalyticsDelay}
        />
      </div>
    </div>
  );
};

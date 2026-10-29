import React, { useState, useEffect, useCallback } from 'react';
import { api } from './api/client';
import type {
  ConsumerGroupLag,
  ConsumerStatusDto,
  PurchaseCreatedResponse,
  SalesSummaryDto,
  ServiceHealthStatus,
  TopicPartitionMetadata,
} from './api/client';
import { Header } from './components/Header';
import { PipelineVisualizer } from './components/PipelineVisualizer';
import { PartitionGrid } from './components/PartitionGrid';
import { ConsumerLagPanel } from './components/ConsumerLagPanel';
import { ProjectionsPanel } from './components/ProjectionsPanel';
import { ReplayStudio } from './components/ReplayStudio';
import { LiveEventLog } from './components/LiveEventLog';
import type { LogEntry } from './components/LiveEventLog';

export const App: React.FC = () => {
  const [health, setHealth] = useState<ServiceHealthStatus>({
    producer: false,
    loyalty: false,
    analytics: false,
    publisher: false,
    replay: false,
  });

  const [topicMetadata, setTopicMetadata] = useState<TopicPartitionMetadata | null>(null);
  const [loyaltyLag, setLoyaltyLag] = useState<ConsumerGroupLag | null>(null);
  const [analyticsLag, setAnalyticsLag] = useState<ConsumerGroupLag | null>(null);
  const [analyticsStatus, setAnalyticsStatus] = useState<ConsumerStatusDto | null>(null);
  const [salesSummary, setSalesSummary] = useState<SalesSummaryDto | null>(null);
  const [recentLogs, setRecentLogs] = useState<LogEntry[]>([]);
  const [lastPurchase, setLastPurchase] = useState<PurchaseCreatedResponse | null>(null);
  const [highlightedPartition] = useState<number | null>(null);
  const [refreshing, setRefreshing] = useState<boolean>(false);
  const [lastEventTime, setLastEventTime] = useState<number>(Date.now());

  const fetchTelemetry = useCallback(async () => {
    try {
      const [h, meta, lLag, aLag, aStat, summ] = await Promise.all([
        api.checkServicesHealth().catch(() => ({
          producer: false,
          loyalty: false,
          analytics: false,
          publisher: false,
          replay: false,
        })),
        api.getPartitions().catch(() => null),
        api.getConsumerLag('loyalty-v1').catch(() => null),
        api.getConsumerLag('analytics-v1').catch(() => null),
        api.getConsumerStatus().catch(() => null),
        api.getAnalyticsSummary().catch(() => null),
      ]);

      setHealth(h);
      if (meta) setTopicMetadata(meta);
      if (lLag) setLoyaltyLag(lLag);
      if (aLag) setAnalyticsLag(aLag);
      if (aStat) setAnalyticsStatus(aStat);
      if (summ) setSalesSummary(summ);
    } catch {
      // Telemetry fetch non-blocking
    }
  }, []);

  // Polling effect every 2000ms
  useEffect(() => {
    fetchTelemetry();
    const timer = setInterval(fetchTelemetry, 2000);
    return () => clearInterval(timer);
  }, [fetchTelemetry]);

  const handleManualRefresh = async () => {
    setRefreshing(true);
    await fetchTelemetry();
    setRefreshing(false);
  };

  const handleEventEmitted = (res: PurchaseCreatedResponse) => {
    setLastPurchase(res);
    setLastEventTime(Date.now());

    // Record in live audit log
    const entry: LogEntry = {
      id: res.eventId,
      timestamp: new Date().toLocaleTimeString(),
      customerId: res.customerId,
      customerSequence: res.customerSequence,
      purchaseId: res.purchaseId,
      status: res.status,
    };
    setRecentLogs((prev) => [entry, ...prev.slice(0, 19)]);

    // Trigger telemetry refresh to observe partition offsets immediately
    setTimeout(() => {
      fetchTelemetry();
    }, 600);
  };

  return (
    <div style={{ maxWidth: '1440px', margin: '0 auto', padding: '24px 20px 48px' }}>
      {/* 1. Header with Cluster Health & Telemetry */}
      <Header
        health={health}
        refreshing={refreshing}
        onRefresh={handleManualRefresh}
      />

      {/* 2. Interactive Request Flow & Outbox Pipeline */}
      <PipelineVisualizer
        onEventEmitted={handleEventEmitted}
        lastPurchase={lastPurchase}
      />

      {/* 3. Kafka 6-Partition Topic Visualizer */}
      <PartitionGrid
        metadata={topicMetadata}
        loading={!topicMetadata}
        highlightedPartition={highlightedPartition}
      />

      {/* 4. Independent Consumer Groups & Real Lag Controls */}
      <ConsumerLagPanel
        loyaltyLag={loyaltyLag}
        analyticsLag={analyticsLag}
        analyticsStatus={analyticsStatus}
        onStatusChanged={fetchTelemetry}
      />

      {/* 5. Downstream Read Projections (Loyalty & Analytics) */}
      <ProjectionsPanel
        summary={salesSummary}
        lastUpdatedEventTime={lastEventTime}
      />

      {/* 6. Bounded Replay & Two-Pass Correction Studio */}
      <ReplayStudio />

      {/* 7. Live Audit & Event Log */}
      <LiveEventLog
        entries={recentLogs}
        onClear={() => setRecentLogs([])}
      />

      {/* Footer */}
      <footer
        style={{
          marginTop: '40px',
          textAlign: 'center',
          color: 'var(--text-dim)',
          fontSize: '0.75rem',
          fontFamily: 'var(--font-mono)',
          borderTop: '1px solid var(--border-subtle)',
          paddingTop: '20px',
          display: 'flex',
          flexDirection: 'column',
          gap: '8px',
          alignItems: 'center',
        }}
      >
        <div>
          KAFKA STREAMING LAB — ARCHITECTURAL DEMO &amp; EVIDENCE OBSERVABILITY PLATFORM
        </div>
        <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap', justifyContent: 'center' }}>
          <span>• 6-Partition Topic Ordering</span>
          <span>• Transactional Outbox (Postgres)</span>
          <span>• Consumer Idempotency (N+1 Commits)</span>
          <span>• Bounded Replay &amp; Correction</span>
          <span>• KRaft Single-Node Broker</span>
        </div>
      </footer>
    </div>
  );
};

export default App;

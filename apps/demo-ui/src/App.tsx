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
import { Navigation } from './components/Navigation';
import type { TabId } from './components/Navigation';
import { PipelineVisualizer } from './components/PipelineVisualizer';
import { PartitionGrid } from './components/PartitionGrid';
import { ConsumerLagPanel } from './components/ConsumerLagPanel';
import { ProjectionsPanel } from './components/ProjectionsPanel';
import { ReplayStudio } from './components/ReplayStudio';
import { LiveEventLog } from './components/LiveEventLog';
import type { LogEntry } from './components/LiveEventLog';
import { UserGuide } from './components/UserGuide';

function getInitialTab(): TabId {
  const hash = window.location.hash.replace('#', '') as TabId;
  const validTabs: TabId[] = ['pipeline', 'partitions', 'lag', 'projections', 'replay', 'guide'];
  return validTabs.includes(hash) ? hash : 'pipeline';
}

export const App: React.FC = () => {
  const [activeTab, setActiveTab] = useState<TabId>(getInitialTab);

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

  // Hash change synchronization for browser navigation
  useEffect(() => {
    const handleHashChange = () => {
      setActiveTab(getInitialTab());
    };
    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);

  const handleTabChange = (tab: TabId) => {
    setActiveTab(tab);
    window.location.hash = tab;
  };

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
      // Non-blocking telemetry sync
    }
  }, []);

  // Background sync every 3000ms
  useEffect(() => {
    fetchTelemetry();
    const timer = setInterval(fetchTelemetry, 3000);
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

    setTimeout(() => {
      fetchTelemetry();
    }, 600);
  };

  return (
    <div style={{ maxWidth: '1400px', margin: '0 auto', padding: '20px 20px 48px' }}>
      {/* 1. Header with Cluster Health & Telemetry */}
      <Header
        health={health}
        refreshing={refreshing}
        onRefresh={handleManualRefresh}
      />

      {/* 2. Sleek Tab Navigation Bar */}
      <Navigation
        activeTab={activeTab}
        onTabChange={handleTabChange}
        isConsumerPaused={analyticsStatus?.isPaused ?? false}
        totalPartitions={topicMetadata?.partitionCount ?? 6}
      />

      {/* 3. Tab Contents: Cleanly Separated Views */}
      <main>
        {activeTab === 'pipeline' && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
            <PipelineVisualizer
              onEventEmitted={handleEventEmitted}
              lastPurchase={lastPurchase}
            />
            <LiveEventLog
              entries={recentLogs}
              onClear={() => setRecentLogs([])}
            />
          </div>
        )}

        {activeTab === 'partitions' && (
          <PartitionGrid
            metadata={topicMetadata}
            loading={!topicMetadata}
            highlightedPartition={highlightedPartition}
            onMetadataChanged={fetchTelemetry}
          />
        )}

        {activeTab === 'lag' && (
          <ConsumerLagPanel
            loyaltyLag={loyaltyLag}
            analyticsLag={analyticsLag}
            analyticsStatus={analyticsStatus}
            onStatusChanged={fetchTelemetry}
          />
        )}

        {activeTab === 'projections' && (
          <ProjectionsPanel
            summary={salesSummary}
            lastUpdatedEventTime={lastEventTime}
          />
        )}

        {activeTab === 'replay' && (
          <ReplayStudio />
        )}

        {activeTab === 'guide' && (
          <UserGuide onNavigateToTab={handleTabChange} />
        )}
      </main>

      {/* Footer */}
      <footer
        style={{
          marginTop: '36px',
          textAlign: 'center',
          color: 'var(--text-muted)',
          fontSize: '0.75rem',
          fontFamily: 'var(--font-mono)',
          borderTop: '1px solid var(--border-subtle)',
          paddingTop: '18px',
          display: 'flex',
          flexDirection: 'column',
          gap: '6px',
          alignItems: 'center',
        }}
      >
        <div style={{ fontWeight: 600, color: 'var(--text-body)' }}>
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

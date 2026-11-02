import { useState, useEffect, useCallback } from 'react';
import { api } from './api/client';
import type {
  ConsumerGroupLag,
  ConsumerStatusDto,
  SalesSummaryDto,
  ServiceHealthStatus,
  TopicPartitionMetadata,
} from './api/client';
import { Header } from './components/Header';
import { WorkspaceSidebar } from './components/WorkspaceSidebar';
import { RealApplication } from './real/RealApplication';
import { parseRoute } from './real/state/routes';
import './real/workspace.css';
import type { TabId } from './components/Navigation';
import { UserGuide } from './components/UserGuide';
import { BenchmarkStudio } from './components/BenchmarkStudio';
import { SimulationLab } from './simulation/ui/SimulationLab';

export function App() {
  const [route, setRoute] = useState(() => parseRoute(window.location.hash));
  const activeTab = route.real ? 'real' : route.page;

  const [health, setHealth] = useState<ServiceHealthStatus>({
    producer: false,
    loyalty: false,
    analytics: false,
    cdc: false,
    replay: false,
  });

  const [topicMetadata, setTopicMetadata] = useState<TopicPartitionMetadata | null>(null);
  const [loyaltyLag, setLoyaltyLag] = useState<ConsumerGroupLag | null>(null);
  const [analyticsLag, setAnalyticsLag] = useState<ConsumerGroupLag | null>(null);
  const [, setAnalyticsStatus] = useState<ConsumerStatusDto | null>(null);
  const [salesSummary, setSalesSummary] = useState<SalesSummaryDto | null>(null);
  const [refreshing, setRefreshing] = useState<boolean>(false);
  const [isBenchmarking, setIsBenchmarking] = useState<boolean>(false);

  // Hash change synchronization for browser navigation
  useEffect(() => {
    const handleHashChange = () => {
      setRoute(parseRoute(window.location.hash));
    };
    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);

  const handleTabChange = (tab: TabId) => {
    window.location.hash = tab;
  };

  const fetchTelemetry = useCallback(async () => {
    try {
      const [h, meta, lLag, aLag, aStat, summ] = await Promise.all([
        api.checkServicesHealth().catch(() => ({
          producer: false,
          loyalty: false,
          analytics: false,
          cdc: false,
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

  // Background sync every 3000ms (paused during active benchmark execution to prevent freezing)
  useEffect(() => {
    if (isBenchmarking || activeTab !== 'benchmark') return;
    fetchTelemetry();
    const timer = setInterval(fetchTelemetry, 3000);
    return () => clearInterval(timer);
  }, [fetchTelemetry, isBenchmarking, activeTab]);

  const handleManualRefresh = async () => {
    setRefreshing(true);
    await fetchTelemetry();
    setRefreshing(false);
  };

  return (
    <div className="workspace-shell">
      <WorkspaceSidebar route={route} />
      <div className="workspace-content">
        {/* 1. Header with Cluster Health & Telemetry */}
        {activeTab === 'benchmark' && (
          <Header health={health} refreshing={refreshing} onRefresh={handleManualRefresh} />
        )}

        {/* 3. Primary Views (Simulation Lab, Benchmark, Guide) */}
        <main tabIndex={-1}>
          {activeTab === 'real' && <RealApplication key={route.customerId} route={route} />}

          {activeTab === 'simulation' && <SimulationLab />}

          {activeTab === 'benchmark' && (
            <BenchmarkStudio
              topicMetadata={topicMetadata}
              loyaltyLag={loyaltyLag}
              analyticsLag={analyticsLag}
              salesSummary={salesSummary}
              loyaltyAccount={null}
              onRefreshTelemetry={fetchTelemetry}
              onBenchmarkStateChange={setIsBenchmarking}
            />
          )}

          {activeTab === 'guide' && <UserGuide onNavigateToTab={handleTabChange} />}
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
            <span>• Partition-scoped Ordering</span>
            <span>• Transactional Outbox (Postgres)</span>
            <span>• Consumer Idempotency (N+1 Commits)</span>
            <span>• Bounded Replay &amp; Correction</span>
            <span>• Kafka KRaft</span>
          </div>
        </footer>
      </div>
    </div>
  );
}

export default App;

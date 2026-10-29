import React from 'react';

interface UserGuideProps {
  onNavigateToTab: (tabId: string) => void;
}

export const UserGuide: React.FC<UserGuideProps> = ({ onNavigateToTab }) => {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {/* Introduction Card */}
      <section className="cyber-card" style={{ padding: '24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '8px' }}>
          <span style={{ fontSize: '1.25rem', color: 'var(--color-primary)' }}>📖</span>
          <h2 style={{ fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-main)' }}>
            Kafka Streaming Lab — Comprehensive User Guide &amp; Demo Manual
          </h2>
        </div>
        <p style={{ color: 'var(--text-body)', fontSize: '0.875rem', lineHeight: 1.6 }}>
          Welcome to the <strong>Kafka Streaming Lab</strong> observability platform. This laboratory demonstrates enterprise-grade Apache Kafka mechanics including <strong>KRaft consensus, key-based partition ordering, PostgreSQL Transactional Outbox, independent consumer group lag isolation, idempotent deduplication,</strong> and <strong>two-pass bounded replay correction</strong>.
        </p>

        <div style={{ display: 'flex', gap: '8px', marginTop: '16px', flexWrap: 'wrap' }}>
          <span className="badge-mono badge-violet">Apache Kafka 4.x KRaft</span>
          <span className="badge-mono badge-cyan">PostgreSQL Transactional Outbox</span>
          <span className="badge-mono badge-emerald">Idempotent Consumers (N+1 Commits)</span>
          <span className="badge-mono badge-amber">Bounded Two-Pass Replay</span>
        </div>
      </section>

      {/* Interactive Demonstration Scenarios */}
      <section className="cyber-card" style={{ padding: '24px' }}>
        <h3 style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-main)', marginBottom: '16px', display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span>🧪</span> 5 Step-by-Step Demonstration Scenarios
        </h3>

        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          
          {/* Scenario 1 */}
          <div style={{ padding: '16px', borderRadius: 'var(--radius-md)', background: 'var(--bg-subtle)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px', flexWrap: 'wrap', gap: '8px' }}>
              <span style={{ fontWeight: 700, color: 'var(--color-primary)', fontSize: '0.9rem' }}>
                Scenario 1: Single Purchase Flow &amp; Atomic Outbox Allocation
              </span>
              <button
                onClick={() => onNavigateToTab('pipeline')}
                className="btn-cyber btn-outline"
                style={{ fontSize: '0.75rem', padding: '4px 10px' }}
              >
                Go to Pipeline Flow ↗
              </button>
            </div>
            <p style={{ fontSize: '0.825rem', color: 'var(--text-body)', lineHeight: 1.5, marginBottom: '8px' }}>
              <strong>Objective:</strong> Prove that client requests do not write directly to Kafka. They commit atomically to PostgreSQL (allocating sequence numbers in <code>outbox_events</code>) and return <code>202 Accepted</code>, completely eliminating dual-write vulnerabilities.
            </p>
            <ol style={{ fontSize: '0.8rem', color: 'var(--text-muted)', paddingLeft: '20px', lineHeight: 1.6 }}>
              <li>Navigate to the <strong>Pipeline Flow</strong> tab.</li>
              <li>Select customer <code>U001</code>, enter an amount (e.g. <code>$120</code>), and click <strong>⚡ Emit Purchase</strong>.</li>
              <li>Observe the 5-stage animation: HTTP validation $\to$ PostgreSQL atomic row-lock $\to$ Outbox Relay $\to$ Kafka Topic $\to$ Dual Consumers.</li>
              <li>Notice the assigned <code>customerSequence</code> and check the Live Event Log at the bottom.</li>
            </ol>
          </div>

          {/* Scenario 2 */}
          <div style={{ padding: '16px', borderRadius: 'var(--radius-md)', background: 'var(--bg-subtle)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px', flexWrap: 'wrap', gap: '8px' }}>
              <span style={{ fontWeight: 700, color: 'var(--color-cyan)', fontSize: '0.9rem' }}>
                Scenario 2: Key-Based Partitioning &amp; FIFO Customer Ordering
              </span>
              <button
                onClick={() => onNavigateToTab('partitions')}
                className="btn-cyber btn-outline"
                style={{ fontSize: '0.75rem', padding: '4px 10px' }}
              >
                Go to Kafka Partitions ↗
              </button>
            </div>
            <p style={{ fontSize: '0.825rem', color: 'var(--text-body)', lineHeight: 1.5, marginBottom: '8px' }}>
              <strong>Objective:</strong> Prove Kafka's single-partition ordering guarantee. Records published with the same <code>key = customerId</code> are deterministically routed by Murmur2 hash into the same partition.
            </p>
            <ol style={{ fontSize: '0.8rem', color: 'var(--text-muted)', paddingLeft: '20px', lineHeight: 1.6 }}>
              <li>In the <strong>Pipeline Flow</strong> tab, click <strong>🚀 Burst 5x (U001)</strong>.</li>
              <li>Switch to the <strong>Kafka Partitions</strong> tab.</li>
              <li>Notice that all 5 events landed on the <em>exact same partition</em> (e.g. Partition #3), while other partitions remained untouched.</li>
              <li>Within that partition, offsets are strictly contiguous and monotonically increasing ($N, N+1, N+2\dots$).</li>
            </ol>
          </div>

          {/* Scenario 3 */}
          <div style={{ padding: '16px', borderRadius: 'var(--radius-md)', background: 'var(--bg-subtle)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px', flexWrap: 'wrap', gap: '8px' }}>
              <span style={{ fontWeight: 700, color: 'var(--color-amber)', fontSize: '0.9rem' }}>
                Scenario 3: Consumer Lag &amp; Independent Consumer Groups
              </span>
              <button
                onClick={() => onNavigateToTab('lag')}
                className="btn-cyber btn-outline"
                style={{ fontSize: '0.75rem', padding: '4px 10px' }}
              >
                Go to Consumer Lag ↗
              </button>
            </div>
            <p style={{ fontSize: '0.825rem', color: 'var(--text-body)', lineHeight: 1.5, marginBottom: '8px' }}>
              <strong>Objective:</strong> Prove that consumer groups (<code>loyalty-v1</code> and <code>analytics-v1</code>) track offsets completely independently. Pausing one group causes lag to accumulate without degrading or blocking the other.
            </p>
            <ol style={{ fontSize: '0.8rem', color: 'var(--text-muted)', paddingLeft: '20px', lineHeight: 1.6 }}>
              <li>Go to the <strong>Consumer Lag</strong> tab and click <strong>⏸ Pause Consumer</strong> on <code>analytics-v1</code>.</li>
              <li>Switch to the <strong>Pipeline Flow</strong> tab and emit 3 purchases.</li>
              <li>Return to <strong>Consumer Lag</strong>: Notice <code>analytics-v1</code> lag shows <strong>3 unprocessed records</strong>, whereas <code>loyalty-v1</code> lag remains at <strong>0</strong>!</li>
              <li>Click <strong>▶ Resume Consumer</strong>: Observe <code>analytics-v1</code> immediately consume the queued records and drain lag back to 0.</li>
            </ol>
          </div>

          {/* Scenario 4 */}
          <div style={{ padding: '16px', borderRadius: 'var(--radius-md)', background: 'var(--bg-subtle)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px', flexWrap: 'wrap', gap: '8px' }}>
              <span style={{ fontWeight: 700, color: 'var(--color-violet)', fontSize: '0.9rem' }}>
                Scenario 4: Purchase Invalidation &amp; Bounded Two-Pass Replay
              </span>
              <button
                onClick={() => onNavigateToTab('replay')}
                className="btn-cyber btn-outline"
                style={{ fontSize: '0.75rem', padding: '4px 10px' }}
              >
                Go to Bounded Replay ↗
              </button>
            </div>
            <p style={{ fontSize: '0.825rem', color: 'var(--text-body)', lineHeight: 1.5, marginBottom: '8px' }}>
              <strong>Objective:</strong> Demonstrate retroactive state correction after out-of-order compensation events without modifying immutable Kafka topics or stopping live consumer groups.
            </p>
            <ol style={{ fontSize: '0.8rem', color: 'var(--text-muted)', paddingLeft: '20px', lineHeight: 1.6 }}>
              <li>In <strong>Pipeline Flow</strong>, emit a purchase, then click <strong>↩ Invalidate Last</strong>.</li>
              <li>Go to <strong>Read Projections</strong>: Notice customer <code>U001</code> displays <code>⚠ Correction flag active</code>.</li>
              <li>Switch to <strong>Bounded Replay</strong> and click <strong>↺ Start Bounded Replay</strong>.</li>
              <li>Observe the Two-Pass correction: Pass 1 discovers invalidation tombstones; Pass 2 recomputes correct points balance ($82 \to 40$ points) into staging tables.</li>
            </ol>
          </div>

          {/* Scenario 5 */}
          <div style={{ padding: '16px', borderRadius: 'var(--radius-md)', background: 'var(--bg-subtle)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px', flexWrap: 'wrap', gap: '8px' }}>
              <span style={{ fontWeight: 700, color: 'var(--color-crimson)', fontSize: '0.9rem' }}>
                Scenario 5: Kafka Log Retention Horizon Failure Handling
              </span>
              <button
                onClick={() => onNavigateToTab('replay')}
                className="btn-cyber btn-outline"
                style={{ fontSize: '0.75rem', padding: '4px 10px' }}
              >
                Test in Replay Studio ↗
              </button>
            </div>
            <p style={{ fontSize: '0.825rem', color: 'var(--text-body)', lineHeight: 1.5, marginBottom: '8px' }}>
              <strong>Objective:</strong> Validate that bounded replay verifies earliest available log start offsets. If required historical records were pruned by topic retention, the worker fails fast with <code>InsufficientRetentionError</code> rather than calculating silent corrupted state.
            </p>
            <ol style={{ fontSize: '0.8rem', color: 'var(--text-muted)', paddingLeft: '20px', lineHeight: 1.6 }}>
              <li>In the <strong>Bounded Replay</strong> tab, click <strong>⚠ Test Retention Horizon Failure</strong>.</li>
              <li>Observe that the replay run immediately halts with a clean error badge indicating the requested partition offset has been deleted by log retention.</li>
            </ol>
          </div>
        </div>
      </section>

      {/* Architectural Principles & Clean Architecture Summary */}
      <section className="cyber-card" style={{ padding: '24px' }}>
        <h3 style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-main)', marginBottom: '14px', display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span>🏛</span> Architectural Invariants &amp; Infrastructure
        </h3>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '14px' }}>
          <div style={{ padding: '12px 14px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ fontWeight: 600, fontSize: '0.85rem', color: 'var(--text-main)', marginBottom: '4px' }}>
              Offset Commits (N+1)
            </div>
            <p style={{ fontSize: '0.775rem', color: 'var(--text-muted)', lineHeight: 1.5 }}>
              A consumed offset $N$ is always committed as $N+1$ so Kafka rebalances resume from the exact next unprocessed message without duplication.
            </p>
          </div>

          <div style={{ padding: '12px 14px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ fontWeight: 600, fontSize: '0.85rem', color: 'var(--text-main)', marginBottom: '4px' }}>
              Database Idempotency
            </div>
            <p style={{ fontSize: '0.775rem', color: 'var(--text-muted)', lineHeight: 1.5 }}>
              Every consumer maintains a <code>processed_events</code> table. If an event is re-delivered, unique constraint deduplication ensures business logic executes exactly once.
            </p>
          </div>

          <div style={{ padding: '12px 14px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ fontWeight: 600, fontSize: '0.85rem', color: 'var(--text-main)', marginBottom: '4px' }}>
              Database Isolation
            </div>
            <p style={{ fontSize: '0.775rem', color: 'var(--text-muted)', lineHeight: 1.5 }}>
              <code>producer_db</code>, <code>loyalty_db</code>, <code>analytics_db</code>, and <code>replay_db</code> are completely independent. No cross-database joins or foreign keys exist.
            </p>
          </div>

          <div style={{ padding: '12px 14px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ fontWeight: 600, fontSize: '0.85rem', color: 'var(--text-main)', marginBottom: '4px' }}>
              KRaft Multi-Partition Log
            </div>
            <p style={{ fontSize: '0.775rem', color: 'var(--text-muted)', lineHeight: 1.5 }}>
              Running Apache Kafka 4.x in KRaft consensus mode with 6 partitions on topic <code>purchase.events</code>, managed through Provectus Kafka UI on port 8080.
            </p>
          </div>
        </div>
      </section>

      {/* Terminal CLI Reference */}
      <section className="cyber-card" style={{ padding: '24px' }}>
        <h3 style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-main)', marginBottom: '12px', display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span>💻</span> Command-Line Automated Test Scripts
        </h3>
        <p style={{ fontSize: '0.825rem', color: 'var(--text-muted)', marginBottom: '12px' }}>
          All phases include deterministic CLI verification scripts that run against the active Kafka cluster:
        </p>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '10px', fontFamily: 'var(--font-mono)', fontSize: '0.775rem' }}>
          <div style={{ padding: '8px 12px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
            <span style={{ color: 'var(--color-primary)', fontWeight: 600 }}>pnpm demo:workload</span>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.725rem', marginTop: '2px' }}>Verify customer sequence ordering in Kafka</div>
          </div>
          <div style={{ padding: '8px 12px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
            <span style={{ color: 'var(--color-cyan)', fontWeight: 600 }}>pnpm demo:lag</span>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.725rem', marginTop: '2px' }}>Inspect dual consumer group offset lag</div>
          </div>
          <div style={{ padding: '8px 12px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
            <span style={{ color: 'var(--color-emerald)', fontWeight: 600 }}>pnpm demo:pause-resume</span>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.725rem', marginTop: '2px' }}>Demonstrate lag accumulation during pause</div>
          </div>
          <div style={{ padding: '8px 12px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
            <span style={{ color: 'var(--color-amber)', fontWeight: 600 }}>pnpm demo:correction</span>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.725rem', marginTop: '2px' }}>Run two-pass bounded replay correction test</div>
          </div>
        </div>
      </section>
    </div>
  );
};

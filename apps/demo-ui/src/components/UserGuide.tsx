import { uiConfig } from '../config/ui-config';
import { routeHash, type RealPage } from '../real/state/routes';

function realLink(page: RealPage) {
  return routeHash({ page, real: true, customerId: '', purchaseId: '', runId: '' });
}

export function UserGuide() {
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
          Welcome to the <strong>Kafka Streaming Lab</strong> observability platform. This
          laboratory demonstrates enterprise-grade Apache Kafka mechanics including{' '}
          <strong>
            KRaft consensus, key-based partition ordering, PostgreSQL Transactional Outbox,
            independent consumer group lag isolation, idempotent deduplication,
          </strong>{' '}
          and <strong>two-pass bounded replay correction</strong>.
        </p>

        <div style={{ display: 'flex', gap: '8px', marginTop: '16px', flexWrap: 'wrap' }}>
          <span className="badge-mono badge-violet">Apache Kafka 4.x KRaft</span>
          <span className="badge-mono badge-cyan">PostgreSQL Transactional Outbox</span>
          <span className="badge-mono badge-emerald">Idempotent Consumers (N+1 Commits)</span>
          <span className="badge-mono badge-amber">Bounded Two-Pass Replay</span>
        </div>
      </section>

      <section className="cyber-card real-guide">
        <h3>Presenter walkthrough</h3>
        <ol>
          <li>
            <a href={realLink('purchases')}>Create a purchase</a> for a fresh synthetic customer.
            Open its event evidence and wait for independent Loyalty and Analytics receipts.
          </li>
          <li>
            Submit a small same-customer burst in <a href={realLink('purchases')}>Purchases</a>.
            Compare customer sequences in <a href={realLink('events')}>Event Evidence</a> and
            inspect the records by key in <a href={uiConfig.VITE_KAFBAT_URL}>Kafbat</a>.
          </li>
          <li>
            Open <a href={realLink('consumers')}>Consumer Controls</a> and note both workers’
            settings. Pause Analytics, submit another purchase, then observe Loyalty progress and
            Analytics lag. Resume Analytics and verify catch-up. Restore the original settings.
          </li>
          <li>
            Invalidate a selected purchase and follow its correction link to{' '}
            <a href={realLink('recovery')}>Replay &amp; Correction</a>. Wait for the Loyalty
            invalidation evidence, start bounded replay and review staged changes. Global cutover
            can promote every eligible staged customer in the run.
          </li>
        </ol>
        <p>
          Use <code>pnpm demo:replay</code> for the separate replay boundary exercise.
        </p>
      </section>

      {/* Architectural Principles & Clean Architecture Summary */}
      <section className="cyber-card" style={{ padding: '24px' }}>
        <h3
          style={{
            fontSize: '1.1rem',
            fontWeight: 700,
            color: 'var(--text-main)',
            marginBottom: '14px',
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
          }}
        >
          <span>🏛</span> Architectural Invariants &amp; Infrastructure
        </h3>

        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
            gap: '14px',
          }}
        >
          <div
            style={{
              padding: '12px 14px',
              background: 'var(--bg-subtle)',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-subtle)',
            }}
          >
            <div
              style={{
                fontWeight: 600,
                fontSize: '0.85rem',
                color: 'var(--text-main)',
                marginBottom: '4px',
              }}
            >
              Offset Commits (N+1)
            </div>
            <p style={{ fontSize: '0.775rem', color: 'var(--text-muted)', lineHeight: 1.5 }}>
              A consumed offset $N$ is always committed as $N+1$ so Kafka rebalances resume from the
              exact next unprocessed message without duplication.
            </p>
          </div>

          <div
            style={{
              padding: '12px 14px',
              background: 'var(--bg-subtle)',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-subtle)',
            }}
          >
            <div
              style={{
                fontWeight: 600,
                fontSize: '0.85rem',
                color: 'var(--text-main)',
                marginBottom: '4px',
              }}
            >
              Database Idempotency
            </div>
            <p style={{ fontSize: '0.775rem', color: 'var(--text-muted)', lineHeight: 1.5 }}>
              Every consumer maintains a <code>processed_events</code> table. If an event is
              re-delivered, unique constraint deduplication ensures business logic executes exactly
              once.
            </p>
          </div>

          <div
            style={{
              padding: '12px 14px',
              background: 'var(--bg-subtle)',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-subtle)',
            }}
          >
            <div
              style={{
                fontWeight: 600,
                fontSize: '0.85rem',
                color: 'var(--text-main)',
                marginBottom: '4px',
              }}
            >
              Database Isolation
            </div>
            <p style={{ fontSize: '0.775rem', color: 'var(--text-muted)', lineHeight: 1.5 }}>
              <code>producer_db</code>, <code>loyalty_db</code>, <code>analytics_db</code>, and{' '}
              <code>replay_db</code> are completely independent. No cross-database joins or foreign
              keys exist.
            </p>
          </div>

          <div
            style={{
              padding: '12px 14px',
              background: 'var(--bg-subtle)',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-subtle)',
            }}
          >
            <div
              style={{
                fontWeight: 600,
                fontSize: '0.85rem',
                color: 'var(--text-main)',
                marginBottom: '4px',
              }}
            >
              KRaft Multi-Partition Log
            </div>
            <p style={{ fontSize: '0.775rem', color: 'var(--text-muted)', lineHeight: 1.5 }}>
              Running Apache Kafka 4.x in KRaft consensus mode with 6 partitions on topic{' '}
              <code>purchase.events</code>, managed through Kafbat UI on port 8080.
            </p>
          </div>
        </div>
      </section>

      {/* Terminal CLI Reference */}
      <section className="cyber-card" style={{ padding: '24px' }}>
        <h3
          style={{
            fontSize: '1.1rem',
            fontWeight: 700,
            color: 'var(--text-main)',
            marginBottom: '12px',
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
          }}
        >
          <span>💻</span> Command-Line Automated Test Scripts
        </h3>
        <p style={{ fontSize: '0.825rem', color: 'var(--text-muted)', marginBottom: '12px' }}>
          All phases include deterministic CLI verification scripts that run against the active
          Kafka cluster:
        </p>

        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
            gap: '10px',
            fontFamily: 'var(--font-mono)',
            fontSize: '0.775rem',
          }}
        >
          <div
            style={{
              padding: '8px 12px',
              background: 'var(--bg-subtle)',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-subtle)',
            }}
          >
            <span style={{ color: 'var(--color-primary)', fontWeight: 600 }}>
              pnpm demo:workload
            </span>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.725rem', marginTop: '2px' }}>
              Verify customer sequence ordering in Kafka
            </div>
          </div>
          <div
            style={{
              padding: '8px 12px',
              background: 'var(--bg-subtle)',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-subtle)',
            }}
          >
            <span style={{ color: 'var(--color-cyan)', fontWeight: 600 }}>pnpm demo:lag</span>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.725rem', marginTop: '2px' }}>
              Inspect dual consumer group offset lag
            </div>
          </div>
          <div
            style={{
              padding: '8px 12px',
              background: 'var(--bg-subtle)',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-subtle)',
            }}
          >
            <span style={{ color: 'var(--color-emerald)', fontWeight: 600 }}>
              pnpm demo:pause-resume
            </span>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.725rem', marginTop: '2px' }}>
              Demonstrate lag accumulation during pause
            </div>
          </div>
          <div
            style={{
              padding: '8px 12px',
              background: 'var(--bg-subtle)',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-subtle)',
            }}
          >
            <span style={{ color: 'var(--color-amber)', fontWeight: 600 }}>
              pnpm demo:correction
            </span>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.725rem', marginTop: '2px' }}>
              Run two-pass bounded replay correction test
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

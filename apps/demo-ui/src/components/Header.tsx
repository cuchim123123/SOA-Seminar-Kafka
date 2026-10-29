import React from 'react';
import type { ServiceHealthStatus } from '../api/client';

interface HeaderProps {
  health: ServiceHealthStatus;
  refreshing: boolean;
  onRefresh: () => void;
}

export const Header: React.FC<HeaderProps> = ({ health, refreshing, onRefresh }) => {
  const allHealthy = Object.values(health).every(Boolean);

  return (
    <header className="cyber-card" style={{ padding: '16px 24px', marginBottom: '20px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '16px' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <span
              style={{
                display: 'inline-block',
                width: '10px',
                height: '10px',
                borderRadius: '50%',
                backgroundColor: allHealthy ? 'var(--color-emerald)' : 'var(--color-amber)',
              }}
              className="animate-pulse-glow"
            />
            <h1
              style={{
                fontSize: '1.35rem',
                fontWeight: 800,
                letterSpacing: '-0.02em',
                color: 'var(--text-main)',
              }}
            >
              Kafka Streaming Lab
            </h1>
            <span className="badge-mono badge-violet">KRaft Cluster</span>
            <span className="badge-mono badge-cyan">Phase 0–5 Active</span>
          </div>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem', marginTop: '2px' }}>
            Interactive Kafka Architecture, Partition Ordering, Transactional Outbox &amp; Bounded Replay Studio
          </p>
        </div>

        {/* Cluster Services Health Badges */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
          <div className={`badge-mono ${health.producer ? 'badge-emerald' : 'badge-crimson'}`}>
            <span>API:3000</span> {health.producer ? '●' : '✕'}
          </div>
          <div className={`badge-mono ${health.loyalty ? 'badge-emerald' : 'badge-crimson'}`}>
            <span>Loyalty:3001</span> {health.loyalty ? '●' : '✕'}
          </div>
          <div className={`badge-mono ${health.analytics ? 'badge-emerald' : 'badge-crimson'}`}>
            <span>Analytics:3002</span> {health.analytics ? '●' : '✕'}
          </div>
          <div className={`badge-mono ${health.publisher ? 'badge-emerald' : 'badge-crimson'}`}>
            <span>Publisher:3003</span> {health.publisher ? '●' : '✕'}
          </div>
          <div className={`badge-mono ${health.replay ? 'badge-emerald' : 'badge-crimson'}`}>
            <span>Replay:3004</span> {health.replay ? '●' : '✕'}
          </div>

          <a
            href="http://localhost:8080"
            target="_blank"
            rel="noopener noreferrer"
            className="btn-cyber btn-outline"
            style={{ textDecoration: 'none' }}
          >
            Kafka UI ↗
          </a>

          <button
            onClick={onRefresh}
            className="btn-cyber btn-outline"
            disabled={refreshing}
            title="Refresh cluster telemetry"
          >
            {refreshing ? '⟳ Refreshing...' : '⟳ Refresh'}
          </button>
        </div>
      </div>
    </header>
  );
};

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
    <header className="cyber-card" style={{ padding: '16px 24px', marginBottom: '24px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '16px' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <span
              style={{
                display: 'inline-block',
                width: '12px',
                height: '12px',
                borderRadius: '50%',
                backgroundColor: allHealthy ? 'var(--color-emerald)' : 'var(--color-amber)',
                boxShadow: allHealthy ? '0 0 12px var(--color-emerald)' : '0 0 12px var(--color-amber)',
              }}
              className="animate-pulse-glow"
            />
            <h1
              style={{
                fontSize: '1.5rem',
                fontWeight: 800,
                letterSpacing: '-0.02em',
                background: 'linear-gradient(135deg, #f1f5f9 0%, #cbd5e1 50%, #94a3b8 100%)',
                WebkitBackgroundClip: 'text',
                WebkitTextFillColor: 'transparent',
              }}
            >
              KAFKA STREAMING LAB
            </h1>
            <span className="badge-mono badge-violet">KRaft Multi-Partition</span>
            <span className="badge-mono badge-cyan">Phase 0–5 Active</span>
          </div>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem', marginTop: '4px' }}>
            Interactive Kafka Architecture, Partitioning, Outbox &amp; Bounded Replay Studio
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
            style={{ padding: '6px 12px', fontSize: '0.75rem', textDecoration: 'none' }}
          >
            Kafka UI ↗
          </a>

          <button
            onClick={onRefresh}
            className="btn-cyber btn-outline"
            style={{ padding: '6px 12px', fontSize: '0.75rem' }}
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

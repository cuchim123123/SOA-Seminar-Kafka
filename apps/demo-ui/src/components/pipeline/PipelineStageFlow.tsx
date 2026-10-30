import React from 'react';

interface PipelineStageFlowProps {
  activeStep: number | null;
}

export const PipelineStageFlow: React.FC<PipelineStageFlowProps> = ({ activeStep }) => {
  return (
    <div
      style={{
        background: 'var(--bg-subtle)',
        borderRadius: 'var(--radius-md)',
        padding: '16px',
        border: '1px solid var(--border-subtle)',
      }}
    >
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '12px' }}>
        {/* Stage 1: Inbound HTTP */}
        <div
          style={{
            padding: '12px 14px',
            borderRadius: 'var(--radius-md)',
            background: activeStep === 1 ? '#eff6ff' : '#ffffff',
            border: activeStep === 1 ? '1px solid #3b82f6' : '1px solid var(--border-subtle)',
            boxShadow: 'var(--shadow-sm)',
          }}
        >
          <div style={{ fontSize: '0.7rem', color: 'var(--color-cyan)', fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
            STAGE 1: INBOUND
          </div>
          <div style={{ fontSize: '0.85rem', fontWeight: 600, marginTop: '2px', color: 'var(--text-main)' }}>HTTP Controller</div>
          <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '2px' }}>
            Validates schema, returns <span className="badge-mono badge-emerald">202 Accepted</span>
          </div>
          <div style={{ marginTop: '6px', fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: 'var(--text-dim)' }}>
            Fast async acceptance
          </div>
        </div>

        {/* Stage 2: Transactional Outbox (Postgres) */}
        <div
          style={{
            padding: '12px 14px',
            borderRadius: 'var(--radius-md)',
            background: activeStep === 2 ? '#f5f3ff' : '#ffffff',
            border: activeStep === 2 ? '1px solid #8b5cf6' : '1px solid var(--border-subtle)',
            boxShadow: 'var(--shadow-sm)',
          }}
        >
          <div style={{ fontSize: '0.7rem', color: 'var(--color-violet)', fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
            STAGE 2: POSTGRESQL
          </div>
          <div style={{ fontSize: '0.85rem', fontWeight: 600, marginTop: '2px', color: 'var(--text-main)' }}>Transactional Outbox</div>
          <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '2px' }}>
            Allocates sequence under row-lock into <span className="badge-mono badge-violet">outbox_events</span>
          </div>
          <div style={{ marginTop: '6px', fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: 'var(--text-dim)' }}>
            Single atomic DB commit
          </div>
        </div>

        {/* Stage 3: Outbox Publisher Relay */}
        <div
          style={{
            padding: '12px 14px',
            borderRadius: 'var(--radius-md)',
            background: activeStep === 3 ? '#ecfdf5' : '#ffffff',
            border: activeStep === 3 ? '1px solid #10b981' : '1px solid var(--border-subtle)',
            boxShadow: 'var(--shadow-sm)',
          }}
        >
          <div style={{ fontSize: '0.7rem', color: 'var(--color-emerald)', fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
            STAGE 3: PUBLISHER
          </div>
          <div style={{ fontSize: '0.85rem', fontWeight: 600, marginTop: '2px', color: 'var(--text-main)' }}>Outbox Relay Worker</div>
          <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '2px' }}>
            Polls pending records, publishes with <span className="badge-mono badge-cyan">key = customerId</span>
          </div>
          <div style={{ marginTop: '6px', fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: 'var(--text-dim)' }}>
            Awaits acks=-1 &amp; marks PUBLISHED
          </div>
        </div>

        {/* Stage 4: Kafka Broker Topic */}
        <div
          style={{
            padding: '12px 14px',
            borderRadius: 'var(--radius-md)',
            background: activeStep === 4 ? '#fffbeb' : '#ffffff',
            border: activeStep === 4 ? '1px solid #f59e0b' : '1px solid var(--border-subtle)',
            boxShadow: 'var(--shadow-sm)',
          }}
        >
          <div style={{ fontSize: '0.7rem', color: 'var(--color-amber)', fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
            STAGE 4: KAFKA
          </div>
          <div style={{ fontSize: '0.85rem', fontWeight: 600, marginTop: '2px', color: 'var(--text-main)' }}>purchase.events</div>
          <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '2px' }}>
            6 Partitions. Hashes key to exact partition, guarantees log order.
          </div>
          <div style={{ marginTop: '6px', fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: 'var(--text-dim)' }}>
            Append-only immutable log
          </div>
        </div>

        {/* Stage 5: Independent Consumer Groups */}
        <div
          style={{
            padding: '12px 14px',
            borderRadius: 'var(--radius-md)',
            background: activeStep === 4 ? '#f5f3ff' : '#ffffff',
            border: activeStep === 4 ? '1px solid #8b5cf6' : '1px solid var(--border-subtle)',
            boxShadow: 'var(--shadow-sm)',
          }}
        >
          <div style={{ fontSize: '0.7rem', color: 'var(--color-violet)', fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
            STAGE 5: FAN-OUT
          </div>
          <div style={{ fontSize: '0.85rem', fontWeight: 600, marginTop: '2px', color: 'var(--text-main)' }}>Dual Consumer Groups</div>
          <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '2px' }}>
            <span className="badge-mono badge-cyan">loyalty-v1</span> + <span className="badge-mono badge-amber">analytics-v1</span>
          </div>
          <div style={{ marginTop: '6px', fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: 'var(--text-dim)' }}>
            Independent offset tracking
          </div>
        </div>
      </div>
    </div>
  );
};

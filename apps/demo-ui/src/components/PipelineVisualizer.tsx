import React, { useState } from 'react';
import { api } from '../api/client';
import type { PurchaseCreatedResponse } from '../api/client';

interface PipelineVisualizerProps {
  onEventEmitted: (event: PurchaseCreatedResponse) => void;
  lastPurchase: PurchaseCreatedResponse | null;
}

export const PipelineVisualizer: React.FC<PipelineVisualizerProps> = ({
  onEventEmitted,
  lastPurchase,
}) => {
  const [customerId, setCustomerId] = useState<string>('U001');
  const [amountDollars, setAmountDollars] = useState<number>(120);
  const [invalidationPurchaseId, setInvalidationPurchaseId] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [activeStep, setActiveStep] = useState<number | null>(null);

  const handleCreatePurchase = async () => {
    setLoading(true);
    setStatusMessage(null);
    setActiveStep(1); // Client -> API

    try {
      setTimeout(() => setActiveStep(2), 250); // API -> Outbox (DB Transaction)
      setTimeout(() => setActiveStep(3), 550); // Outbox -> Kafka Broker
      setTimeout(() => setActiveStep(4), 850); // Kafka -> Consumer Fan-out

      const res = await api.createPurchase(customerId, Math.round(amountDollars * 100));
      onEventEmitted(res);
      setInvalidationPurchaseId(res.purchaseId);
      setStatusMessage(
        `✓ Purchase accepted (202): Seq #${res.customerSequence} for ${res.customerId} allocated in Outbox & relayed to Kafka.`,
      );
    } catch (err) {
      setStatusMessage(`✕ Error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading(false);
      setTimeout(() => setActiveStep(null), 2000);
    }
  };

  const handleInvalidate = async () => {
    if (!invalidationPurchaseId) {
      setStatusMessage('✕ Please specify a Purchase ID to invalidate.');
      return;
    }

    setLoading(true);
    setStatusMessage(null);
    setActiveStep(1);

    try {
      setTimeout(() => setActiveStep(2), 250);
      setTimeout(() => setActiveStep(3), 550);
      setTimeout(() => setActiveStep(4), 850);

      const res = await api.invalidatePurchase(invalidationPurchaseId, 'Return requested by customer');
      setStatusMessage(
        `✓ Invalidation processed: Seq #${res.customerSequence} for ${res.customerId} (Already Invalidated: ${res.isAlreadyInvalidated}).`,
      );
    } catch (err) {
      setStatusMessage(`✕ Invalidation failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading(false);
      setTimeout(() => setActiveStep(null), 2000);
    }
  };

  const handleSendBurst = async () => {
    setLoading(true);
    setStatusMessage('Emitting 5 sequential purchases for customer U001...');
    try {
      for (let i = 1; i <= 5; i++) {
        setActiveStep(2);
        const res = await api.createPurchase('U001', 5000 + i * 1000);
        onEventEmitted(res);
        setInvalidationPurchaseId(res.purchaseId);
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
      setStatusMessage('✓ Burst complete: 5 purchases emitted for U001. Check Partition Grid to see strictly ascending offsets!');
    } catch (err) {
      setStatusMessage(`✕ Burst failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading(false);
      setActiveStep(null);
    }
  };

  return (
    <section className="cyber-card" style={{ padding: '24px', marginBottom: '24px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '12px' }}>
        <div>
          <h2 style={{ fontSize: '1.15rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ color: 'var(--color-violet)' }}>⚡</span>
            INTERACTIVE EVENT FLOW &amp; TRANSACTIONAL OUTBOX PIPELINE
          </h2>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem' }}>
            Trigger real commands to observe PostgreSQL atomic sequence allocation, Outbox polling, Kafka key-based routing, and consumer group fan-out.
          </p>
        </div>

        {/* Action Controls */}
        <div style={{ display: 'flex', gap: '10px', alignItems: 'center', flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <label style={{ fontSize: '0.75rem', color: 'var(--text-dim)', fontFamily: varFallback('--font-mono') }}>Customer:</label>
            <select
              value={customerId}
              onChange={(e) => setCustomerId(e.target.value)}
              style={{
                background: 'var(--bg-input)',
                color: 'var(--text-main)',
                border: '1px solid var(--border-subtle)',
                borderRadius: 'var(--radius-sm)',
                padding: '6px 10px',
                fontSize: '0.8rem',
                fontFamily: 'var(--font-mono)',
                outline: 'none',
              }}
            >
              <option value="U001">U001 (Fixture Customer)</option>
              <option value="U002">U002 (Fixture Customer)</option>
              <option value="U003">U003 (Fixture Customer)</option>
              <option value={`U_${Date.now().toString().slice(-4)}`}>Random New Customer</option>
            </select>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <label style={{ fontSize: '0.75rem', color: 'var(--text-dim)', fontFamily: varFallback('--font-mono') }}>Amount ($):</label>
            <input
              type="number"
              value={amountDollars}
              onChange={(e) => setAmountDollars(Number(e.target.value))}
              style={{
                width: '80px',
                background: 'var(--bg-input)',
                color: 'var(--text-main)',
                border: '1px solid var(--border-subtle)',
                borderRadius: 'var(--radius-sm)',
                padding: '6px 8px',
                fontSize: '0.8rem',
                fontFamily: 'var(--font-mono)',
                outline: 'none',
              }}
              min="1"
              max="9999"
            />
          </div>

          <button
            onClick={handleCreatePurchase}
            disabled={loading}
            className="btn-cyber btn-violet"
          >
            {loading ? 'Transacting...' : '⚡ Emit Purchase'}
          </button>

          <button
            onClick={handleSendBurst}
            disabled={loading}
            className="btn-cyber btn-cyan"
            title="Send 5 purchases for U001 to prove single-partition sequential ordering"
          >
            🚀 Burst 5x (U001)
          </button>

          <button
            onClick={handleInvalidate}
            disabled={loading || !invalidationPurchaseId}
            className="btn-cyber btn-amber"
            title="Invalidate the most recent purchase to test Phase 5 compensation"
          >
            ↩ Invalidate Last
          </button>
        </div>
      </div>

      {/* Animated Pipeline Stage Flow */}
      <div
        style={{
          background: 'rgba(7, 10, 18, 0.7)',
          borderRadius: 'var(--radius-md)',
          padding: '20px',
          border: '1px solid var(--border-subtle)',
          position: 'relative',
        }}
      >
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '16px', position: 'relative' }}>
          
          {/* Stage 1: Client Request */}
          <div
            className="cyber-card"
            style={{
              padding: '14px',
              border: activeStep === 1 ? '1px solid var(--color-cyan)' : '1px solid var(--border-subtle)',
              boxShadow: activeStep === 1 ? 'var(--shadow-neon-cyan)' : 'none',
              transform: activeStep === 1 ? 'scale(1.02)' : 'none',
            }}
          >
            <div style={{ fontSize: '0.7rem', color: 'var(--color-cyan)', fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
              STAGE 1: INBOUND
            </div>
            <div style={{ fontSize: '0.9rem', fontWeight: 600, marginTop: '4px' }}>HTTP Controller</div>
            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '4px' }}>
              Validates Zod payload, returns <span className="badge-mono badge-emerald">202 Accepted</span>
            </div>
            <div style={{ marginTop: '8px', fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: 'var(--text-dim)' }}>
              Non-blocking response
            </div>
          </div>

          {/* Stage 2: Transactional Outbox (Postgres) */}
          <div
            className="cyber-card"
            style={{
              padding: '14px',
              border: activeStep === 2 ? '1px solid var(--color-violet)' : '1px solid var(--border-subtle)',
              boxShadow: activeStep === 2 ? 'var(--shadow-neon-violet)' : 'none',
              transform: activeStep === 2 ? 'scale(1.02)' : 'none',
            }}
          >
            <div style={{ fontSize: '0.7rem', color: 'var(--color-violet)', fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
              STAGE 2: POSTGRESQL
            </div>
            <div style={{ fontSize: '0.9rem', fontWeight: 600, marginTop: '4px' }}>Transactional Outbox</div>
            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '4px' }}>
              Allocates customer sequence under row-lock &amp; inserts into <span className="badge-mono badge-violet">outbox_events</span>
            </div>
            <div style={{ marginTop: '8px', fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: 'var(--text-dim)' }}>
              Single atomic DB commit
            </div>
          </div>

          {/* Stage 3: Outbox Publisher Relay */}
          <div
            className="cyber-card"
            style={{
              padding: '14px',
              border: activeStep === 3 ? '1px solid var(--color-emerald)' : '1px solid var(--border-subtle)',
              boxShadow: activeStep === 3 ? 'var(--shadow-neon-emerald)' : 'none',
              transform: activeStep === 3 ? 'scale(1.02)' : 'none',
            }}
          >
            <div style={{ fontSize: '0.7rem', color: 'var(--color-emerald)', fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
              STAGE 3: PUBLISHER
            </div>
            <div style={{ fontSize: '0.9rem', fontWeight: 600, marginTop: '4px' }}>Outbox Relay Worker</div>
            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '4px' }}>
              Polls pending events, publishes to Kafka with <span className="badge-mono badge-cyan">key = customerId</span>
            </div>
            <div style={{ marginTop: '8px', fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: 'var(--text-dim)' }}>
              Awaits acks=-1 &amp; marks PUBLISHED
            </div>
          </div>

          {/* Stage 4: Kafka Broker Topic */}
          <div
            className="cyber-card"
            style={{
              padding: '14px',
              border: activeStep === 4 ? '1px solid var(--color-amber)' : '1px solid var(--border-subtle)',
              boxShadow: activeStep === 4 ? 'var(--shadow-neon-amber)' : 'none',
              transform: activeStep === 4 ? 'scale(1.02)' : 'none',
            }}
          >
            <div style={{ fontSize: '0.7rem', color: 'var(--color-amber)', fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
              STAGE 4: KAFKA
            </div>
            <div style={{ fontSize: '0.9rem', fontWeight: 600, marginTop: '4px' }}>purchase.events</div>
            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '4px' }}>
              6 Partitions. Hashes key to exact partition, guarantees log order.
            </div>
            <div style={{ marginTop: '8px', fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: 'var(--text-dim)' }}>
              Append-only immutable log
            </div>
          </div>

          {/* Stage 5: Independent Consumer Groups */}
          <div
            className="cyber-card"
            style={{
              padding: '14px',
              border: activeStep === 4 ? '1px solid var(--color-violet)' : '1px solid var(--border-subtle)',
              boxShadow: activeStep === 4 ? 'var(--shadow-neon-violet)' : 'none',
              transform: activeStep === 4 ? 'scale(1.02)' : 'none',
            }}
          >
            <div style={{ fontSize: '0.7rem', color: 'var(--color-violet)', fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
              STAGE 5: FAN-OUT
            </div>
            <div style={{ fontSize: '0.9rem', fontWeight: 600, marginTop: '4px' }}>Dual Consumer Groups</div>
            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '4px' }}>
              <span className="badge-mono badge-cyan">loyalty-v1</span> + <span className="badge-mono badge-amber">analytics-v1</span>
            </div>
            <div style={{ marginTop: '8px', fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: 'var(--text-dim)' }}>
              Independent offset tracking
            </div>
          </div>
        </div>

        {/* Live Feedback Notification Banner */}
        {statusMessage && (
          <div
            style={{
              marginTop: '16px',
              padding: '10px 14px',
              borderRadius: 'var(--radius-sm)',
              background: statusMessage.startsWith('✓') ? 'rgba(16, 185, 129, 0.12)' : 'rgba(239, 68, 68, 0.12)',
              border: statusMessage.startsWith('✓') ? '1px solid rgba(16, 185, 129, 0.3)' : '1px solid rgba(239, 68, 68, 0.3)',
              color: statusMessage.startsWith('✓') ? '#a7f3d0' : '#fecaca',
              fontSize: '0.8rem',
              fontFamily: 'var(--font-mono)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
            }}
          >
            <span>{statusMessage}</span>
            {lastPurchase && (
              <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>
                ID: {lastPurchase.purchaseId.slice(0, 16)}...
              </span>
            )}
          </div>
        )}
      </div>
    </section>
  );
};

function varFallback(name: string): string {
  return `var(${name}, monospace)`;
}

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
  const [invalidationReason, setInvalidationReason] = useState<string>('Customer return requested');
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
        `✓ Purchase accepted (202): Customer ${res.customerId} event registered (Event: ${res.eventId.slice(0, 8)}...). Relaying to Kafka via Transactional Outbox.`,
      );
    } catch (err) {
      setStatusMessage(`✕ Error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading(false);
      setTimeout(() => setActiveStep(null), 1500);
    }
  };

  const handleInvalidate = async () => {
    if (!invalidationPurchaseId.trim()) {
      setStatusMessage('✕ Please specify or enter a Purchase ID to invalidate.');
      return;
    }

    setLoading(true);
    setStatusMessage(null);
    setActiveStep(1);

    try {
      setTimeout(() => setActiveStep(2), 250);
      setTimeout(() => setActiveStep(3), 550);
      setTimeout(() => setActiveStep(4), 850);

      const res = await api.invalidatePurchase(invalidationPurchaseId.trim(), invalidationReason);
      setStatusMessage(
        `✓ Invalidation compensation emitted: Customer ${res.customerId} allocated Seq #${res.customerSequence} (Already Invalidated: ${res.isAlreadyInvalidated}).`,
      );
    } catch (err) {
      setStatusMessage(`✕ Invalidation failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading(false);
      setTimeout(() => setActiveStep(null), 1500);
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
        await new Promise((resolve) => setTimeout(resolve, 150));
      }
      setStatusMessage('✓ Burst complete: 5 purchases emitted for U001. All mapped to the same Kafka partition with strictly ascending offsets.');
    } catch (err) {
      setStatusMessage(`✕ Burst failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading(false);
      setActiveStep(null);
    }
  };

  return (
    <section className="cyber-card" style={{ padding: '20px 24px', marginBottom: '20px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '12px' }}>
        <div>
          <h2 style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ color: 'var(--color-primary)' }}>⚡</span>
            Interactive Event Flow &amp; Transactional Outbox Pipeline
          </h2>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem', marginTop: '2px' }}>
            Trigger real commands to observe PostgreSQL atomic sequence allocation, Outbox polling, Kafka key-based routing, and consumer fan-out.
          </p>
        </div>

        {/* Action Controls */}
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center', flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <label style={{ fontSize: '0.75rem', color: 'var(--text-muted)', fontWeight: 600 }}>Customer:</label>
            <select
              value={customerId}
              onChange={(e) => setCustomerId(e.target.value)}
              style={{
                background: '#ffffff',
                color: 'var(--text-main)',
                border: '1px solid var(--border-medium)',
                borderRadius: 'var(--radius-sm)',
                padding: '6px 10px',
                fontSize: '0.8rem',
                fontFamily: 'var(--font-mono)',
                outline: 'none',
              }}
            >
              <option value="U001">U001 (Fixture)</option>
              <option value="U002">U002 (Fixture)</option>
              <option value="U003">U003 (Fixture)</option>
              <option value={`U_${Date.now().toString().slice(-4)}`}>Random New</option>
            </select>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <label style={{ fontSize: '0.75rem', color: 'var(--text-muted)', fontWeight: 600 }}>Amount ($):</label>
            <input
              type="number"
              value={amountDollars}
              onChange={(e) => setAmountDollars(Number(e.target.value))}
              style={{
                width: '74px',
                background: '#ffffff',
                color: 'var(--text-main)',
                border: '1px solid var(--border-medium)',
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

      {/* Pipeline Stage Flow (Bright, crisp styling) */}
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

        {/* Live Feedback Notification Banner */}
        {statusMessage && (
          <div
            style={{
              marginTop: '12px',
              padding: '10px 14px',
              borderRadius: 'var(--radius-sm)',
              background: statusMessage.startsWith('✓') ? '#ecfdf5' : '#fef2f2',
              border: statusMessage.startsWith('✓') ? '1px solid #a7f3d0' : '1px solid #fecaca',
              color: statusMessage.startsWith('✓') ? '#065f46' : '#991b1b',
              fontSize: '0.8rem',
              fontFamily: 'var(--font-mono)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
            }}
          >
            <span>{statusMessage}</span>
            {lastPurchase && (
              <span style={{ fontSize: '0.75rem', color: '#64748b' }}>
                Purchase ID: {lastPurchase.purchaseId.slice(0, 16)}...
              </span>
            )}
          </div>
        )}
      </div>

      {/* 2. Compensating Action: Invalidation Studio */}
      <div
        style={{
          marginTop: '16px',
          padding: '16px 20px',
          borderRadius: 'var(--radius-md)',
          background: '#fffbeb',
          border: '1px solid #fde68a',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px', flexWrap: 'wrap', gap: '8px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ fontSize: '1.1rem', color: '#d97706' }}>↩</span>
            <span style={{ fontSize: '0.9rem', fontWeight: 700, color: '#92400e' }}>
              Phase 5 Compensation: Invalidate Purchase Event
            </span>
          </div>
          <span className="badge-mono badge-amber">POST /api/v1/purchases/:purchaseId/invalidate</span>
        </div>

        <p style={{ fontSize: '0.8rem', color: '#78350f', marginBottom: '12px', lineHeight: '1.4' }}>
          In Kafka event streaming, past records are <strong>immutable</strong>. Rather than deleting rows or mutating Kafka log offsets, an explicit <code style={{ fontFamily: 'var(--font-mono)', fontWeight: 600 }}>PurchaseInvalidated</code> event is emitted into the customer's partition with an incremented sequence number.
        </p>

        <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            value={invalidationPurchaseId}
            onChange={(e) => setInvalidationPurchaseId(e.target.value)}
            placeholder="Purchase ID (e.g. pur_01a188...)"
            style={{
              flex: '2 1 280px',
              padding: '7px 12px',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid #d97706',
              fontFamily: 'var(--font-mono)',
              fontSize: '0.8rem',
              background: '#ffffff',
            }}
          />

          <input
            type="text"
            value={invalidationReason}
            onChange={(e) => setInvalidationReason(e.target.value)}
            placeholder="Reason (e.g. Customer return)"
            style={{
              flex: '1 1 200px',
              padding: '7px 12px',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-medium)',
              fontSize: '0.8rem',
              background: '#ffffff',
            }}
          />

          <button
            type="button"
            onClick={handleInvalidate}
            disabled={loading || !invalidationPurchaseId.trim()}
            style={{
              background: 'linear-gradient(135deg, #d97706 0%, #b45309 100%)',
              color: '#ffffff',
              border: 'none',
              borderRadius: 'var(--radius-sm)',
              padding: '7px 18px',
              fontSize: '0.8rem',
              fontWeight: 600,
              cursor: loading || !invalidationPurchaseId.trim() ? 'not-allowed' : 'pointer',
              opacity: loading || !invalidationPurchaseId.trim() ? 0.6 : 1,
              boxShadow: 'var(--shadow-sm)',
            }}
          >
            {loading ? 'Invalidating...' : '↩ Invalidate Purchase'}
          </button>
        </div>
      </div>
    </section>
  );
};

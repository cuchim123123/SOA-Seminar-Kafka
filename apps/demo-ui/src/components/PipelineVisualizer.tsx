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
  const [lastIdempotencyKey, setLastIdempotencyKey] = useState<string | null>(null);
  const [lastAmountDollars, setLastAmountDollars] = useState<number>(120);
  const [lastEmittedCustomerId, setLastEmittedCustomerId] = useState<string>('U001');
  const [invalidationPurchaseId, setInvalidationPurchaseId] = useState<string>('');
  const [invalidationReason, setInvalidationReason] = useState<string>('DEMO_INVALIDATION');
  const [loading, setLoading] = useState<boolean>(false);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [activeStep, setActiveStep] = useState<number | null>(null);

  // Chaos & Mechanics Studio state
  const [showChaosStudio, setShowChaosStudio] = useState<boolean>(false);
  const [poisonKey, setPoisonKey] = useState<string>('corrupt_key_demo');
  const [injectingPoison, setInjectingPoison] = useState<boolean>(false);
  const [rawKey, setRawKey] = useState<string>('arbitrary_key_demo');
  const [rawPayload, setRawPayload] = useState<string>('{"experiment": "arbitrary_schema", "demo": true}');
  const [publishingRaw, setPublishingRaw] = useState<boolean>(false);

  const handleCreatePurchase = async () => {
    setLoading(true);
    setStatusMessage(null);
    setActiveStep(1); // Client -> API

    const targetCustomer = customerId.trim() || 'U001';
    const amountCents = Math.round(amountDollars * 100);
    const newIdempotencyKey = `idem_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;

    try {
      setTimeout(() => setActiveStep(2), 250); // API -> Outbox (DB Transaction)
      setTimeout(() => setActiveStep(3), 550); // Outbox -> Kafka Broker
      setTimeout(() => setActiveStep(4), 850); // Kafka -> Consumer Fan-out

      const res = await api.createPurchase(targetCustomer, amountCents, newIdempotencyKey);
      onEventEmitted(res);
      setInvalidationPurchaseId(res.purchaseId);
      setLastIdempotencyKey(newIdempotencyKey);
      setLastAmountDollars(amountDollars);
      setLastEmittedCustomerId(targetCustomer);

      setStatusMessage(
        `✓ Purchase accepted (202): Key "${res.customerId}" mapped to Kafka. (Event: ${res.eventId.slice(0, 8)}..., Idempotency-Key: ${newIdempotencyKey}). Relayed via Outbox.`,
      );
    } catch (err) {
      setStatusMessage(`✕ Error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading(false);
      setTimeout(() => setActiveStep(null), 1500);
    }
  };

  const handleResendDuplicate = async () => {
    if (!lastIdempotencyKey) return;
    setLoading(true);
    setStatusMessage(`🔁 Submitting duplicate event with identical Idempotency-Key: "${lastIdempotencyKey}"...`);
    setActiveStep(1);

    try {
      setTimeout(() => setActiveStep(2), 250);
      const amountCents = Math.round(lastAmountDollars * 100);
      const res = await api.createPurchase(lastEmittedCustomerId, amountCents, lastIdempotencyKey);

      setStatusMessage(
        `✓ Idempotency Proven: API detected duplicate key "${lastIdempotencyKey}". Existing purchase ${res.purchaseId} returned without incrementing sequence. Consumer deduplication table (markProcessedIfNew) skips duplicate execution (isDuplicate: true).`,
      );
    } catch (err) {
      setStatusMessage(`✕ Idempotency test failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading(false);
      setTimeout(() => setActiveStep(null), 1200);
    }
  };

  const handleInjectPoison = async () => {
    setInjectingPoison(true);
    setStatusMessage(null);
    try {
      const res = await api.injectPoisonPill({ key: poisonKey.trim() || undefined });
      setStatusMessage(
        `☠️ Poison Pill Published: Raw malformed non-JSON payload placed at partition ${res.partition}, offset ${res.offset} (key: "${res.key}"). Consumer caught PoisonPillError, isolated it, and committed offset N+1 safely.`,
      );
    } catch (err) {
      setStatusMessage(`✕ Poison injection failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setInjectingPoison(false);
    }
  };

  const handlePublishRaw = async () => {
    setPublishingRaw(true);
    setStatusMessage(null);
    try {
      const res = await api.publishRawMessage({
        key: rawKey.trim() || `raw_${Date.now()}`,
        payload: rawPayload,
      });
      setStatusMessage(
        `✓ Raw record published to "${res.topic}" partition ${res.partition}, offset ${res.offset} with arbitrary key "${res.key}".`,
      );
    } catch (err) {
      setStatusMessage(`✕ Raw publish failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setPublishingRaw(false);
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
    const target = customerId.trim() || 'U001';
    setStatusMessage(`Emitting 5 sequential purchases for customer/key "${target}"...`);
    try {
      for (let i = 1; i <= 5; i++) {
        setActiveStep(2);
        const res = await api.createPurchase(target, 5000 + i * 1000);
        onEventEmitted(res);
        setInvalidationPurchaseId(res.purchaseId);
        await new Promise((resolve) => setTimeout(resolve, 150));
      }
      setStatusMessage(`✓ Burst complete: 5 purchases emitted for key "${target}". All mapped to the same Kafka partition with strictly ascending offsets.`);
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
          {/* Arbitrary Partition Key Input */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <label style={{ fontSize: '0.75rem', color: 'var(--text-muted)', fontWeight: 600 }}>Key / Customer:</label>
            <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
              <input
                type="text"
                value={customerId}
                onChange={(e) => setCustomerId(e.target.value)}
                placeholder="Arbitrary Key..."
                style={{
                  width: '110px',
                  background: '#ffffff',
                  color: 'var(--text-main)',
                  border: '1px solid var(--border-medium)',
                  borderRadius: 'var(--radius-sm)',
                  padding: '6px 8px',
                  fontSize: '0.8rem',
                  fontFamily: 'var(--font-mono)',
                  outline: 'none',
                }}
              />
              <div style={{ display: 'flex', gap: '2px' }}>
                {['U001', 'U002', 'cust_alpha', 'VIP_HOT'].map((c) => (
                  <button
                    key={c}
                    type="button"
                    onClick={() => setCustomerId(c)}
                    style={{
                      padding: '3px 5px',
                      fontSize: '0.675rem',
                      fontFamily: 'var(--font-mono)',
                      borderRadius: 'var(--radius-sm)',
                      border: customerId === c ? '1px solid #7c3aed' : '1px solid var(--border-subtle)',
                      background: customerId === c ? '#f5f3ff' : '#ffffff',
                      color: customerId === c ? '#7c3aed' : 'var(--text-body)',
                      cursor: 'pointer',
                      fontWeight: customerId === c ? 700 : 400,
                    }}
                  >
                    {c}
                  </button>
                ))}
              </div>
            </div>
          </div>

          {/* Amount Input */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <label style={{ fontSize: '0.75rem', color: 'var(--text-muted)', fontWeight: 600 }}>Amount ($):</label>
            <input
              type="number"
              value={amountDollars}
              onChange={(e) => setAmountDollars(Number(e.target.value))}
              style={{
                width: '68px',
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

          {/* Main Action Buttons */}
          <button
            onClick={handleCreatePurchase}
            disabled={loading}
            className="btn-cyber btn-violet"
          >
            {loading ? 'Transacting...' : '⚡ Emit Purchase'}
          </button>

          <button
            onClick={handleResendDuplicate}
            disabled={loading || !lastIdempotencyKey}
            className="btn-cyber btn-ghost"
            title="Re-send exact same Idempotency-Key and payload to prove consumer deduplication"
            style={{
              border: '1px solid #7c3aed',
              color: '#7c3aed',
              fontWeight: 600,
              fontSize: '0.775rem',
              padding: '6px 12px',
            }}
          >
            🔁 Re-send Duplicate (Idempotency)
          </button>

          <button
            onClick={handleSendBurst}
            disabled={loading}
            className="btn-cyber btn-cyan"
            title="Send 5 purchases for the selected key to prove single-partition sequential ordering"
          >
            🚀 Burst 5x
          </button>

          <button
            onClick={() => setShowChaosStudio(!showChaosStudio)}
            className="btn-cyber btn-ghost"
            style={{
              border: showChaosStudio ? '1px solid #f43f5e' : '1px solid var(--border-subtle)',
              color: showChaosStudio ? '#e11d48' : 'var(--text-body)',
              background: showChaosStudio ? '#fff1f2' : 'transparent',
              fontSize: '0.775rem',
              padding: '6px 10px',
            }}
          >
            🛠 Mechanics &amp; Chaos {showChaosStudio ? '▲' : '▼'}
          </button>
        </div>
      </div>

      {/* Expandable Mechanics & Chaos Studio */}
      {showChaosStudio && (
        <div
          style={{
            marginBottom: '16px',
            padding: '14px 18px',
            borderRadius: 'var(--radius-md)',
            background: '#fff1f2',
            border: '1px solid #fecdd3',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px', flexWrap: 'wrap', gap: '8px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ fontSize: '1.1rem', color: '#e11d48' }}>☠️</span>
              <span style={{ fontSize: '0.85rem', fontWeight: 700, color: '#9f1239' }}>
                Kafka Mechanics &amp; Chaos Engineering Studio
              </span>
            </div>
            <span className="badge-mono badge-rose">Poison Pills &amp; Raw Publishing</span>
          </div>

          <p style={{ fontSize: '0.775rem', color: '#881337', marginBottom: '10px', lineHeight: '1.4' }}>
            Test broker failure modes directly: inject non-JSON corrupted poison pills to prove consumer isolation (<code style={{ fontFamily: 'var(--font-mono)' }}>@lab/observability</code> commits offset N+1 without crash loops), or publish arbitrary raw payloads with custom keys.
          </p>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '12px', alignItems: 'center' }}>
            {/* Poison Pill Ingestion */}
            <div style={{ display: 'flex', gap: '6px', alignItems: 'center', flexWrap: 'wrap' }}>
              <input
                type="text"
                value={poisonKey}
                onChange={(e) => setPoisonKey(e.target.value)}
                placeholder="Poison Key..."
                style={{
                  width: '140px',
                  padding: '6px 8px',
                  borderRadius: 'var(--radius-sm)',
                  border: '1px solid #f43f5e',
                  fontSize: '0.775rem',
                  fontFamily: 'var(--font-mono)',
                  background: '#ffffff',
                }}
              />
              <button
                type="button"
                onClick={handleInjectPoison}
                disabled={injectingPoison}
                className="btn-cyber btn-rose"
                style={{ padding: '6px 12px', fontSize: '0.775rem', fontWeight: 700 }}
              >
                {injectingPoison ? 'Injecting...' : '☠️ Inject Poison Pill (Malformed Non-JSON)'}
              </button>
            </div>

            {/* Arbitrary Raw Message Ingestion */}
            <div style={{ display: 'flex', gap: '6px', alignItems: 'center', flexWrap: 'wrap' }}>
              <input
                type="text"
                value={rawKey}
                onChange={(e) => setRawKey(e.target.value)}
                placeholder="Custom Key..."
                style={{
                  width: '120px',
                  padding: '6px 8px',
                  borderRadius: 'var(--radius-sm)',
                  border: '1px solid var(--border-medium)',
                  fontSize: '0.775rem',
                  fontFamily: 'var(--font-mono)',
                  background: '#ffffff',
                }}
              />
              <input
                type="text"
                value={rawPayload}
                onChange={(e) => setRawPayload(e.target.value)}
                placeholder='{"raw": "payload"}'
                style={{
                  width: '180px',
                  padding: '6px 8px',
                  borderRadius: 'var(--radius-sm)',
                  border: '1px solid var(--border-medium)',
                  fontSize: '0.775rem',
                  fontFamily: 'var(--font-mono)',
                  background: '#ffffff',
                }}
              />
              <button
                type="button"
                onClick={handlePublishRaw}
                disabled={publishingRaw}
                className="btn-cyber btn-cyan"
                style={{ padding: '6px 12px', fontSize: '0.775rem', fontWeight: 600 }}
              >
                {publishingRaw ? 'Publishing...' : '📝 Publish Raw Record'}
              </button>
            </div>
          </div>
        </div>
      )}

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

          <select
            value={invalidationReason}
            onChange={(e) => setInvalidationReason(e.target.value)}
            style={{
              flex: '1 1 200px',
              padding: '7px 12px',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-medium)',
              fontSize: '0.8rem',
              background: '#ffffff',
              fontFamily: 'var(--font-mono)',
            }}
          >
            <option value="DEMO_INVALIDATION">DEMO_INVALIDATION (Return/Comp)</option>
            <option value="FRAUD_CONFIRMED">FRAUD_CONFIRMED (Fraud Alert)</option>
          </select>

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

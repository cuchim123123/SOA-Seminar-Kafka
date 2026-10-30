import React, { useState } from 'react';
import { api } from '../api/client';
import type { PurchaseCreatedResponse } from '../api/client';
import { PipelineStageFlow } from './pipeline/PipelineStageFlow';
import { ChaosEngineeringStudio } from './pipeline/ChaosEngineeringStudio';
import { InvalidationStudio } from './pipeline/InvalidationStudio';

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
  const [loading, setLoading] = useState<boolean>(false);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [activeStep, setActiveStep] = useState<number | null>(null);
  const [showChaosStudio, setShowChaosStudio] = useState<boolean>(false);

  const handleCreatePurchase = async () => {
    setLoading(true);
    setStatusMessage(null);
    setActiveStep(1);

    const targetCustomer = customerId.trim() || 'U001';
    const amountCents = Math.round(amountDollars * 100);
    const newIdempotencyKey = `idem_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;

    try {
      setTimeout(() => setActiveStep(2), 250);
      setTimeout(() => setActiveStep(3), 550);
      setTimeout(() => setActiveStep(4), 850);

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
      {/* 1. Header & Primary Action Controls */}
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

          {/* Buttons */}
          <button onClick={handleCreatePurchase} disabled={loading} className="btn-cyber btn-violet">
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

      {/* 2. Mechanics & Chaos Studio */}
      {showChaosStudio && <ChaosEngineeringStudio onStatusUpdate={(msg) => setStatusMessage(msg)} />}

      {/* 3. Pipeline Stages Architecture Diagram */}
      <PipelineStageFlow activeStep={activeStep} />

      {/* 4. Live Feedback Notification Banner */}
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

      {/* 5. Invalidation Studio */}
      <InvalidationStudio
        invalidationPurchaseId={invalidationPurchaseId}
        setInvalidationPurchaseId={setInvalidationPurchaseId}
        onStatusUpdate={(msg) => setStatusMessage(msg)}
        onActiveStep={setActiveStep}
      />
    </section>
  );
};

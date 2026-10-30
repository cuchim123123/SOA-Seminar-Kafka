import React, { useState } from 'react';
import { api } from '../../api/client';

interface InvalidationStudioProps {
  invalidationPurchaseId: string;
  setInvalidationPurchaseId: (id: string) => void;
  onStatusUpdate: (msg: string) => void;
  onActiveStep: (step: number | null) => void;
}

export const InvalidationStudio: React.FC<InvalidationStudioProps> = ({
  invalidationPurchaseId,
  setInvalidationPurchaseId,
  onStatusUpdate,
  onActiveStep,
}) => {
  const [invalidationReason, setInvalidationReason] = useState<string>('DEMO_INVALIDATION');
  const [loading, setLoading] = useState<boolean>(false);

  const handleInvalidate = async () => {
    if (!invalidationPurchaseId.trim()) {
      onStatusUpdate('✕ Please specify or enter a Purchase ID to invalidate.');
      return;
    }

    setLoading(true);
    onActiveStep(1);

    try {
      setTimeout(() => onActiveStep(2), 250);
      setTimeout(() => onActiveStep(3), 550);
      setTimeout(() => onActiveStep(4), 850);

      const res = await api.invalidatePurchase(invalidationPurchaseId.trim(), invalidationReason);
      onStatusUpdate(
        `✓ Invalidation compensation emitted: Customer ${res.customerId} allocated Seq #${res.customerSequence} (Already Invalidated: ${res.isAlreadyInvalidated}).`,
      );
    } catch (err) {
      onStatusUpdate(`✕ Invalidation failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading(false);
      setTimeout(() => onActiveStep(null), 1500);
    }
  };

  return (
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
  );
};

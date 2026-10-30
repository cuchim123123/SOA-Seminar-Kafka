import React from 'react';
import type { CustomerComparisonDto } from '../../api/client';

interface ReplayComparisonCardProps {
  comparison: CustomerComparisonDto | null;
  targetCustomer: string;
  setTargetCustomer: (id: string) => void;
  comparing: boolean;
  onFetchComparison: (customerId: string) => void;
  isCuttingOver: boolean;
  cutoverSuccess: string | null;
  onCutover: () => void;
}

export const ReplayComparisonCard: React.FC<ReplayComparisonCardProps> = ({
  comparison,
  targetCustomer,
  setTargetCustomer,
  comparing,
  onFetchComparison,
  isCuttingOver,
  cutoverSuccess,
  onCutover,
}) => {
  return (
    <div
      style={{
        padding: '16px 20px',
        borderRadius: 'var(--radius-md)',
        background: '#f5f3ff',
        border: '1px solid #ddd6fe',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '10px' }}>
        <div>
          <span style={{ fontWeight: 700, fontSize: '0.95rem', color: '#4c1d95' }}>
            Two-Pass Correction Discrepancy Inspector
          </span>
          <p style={{ fontSize: '0.785rem', color: '#6d28d9', marginTop: '2px' }}>
            Comparing live projection against replayed state reconstructed from Kafka historical events.
          </p>
        </div>

        {/* Customer Selector */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <label style={{ fontSize: '0.75rem', fontWeight: 600, color: '#5b21b6' }}>
            Customer:
          </label>
          <div style={{ display: 'flex', gap: '4px' }}>
            {['U001', 'U002', 'VIP_001'].map((c) => (
              <button
                key={c}
                type="button"
                onClick={() => {
                  setTargetCustomer(c);
                  onFetchComparison(c);
                }}
                style={{
                  padding: '3px 8px',
                  fontSize: '0.75rem',
                  fontFamily: 'var(--font-mono)',
                  borderRadius: 'var(--radius-sm)',
                  border: '1px solid #c4b5fd',
                  background: targetCustomer === c ? '#7c3aed' : '#ffffff',
                  color: targetCustomer === c ? '#ffffff' : '#5b21b6',
                  cursor: 'pointer',
                  fontWeight: 600,
                }}
              >
                {c}
              </button>
            ))}
          </div>
          <input
            type="text"
            value={targetCustomer}
            onChange={(e) => setTargetCustomer(e.target.value)}
            placeholder="Other Customer ID..."
            style={{
              padding: '4px 8px',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid #c4b5fd',
              fontFamily: 'var(--font-mono)',
              fontSize: '0.785rem',
              width: '120px',
              background: '#ffffff',
            }}
          />
          <button
            type="button"
            onClick={() => onFetchComparison(targetCustomer.trim())}
            disabled={comparing}
            style={{
              padding: '4px 10px',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid #7c3aed',
              background: '#7c3aed',
              color: '#ffffff',
              fontSize: '0.75rem',
              fontWeight: 600,
              cursor: 'pointer',
            }}
          >
            {comparing ? '...' : 'Compare'}
          </button>
        </div>
      </div>

      {comparison ? (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '12px', fontFamily: 'var(--font-mono)' }}>
          {/* Live State */}
          <div style={{ padding: '14px', background: '#ffffff', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', fontWeight: 600 }}>
              Live DB Projection (Customer {comparison.customerId}):
            </div>
            <div style={{ fontSize: '1.4rem', fontWeight: 800, color: 'var(--color-amber)', marginTop: '4px' }}>
              {comparison.live?.points ?? 0} pts
            </div>
            <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '4px' }}>
              Tier: <strong style={{ color: 'var(--text-main)' }}>{comparison.live?.tier || 'N/A'}</strong>
            </div>
            <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '2px' }}>
              Correction Flag: <strong style={{ color: comparison.live?.correctionRequired ? 'var(--color-crimson)' : 'var(--color-emerald)' }}>{comparison.live?.correctionRequired ? 'TRUE' : 'FALSE'}</strong>
            </div>
            <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '2px' }}>
              Last Seq: #{comparison.live?.lastCustomerSequence ?? 0}
            </div>
          </div>

          {/* Staged State */}
          <div style={{ padding: '14px', background: '#ffffff', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', fontWeight: 600 }}>
              Staged Projection (Replayed from Kafka):
            </div>
            <div style={{ fontSize: '1.4rem', fontWeight: 800, color: 'var(--color-emerald)', marginTop: '4px' }}>
              {comparison.staged?.points ?? 0} pts
            </div>
            <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '4px' }}>
              Tier: <strong style={{ color: 'var(--text-main)' }}>{comparison.staged?.tier || 'N/A'}</strong>
            </div>
            <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '2px' }}>
              Scanned Events: <strong>{comparison.staged?.scannedEventCount ?? 0}</strong>
            </div>
            <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '2px' }}>
              Replayed Seq: #{comparison.staged?.lastCustomerSequence ?? 0}
            </div>
          </div>

          {/* Discrepancy */}
          <div style={{ padding: '14px', background: '#ffffff', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', fontWeight: 600 }}>
              Discrepancy:
            </div>
            <div style={{ fontSize: '1.4rem', fontWeight: 800, color: comparison.discrepancy.pointsDifference !== 0 ? 'var(--color-crimson)' : 'var(--color-emerald)', marginTop: '4px' }}>
              {comparison.discrepancy.pointsDifference > 0 ? `+${comparison.discrepancy.pointsDifference}` : comparison.discrepancy.pointsDifference} pts
            </div>
            <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '4px' }}>
              Spend Delta: ${(comparison.discrepancy.spendDifferenceCents / 100).toFixed(2)}
            </div>
            <div style={{ fontSize: '0.725rem', color: 'var(--text-muted)', marginTop: '2px' }}>
              Tier Changed: <strong style={{ color: comparison.discrepancy.tierChanged ? 'var(--color-amber)' : 'var(--color-emerald)' }}>{comparison.discrepancy.tierChanged ? 'YES' : 'NO'}</strong>
            </div>
          </div>
        </div>
      ) : (
        <div style={{ padding: '18px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '0.8rem', background: '#ffffff', borderRadius: 'var(--radius-sm)' }}>
          No events found for customer <strong>{targetCustomer}</strong> in this replay staging run. Select another customer above.
        </div>
      )}

      {/* Controlled Cutover Action Card */}
      {comparison && (
        <div
          style={{
            marginTop: '14px',
            padding: '12px 16px',
            borderRadius: 'var(--radius-sm)',
            background:
              comparison.discrepancy.pointsDifference !== 0 || comparison.live?.correctionRequired
                ? '#ecfdf5'
                : '#f8fafc',
            border:
              comparison.discrepancy.pointsDifference !== 0 || comparison.live?.correctionRequired
                ? '1px solid #a7f3d0'
                : '1px solid var(--border-subtle)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexWrap: 'wrap',
            gap: '12px',
          }}
        >
          <div>
            <div
              style={{
                fontWeight: 700,
                fontSize: '0.85rem',
                color:
                  comparison.discrepancy.pointsDifference !== 0 || comparison.live?.correctionRequired
                    ? '#065f46'
                    : 'var(--text-body)',
              }}
            >
              {comparison.discrepancy.pointsDifference !== 0 || comparison.live?.correctionRequired
                ? '⚡ Ready for Controlled Cutover: Promote Staged Corrections to Live Database'
                : '✓ Live Projection Reconciled with Staged Historical Replay'}
            </div>
            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '2px' }}>
              {comparison.discrepancy.pointsDifference !== 0 || comparison.live?.correctionRequired
                ? 'Clicking Cutover writes recomputed points and eligible spend to live loyalty_accounts in PostgreSQL and clears the correction flag.'
                : 'Live database matches replayed Kafka historical truth. Discrepancy is 0.'}
            </div>
          </div>

          {(comparison.discrepancy.pointsDifference !== 0 || comparison.live?.correctionRequired) && (
            <button
              type="button"
              disabled={isCuttingOver}
              onClick={onCutover}
              style={{
                background: 'linear-gradient(135deg, #059669 0%, #047857 100%)',
                color: '#ffffff',
                border: 'none',
                borderRadius: 'var(--radius-sm)',
                padding: '7px 16px',
                fontSize: '0.8rem',
                fontWeight: 700,
                cursor: isCuttingOver ? 'not-allowed' : 'pointer',
                display: 'inline-flex',
                alignItems: 'center',
                gap: '6px',
                boxShadow: '0 2px 6px rgba(5, 150, 105, 0.3)',
              }}
            >
              {isCuttingOver ? 'Applying Cutover...' : '✓ Cutover / Save to Live DB'}
            </button>
          )}
        </div>
      )}

      {cutoverSuccess && (
        <div
          style={{
            marginTop: '12px',
            padding: '10px 14px',
            borderRadius: 'var(--radius-sm)',
            background: '#ecfdf5',
            border: '1px solid #6ee7b7',
            color: '#065f46',
            fontSize: '0.8rem',
            fontWeight: 600,
          }}
        >
          {cutoverSuccess}
        </div>
      )}
    </div>
  );
};

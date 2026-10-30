import React from 'react';

interface ScalePartitionsModalProps {
  show: boolean;
  onClose: () => void;
  topicName: string;
  partitionCount: number;
  targetCount: number;
  setTargetCount: (n: number) => void;
  scaling: boolean;
  scaleError: string | null;
  scaleSuccess: string | null;
  onScale: () => void;
}

export const ScalePartitionsModal: React.FC<ScalePartitionsModalProps> = ({
  show,
  onClose,
  topicName,
  partitionCount,
  targetCount,
  setTargetCount,
  scaling,
  scaleError,
  scaleSuccess,
  onScale,
}) => {
  if (!show) return null;

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        backgroundColor: 'rgba(15, 23, 42, 0.65)',
        backdropFilter: 'blur(3px)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 9999,
        padding: '16px',
      }}
      onClick={onClose}
    >
      <div
        style={{
          background: '#ffffff',
          borderRadius: 'var(--radius-md)',
          padding: '24px',
          maxWidth: '480px',
          width: '100%',
          boxShadow: 'var(--shadow-lg)',
          border: '1px solid var(--border-medium)',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px' }}>
          <h3 style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-main)' }}>
            Scale Topic Partitions
          </h3>
          <button
            onClick={onClose}
            style={{
              background: 'none',
              border: 'none',
              fontSize: '1.2rem',
              cursor: 'pointer',
              color: 'var(--text-muted)',
            }}
          >
            ✕
          </button>
        </div>

        <p style={{ fontSize: '0.825rem', color: 'var(--text-body)', marginBottom: '16px', lineHeight: '1.5' }}>
          Increase the partition count for <code style={{ fontFamily: 'var(--font-mono)' }}>{topicName}</code>.
          <strong> Note:</strong> Kafka allows partition counts to be <strong>increased only</strong>. Once scaled, you cannot decrease without recreating the topic.
        </p>

        <div style={{ marginBottom: '16px' }}>
          <label style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-muted)', display: 'block', marginBottom: '6px' }}>
            Current Partitions: <strong style={{ color: 'var(--text-main)' }}>{partitionCount}</strong>
          </label>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <label style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-main)' }}>
              New Partition Count:
            </label>
            <input
              type="number"
              min={partitionCount + 1}
              max={32}
              value={targetCount}
              onChange={(e) => setTargetCount(Math.max(partitionCount + 1, parseInt(e.target.value, 10) || partitionCount + 1))}
              style={{
                width: '80px',
                padding: '6px 10px',
                borderRadius: 'var(--radius-sm)',
                border: '1px solid var(--border-medium)',
                fontFamily: 'var(--font-mono)',
                fontSize: '0.85rem',
              }}
            />
          </div>
        </div>

        {scaleError && (
          <div style={{ padding: '8px 12px', background: '#fef2f2', border: '1px solid #fecaca', borderRadius: 'var(--radius-sm)', color: '#991b1b', fontSize: '0.8rem', marginBottom: '16px' }}>
            ✕ {scaleError}
          </div>
        )}

        {scaleSuccess && (
          <div style={{ padding: '8px 12px', background: '#ecfdf5', border: '1px solid #a7f3d0', borderRadius: 'var(--radius-sm)', color: '#065f46', fontSize: '0.8rem', marginBottom: '16px' }}>
            {scaleSuccess}
          </div>
        )}

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px' }}>
          <button
            type="button"
            onClick={onClose}
            disabled={scaling}
            className="btn-cyber btn-ghost"
            style={{ padding: '6px 14px', fontSize: '0.8rem' }}
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={onScale}
            disabled={scaling || targetCount <= partitionCount}
            className="btn-cyber btn-cyan"
            style={{ padding: '6px 16px', fontSize: '0.8rem', fontWeight: 700 }}
          >
            {scaling ? 'Scaling Partitions...' : `Scale to ${targetCount} Partitions`}
          </button>
        </div>
      </div>
    </div>
  );
};

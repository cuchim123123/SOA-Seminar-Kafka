import React from 'react';
import type { BrokerMessageRecord } from '../../api/client';

interface MessagePayloadModalProps {
  message: BrokerMessageRecord | null;
  onClose: () => void;
  onCopy: (text: string, label: string) => void;
}

export const MessagePayloadModal: React.FC<MessagePayloadModalProps> = ({
  message,
  onClose,
  onCopy,
}) => {
  if (!message) return null;

  const payloadString =
    typeof message.payload === 'object'
      ? JSON.stringify(message.payload, null, 2)
      : String(message.payload ?? '');

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
          maxWidth: '640px',
          width: '100%',
          maxHeight: '80vh',
          display: 'flex',
          flexDirection: 'column',
          boxShadow: 'var(--shadow-lg)',
          border: '1px solid var(--border-medium)',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span className="badge-mono badge-cyan">Offset: {message.offset}</span>
            <span className="badge-mono badge-violet">P#{message.partition}</span>
            <span style={{ fontSize: '0.85rem', fontWeight: 600, color: 'var(--text-main)', fontFamily: 'var(--font-mono)' }}>
              Key: {message.key || 'null'}
            </span>
          </div>
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

        <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginBottom: '8px', fontFamily: 'var(--font-mono)' }}>
          Timestamp: {message.timestamp}
        </div>

        <pre
          style={{
            flex: 1,
            overflowY: 'auto',
            background: '#0f172a',
            color: '#38bdf8',
            padding: '16px',
            borderRadius: 'var(--radius-sm)',
            fontSize: '0.75rem',
            fontFamily: 'var(--font-mono)',
            margin: 0,
            lineHeight: '1.4',
          }}
        >
          {payloadString}
        </pre>

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px', marginTop: '16px' }}>
          <button
            type="button"
            onClick={() => onCopy(payloadString, 'Payload copied')}
            className="btn-cyber btn-ghost"
            style={{ padding: '6px 14px', fontSize: '0.8rem' }}
          >
            📋 Copy JSON
          </button>
          <button
            type="button"
            onClick={onClose}
            className="btn-cyber btn-cyan"
            style={{ padding: '6px 14px', fontSize: '0.8rem' }}
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
};

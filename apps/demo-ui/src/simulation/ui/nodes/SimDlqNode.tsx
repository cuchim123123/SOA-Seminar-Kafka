import React from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { SimDlqData } from '../../domain/types';

export const SimDlqNode: React.FC<NodeProps> = ({ data, selected }) => {
  const d = data as unknown as SimDlqData;
  const deadCount = d.deadLetterCount || 0;

  return (
    <div
      style={{
        background: '#18181b',
        border: selected ? '2px solid #ef4444' : '1px solid #3f3f46',
        borderRadius: '10px',
        padding: '12px 14px',
        width: '210px',
        boxShadow: selected ? '0 0 16px rgba(239, 68, 68, 0.4)' : '0 4px 12px rgba(0, 0, 0, 0.25)',
        fontFamily: 'var(--font-sans)',
        position: 'relative',
        color: '#f4f4f5',
      }}
    >
      {/* Input Port on Left */}
      <Handle
        type="target"
        position={Position.Left}
        id="dlq-route"
        style={{
          background: '#ef4444',
          width: 9,
          height: 9,
          borderRadius: '50%',
          border: '2px solid #18181b',
        }}
      />

      {/* Header */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          marginBottom: '8px',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '1.25rem' }}>☠️</span>
          <div>
            <div style={{ fontSize: '0.825rem', fontWeight: 800, color: '#f87171' }}>
              {d.title || 'Dead-Letter Queue'}
            </div>
            <div style={{ fontSize: '0.65rem', color: '#a1a1aa', fontFamily: 'var(--font-mono)' }}>
              {d.topicName || 'purchases.DLQ'}
            </div>
          </div>
        </div>

        <span
          style={{
            fontSize: '0.6rem',
            fontWeight: 800,
            padding: '2px 5px',
            borderRadius: '4px',
            background: deadCount > 0 ? '#450a0a' : '#27272a',
            color: deadCount > 0 ? '#f87171' : '#a1a1aa',
            border: deadCount > 0 ? '1px solid #ef4444' : '1px solid #3f3f46',
          }}
        >
          {deadCount > 0 ? 'ISOLATING' : 'IDLE'}
        </span>
      </div>

      {/* Metrics Strip */}
      <div
        style={{
          background: '#09090b',
          borderRadius: '6px',
          padding: '8px 10px',
          border: '1px solid #27272a',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          fontFamily: 'var(--font-mono)',
        }}
      >
        <span style={{ fontSize: '0.675rem', color: '#a1a1aa' }}>Poison Pills:</span>
        <span
          style={{
            fontSize: '0.95rem',
            fontWeight: 800,
            color: deadCount > 0 ? '#ef4444' : '#10b981',
          }}
        >
          {deadCount}
        </span>
      </div>
    </div>
  );
};

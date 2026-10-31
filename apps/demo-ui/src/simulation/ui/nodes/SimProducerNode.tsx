import React from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { SimProducerData } from '../../domain/types';

export const SimProducerNode: React.FC<NodeProps> = ({ data, selected }) => {
  const d = data as unknown as SimProducerData;

  return (
    <div
      style={{
        background: '#ffffff',
        border: selected ? '2px solid #7c3aed' : '1px solid #cbd5e1',
        borderRadius: '10px',
        padding: '12px 14px',
        width: '210px',
        boxShadow: selected
          ? '0 0 0 3px rgba(124, 58, 237, 0.25), 0 4px 12px rgba(0, 0, 0, 0.08)'
          : '0 2px 6px rgba(0, 0, 0, 0.05)',
        fontFamily: 'var(--font-sans)',
        cursor: 'pointer',
        transition: 'all 0.15s ease',
      }}
    >
      {/* Top Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '1.25rem' }}>{d.icon || '🚀'}</span>
          <div>
            <div style={{ fontSize: '0.85rem', fontWeight: 800, color: '#0f172a' }}>
              {d.title || 'Purchase Producer'}
            </div>
            <div style={{ fontSize: '0.675rem', color: '#64748b', fontFamily: 'var(--font-mono)' }}>
              Producer Client
            </div>
          </div>
        </div>

        <span
          style={{
            fontSize: '0.625rem',
            fontWeight: 800,
            padding: '2px 6px',
            borderRadius: '4px',
            background: '#f3e8ff',
            color: '#7c3aed',
            border: '1px solid #d8b4fe',
          }}
        >
          SOURCE
        </span>
      </div>

      {/* Configuration Details */}
      <div
        style={{
          background: '#f8fafc',
          borderRadius: '6px',
          padding: '6px 8px',
          display: 'flex',
          flexDirection: 'column',
          gap: '4px',
          fontSize: '0.7rem',
          fontFamily: 'var(--font-mono)',
          border: '1px solid #e2e8f0',
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: '#64748b' }}>Key Strategy:</span>
          <span style={{ fontWeight: 700, color: '#0284c7' }}>Murmur2 ({d.messageKey || 'U001'})</span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: '#64748b' }}>Target Topic:</span>
          <span style={{ fontWeight: 700, color: '#059669' }}>{d.targetTopic || 'purchases'}</span>
        </div>
      </div>

      {/* Output Port on Right */}
      <Handle
        type="source"
        position={Position.Right}
        id="publish"
        style={{
          background: '#38bdf8',
          width: 9,
          height: 9,
          border: '2px solid #ffffff',
        }}
      />
    </div>
  );
};

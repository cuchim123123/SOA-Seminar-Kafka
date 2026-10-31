import React from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { SimDatabaseData } from '../../domain/types';

export const SimDatabaseNode: React.FC<NodeProps> = ({ data, selected }) => {
  const d = data as unknown as SimDatabaseData;
  const count = Number(d.committedRecordCount) || 0;

  return (
    <div
      style={{
        background: '#ffffff',
        border: selected ? '2px solid #0284c7' : '1px solid #cbd5e1',
        borderRadius: '10px',
        padding: '12px 14px',
        width: '210px',
        boxShadow: selected
          ? '0 0 0 3px rgba(2, 132, 199, 0.25), 0 4px 12px rgba(0, 0, 0, 0.08)'
          : '0 2px 6px rgba(0, 0, 0, 0.05)',
        fontFamily: 'var(--font-sans)',
        cursor: 'pointer',
        transition: 'all 0.15s ease',
      }}
    >
      {/* Input Port on Left */}
      <Handle
        type="target"
        position={Position.Left}
        id="handler-action"
        style={{
          background: '#10b981',
          width: 9,
          height: 9,
          border: '2px solid #ffffff',
        }}
      />

      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '1.25rem' }}>
            {d.icon ||
              (d.engine?.toLowerCase().includes('mongo')
                ? '🍃'
                : d.engine?.toLowerCase().includes('redis')
                  ? '⚡'
                  : d.engine?.toLowerCase().includes('elastic')
                    ? '🔍'
                    : d.engine?.toLowerCase().includes('click')
                      ? '📊'
                      : '🗄️')}
          </span>
          <div>
            <div style={{ fontSize: '0.85rem', fontWeight: 800, color: '#0f172a' }}>
              {d.title || 'Storage Sink'}
            </div>
            <div style={{ fontSize: '0.675rem', color: '#64748b', fontFamily: 'var(--font-mono)' }}>
              {d.engine || d.databaseName || 'Database'}
            </div>
          </div>
        </div>

        <span
          style={{
            fontSize: '0.625rem',
            fontWeight: 800,
            padding: '2px 6px',
            borderRadius: '4px',
            background: '#ecfdf5',
            color: '#059669',
            border: '1px solid #a7f3d0',
          }}
        >
          SINK
        </span>
      </div>

      {/* Database Stats */}
      <div
        style={{
          background: '#f8fafc',
          borderRadius: '6px',
          padding: '6px 8px',
          fontSize: '0.7rem',
          fontFamily: 'var(--font-mono)',
          border: '1px solid #e2e8f0',
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
          <span style={{ color: '#64748b' }}>Committed Records:</span>
          <span style={{ fontWeight: 800, color: '#059669' }}>{count}</span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: '#64748b' }}>Write Latency:</span>
          <span style={{ fontWeight: 700, color: '#0f172a' }}>{d.writeDelayMs || 20}ms</span>
        </div>
      </div>
    </div>
  );
};

import React from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { SimOutboxTableData } from '../../domain/types';

export const SimOutboxTableNode: React.FC<NodeProps> = ({ data, selected }) => {
  const d = data as unknown as SimOutboxTableData;
  const pending = Number(d.pendingRecordsCount) || 0;
  const published = Number(d.publishedRecordsCount) || 0;

  return (
    <div
      style={{
        background: '#ffffff',
        border: selected ? '2px solid #ea580c' : '1px solid #fed7aa',
        borderRadius: '10px',
        padding: '12px 14px',
        width: '210px',
        boxShadow: selected
          ? '0 0 0 3px rgba(234, 88, 12, 0.25), 0 4px 12px rgba(0, 0, 0, 0.08)'
          : '0 2px 6px rgba(0, 0, 0, 0.05)',
        fontFamily: 'var(--font-sans)',
        cursor: 'pointer',
        transition: 'all 0.15s ease',
      }}
    >
      {/* Input Port (Left): accepts events from Producer / Application */}
      <Handle
        type="target"
        position={Position.Left}
        id="publish"
        style={{
          background: '#ea580c',
          width: 9,
          height: 9,
          border: '2px solid #ffffff',
        }}
      />

      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '1.25rem' }}>{d.icon || '📦'}</span>
          <div>
            <div style={{ fontSize: '0.85rem', fontWeight: 800, color: '#0f172a' }}>
              {d.title || 'Outbox Table'}
            </div>
            <div style={{ fontSize: '0.675rem', color: '#64748b', fontFamily: 'var(--font-mono)' }}>
              {d.tableName || 'outbox_events'}
            </div>
          </div>
        </div>

        <span
          style={{
            fontSize: '0.625rem',
            fontWeight: 800,
            padding: '2px 6px',
            borderRadius: '4px',
            background: pending > 0 ? '#fff7ed' : '#f8fafc',
            color: pending > 0 ? '#c2410c' : '#64748b',
            border: pending > 0 ? '1px solid #fdba74' : '1px solid #e2e8f0',
          }}
        >
          {pending > 0 ? 'BUFFERING' : 'IDLE'}
        </span>
      </div>

      {/* Outbox Metrics */}
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
          <span style={{ color: '#64748b' }}>Pending Rows:</span>
          <span style={{ fontWeight: 800, color: pending > 0 ? '#ea580c' : '#0f172a' }}>{pending}</span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: '#64748b' }}>Published to Log:</span>
          <span style={{ fontWeight: 700, color: '#16a34a' }}>{published}</span>
        </div>
      </div>

      {/* Output Port (Right): streams to CDC Relay */}
      <Handle
        type="source"
        position={Position.Right}
        id="cdc-tail"
        style={{
          background: '#ea580c',
          width: 9,
          height: 9,
          border: '2px solid #ffffff',
        }}
      />
    </div>
  );
};

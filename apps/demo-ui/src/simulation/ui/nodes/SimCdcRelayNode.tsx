import React from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { SimCdcRelayData } from '../../domain/types';

export const SimCdcRelayNode: React.FC<NodeProps> = ({ data, selected }) => {
  const d = data as unknown as SimCdcRelayData;
  const streamed = Number(d.streamedCount) || 0;

  return (
    <div
      style={{
        background: '#ffffff',
        border: selected ? '2px solid #0891b2' : '1px solid #a5f3fc',
        borderRadius: '10px',
        padding: '12px 14px',
        width: '215px',
        boxShadow: selected
          ? '0 0 0 3px rgba(8, 145, 178, 0.25), 0 4px 12px rgba(0, 0, 0, 0.08)'
          : '0 2px 6px rgba(0, 0, 0, 0.05)',
        fontFamily: 'var(--font-sans)',
        cursor: 'pointer',
        transition: 'all 0.15s ease',
      }}
    >
      {/* Input Port (Left): accepts cdc-tail from Outbox Table */}
      <Handle
        type="target"
        position={Position.Left}
        id="cdc-tail"
        style={{
          background: '#0891b2',
          width: 9,
          height: 9,
          border: '2px solid #ffffff',
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
          <span style={{ fontSize: '1.25rem' }}>{d.icon || '🔄'}</span>
          <div>
            <div style={{ fontSize: '0.85rem', fontWeight: 800, color: '#0f172a' }}>
              {d.title || 'CDC / Outbox Relay'}
            </div>
            <div style={{ fontSize: '0.675rem', color: '#64748b', fontFamily: 'var(--font-mono)' }}>
              {d.relayType === 'debezium' ? 'Debezium CDC Engine' : 'Polling Worker'}
            </div>
          </div>
        </div>

        <span
          style={{
            fontSize: '0.625rem',
            fontWeight: 800,
            padding: '2px 6px',
            borderRadius: '4px',
            background: '#ecfeff',
            color: '#0e7490',
            border: '1px solid #a5f3fc',
          }}
        >
          {d.status || 'STREAMING'}
        </span>
      </div>

      {/* Relay Metrics */}
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
          <span style={{ color: '#64748b' }}>Events Relayed:</span>
          <span style={{ fontWeight: 800, color: '#0891b2' }}>{streamed}</span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: '#64748b' }}>Transfer timing:</span>
          <span style={{ fontWeight: 700, color: '#0f172a' }}>{'Model default'}</span>
        </div>
      </div>

      {/* Output Port (Right): streams to Kafka Cluster */}
      <Handle
        type="source"
        position={Position.Right}
        id="publish"
        style={{
          background: '#0891b2',
          width: 9,
          height: 9,
          border: '2px solid #ffffff',
        }}
      />
    </div>
  );
};

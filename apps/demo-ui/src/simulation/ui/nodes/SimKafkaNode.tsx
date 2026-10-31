import React from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { SimKafkaClusterData } from '../../domain/types';

export const SimKafkaNode: React.FC<NodeProps> = ({ data, selected }) => {
  const d = data as unknown as SimKafkaClusterData;
  const brokerCount = d.brokerCount || 3;
  const partitionCount = d.partitionCount || 4;

  return (
    <div
      style={{
        background: '#ffffff',
        border: selected ? '2px solid #0284c7' : '1px solid #cbd5e1',
        borderRadius: '10px',
        padding: '12px 14px',
        width: '230px',
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
        id="publish"
        style={{
          background: '#38bdf8',
          width: 9,
          height: 9,
          border: '2px solid #ffffff',
        }}
      />

      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '1.25rem' }}>📦</span>
          <div>
            <div style={{ fontSize: '0.85rem', fontWeight: 800, color: '#0f172a' }}>
              {d.title || 'Kafka Cluster'}
            </div>
            <div style={{ fontSize: '0.675rem', color: '#64748b', fontFamily: 'var(--font-mono)' }}>
              KRaft Multi-Broker
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
          ONLINE
        </span>
      </div>

      {/* Internal Brokers Indicator */}
      <div style={{ marginBottom: '8px' }}>
        <div style={{ fontSize: '0.65rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
          CLUSTER BROKERS ({brokerCount}):
        </div>
        <div style={{ display: 'flex', gap: '4px' }}>
          {Array.from({ length: brokerCount }).map((_, idx) => (
            <span
              key={idx}
              style={{
                fontSize: '0.625rem',
                fontFamily: 'var(--font-mono)',
                fontWeight: 700,
                padding: '2px 6px',
                borderRadius: '4px',
                background: idx === 0 ? '#eff6ff' : '#f1f5f9',
                color: idx === 0 ? '#0284c7' : '#334155',
                border: idx === 0 ? '1px solid #bfdbfe' : '1px solid #e2e8f0',
              }}
              title={idx === 0 ? 'Broker 1 (Controller & Partition Leader)' : `Broker ${idx + 1}`}
            >
              B{idx + 1}{idx === 0 ? '*' : ''}
            </span>
          ))}
        </div>
      </div>

      {/* Topic & Partitions Strip */}
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
          <span style={{ color: '#64748b' }}>Topic:</span>
          <span style={{ fontWeight: 700, color: '#0f172a' }}>{d.activeTopic || 'purchases'}</span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: '#64748b' }}>Partitions:</span>
          <span style={{ fontWeight: 700, color: '#7c3aed' }}>{partitionCount} (P0..P{partitionCount - 1})</span>
        </div>
      </div>

      {/* Per-Partition Grid with LEO and Lag */}
      <div style={{ marginTop: '8px' }}>
        <div style={{ fontSize: '0.65rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
          PARTITIONS &amp; LAG:
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '4px' }}>
          {Array.from({ length: partitionCount }).map((_, pId) => {
            const lag = d.partitionLags ? d.partitionLags[pId] ?? 0 : 0;
            const leo = d.partitionLogEndOffsets ? d.partitionLogEndOffsets[pId] ?? 0 : 0;
            const hasLag = lag > 0;

            return (
              <div
                key={pId}
                style={{
                  fontSize: '0.625rem',
                  fontFamily: 'var(--font-mono)',
                  padding: '3px 6px',
                  borderRadius: '4px',
                  background: hasLag ? '#fffbeb' : '#f8fafc',
                  border: hasLag ? '1px solid #fde68a' : '1px solid #e2e8f0',
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                }}
              >
                <span style={{ fontWeight: 700, color: '#0f172a' }}>P{pId}</span>
                <span style={{ color: '#64748b' }}>
                  {hasLag ? (
                    <strong style={{ color: '#d97706' }}>Lag: {lag}</strong>
                  ) : (
                    <span>Off: {leo}</span>
                  )}
                </span>
              </div>
            );
          })}
        </div>
      </div>

      {/* Output Port on Right */}
      <Handle
        type="source"
        position={Position.Right}
        id="dispatch"
        style={{
          background: '#fbbf24',
          width: 9,
          height: 9,
          border: '2px solid #ffffff',
        }}
      />
    </div>
  );
};

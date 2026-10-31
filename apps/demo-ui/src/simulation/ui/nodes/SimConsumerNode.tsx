import React from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { SimConsumerData } from '../../domain/types';

export const SimConsumerNode: React.FC<NodeProps> = ({ data, selected }) => {
  const d = data as unknown as SimConsumerData;
  const partitions = d.assignedPartitions || [];
  const status = d.status || 'POLLING';

  const isIdle = partitions.length === 0;
  const isPaused = Boolean(d.isPaused || status === 'PAUSED');

  const statusBg =
    status === 'CRASHED'
      ? '#fee2e2'
      : isPaused
        ? '#fef3c7'
        : status === 'PROCESSING'
          ? '#fef3c7'
          : isIdle
            ? '#f1f5f9'
            : '#ecfdf5';

  const statusColor =
    status === 'CRASHED'
      ? '#dc2626'
      : isPaused
        ? '#d97706'
        : status === 'PROCESSING'
          ? '#d97706'
          : isIdle
            ? '#64748b'
            : '#059669';

  return (
    <div
      style={{
        background: '#ffffff',
        border: selected ? '2px solid #059669' : isPaused ? '1px solid #f59e0b' : '1px solid #cbd5e1',
        borderRadius: '10px',
        padding: '12px 14px',
        width: '225px',
        boxShadow: selected
          ? '0 0 0 3px rgba(5, 150, 105, 0.25), 0 4px 12px rgba(0, 0, 0, 0.08)'
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
        id="dispatch"
        style={{
          background: isPaused ? '#f59e0b' : '#fbbf24',
          width: 9,
          height: 9,
          border: '2px solid #ffffff',
        }}
      />

      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '1.25rem' }}>{d.icon || '👥'}</span>
          <div>
            <div style={{ fontSize: '0.85rem', fontWeight: 800, color: '#0f172a' }}>
              {d.title || 'Consumer Instance'}
            </div>
            <div style={{ fontSize: '0.675rem', color: '#64748b', fontFamily: 'var(--font-mono)' }}>
              Group: {d.groupId || 'loyalty-v1'}
            </div>
          </div>
        </div>

        <span
          style={{
            fontSize: '0.625rem',
            fontWeight: 800,
            padding: '2px 6px',
            borderRadius: '4px',
            background: statusBg,
            color: statusColor,
            border: `1px solid ${statusColor}44`,
          }}
        >
          {isPaused ? 'PAUSED' : isIdle ? 'IDLE' : status}
        </span>
      </div>

      {/* Assigned Partitions Strip */}
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
          <span style={{ color: '#64748b' }}>Assigned Partitions:</span>
          <span style={{ fontWeight: 800, color: isIdle ? '#94a3b8' : '#7c3aed' }}>
            {isIdle
              ? 'None (Idle)'
              : partitions.map((p) => {
                  const isPartCut = d.pausedPartitions?.includes(p);
                  return isPartCut ? `P${p}[✂️]` : `P${p}`;
                }).join(', ')}
          </span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: '#64748b' }}>Processing Delay:</span>
          <span style={{ fontWeight: 700, color: '#0f172a' }}>{d.processingDelayMs || 120}ms</span>
        </div>
        {status === 'PROCESSING' && d.currentProcessingPartition !== undefined && (
          <div
            style={{
              marginTop: '4px',
              paddingTop: '4px',
              borderTop: '1px solid #fde68a',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
            }}
          >
            <span style={{ color: '#d97706', fontWeight: 600 }}>Active Offset:</span>
            <span style={{ fontWeight: 800, color: '#b45309' }}>
              P{d.currentProcessingPartition}:#{d.currentProcessingOffset}
            </span>
          </div>
        )}
      </div>

      {/* Quick Pause / Resume Polling Button */}
      <div style={{ marginTop: '8px' }}>
        <button
          onClick={(e) => {
            e.stopPropagation();
            window.dispatchEvent(
              new CustomEvent('sim:toggle-pause-consumer', { detail: { id: d.id } }),
            );
          }}
          style={{
            width: '100%',
            padding: '4px 8px',
            borderRadius: '5px',
            border: isPaused ? '1px solid #16a34a' : '1px solid #f59e0b',
            background: isPaused ? '#f0fdf4' : '#fffbeb',
            color: isPaused ? '#15803d' : '#b45309',
            fontWeight: 700,
            fontSize: '0.675rem',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: '5px',
            transition: 'all 0.15s ease',
          }}
        >
          <span>{isPaused ? '▶️' : '⏸️'}</span>
          <span>{isPaused ? 'Resume Polling' : 'Pause Polling'}</span>
        </button>
      </div>

      {/* Output Port on Right */}
      <Handle
        type="source"
        position={Position.Right}
        id="handler-action"
        style={{
          background: '#10b981',
          width: 9,
          height: 9,
          border: '2px solid #ffffff',
        }}
      />
    </div>
  );
};

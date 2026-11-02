import React from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { SIMULATION_LIMITS } from '../../domain/limits';
import type { SimKafkaBrokerData } from '../../domain/types';

interface SimKafkaBrokerNodeProps extends NodeProps {
  data: SimKafkaBrokerData;
}

export const SimKafkaBrokerNode: React.FC<SimKafkaBrokerNodeProps> = ({ data, selected }) => {
  const brokerId = data.brokerId ?? 1;
  const isCrashed = data.status === 'CRASHED';
  const isController = Boolean(data.isController);
  const topicPartitions = data.topicPartitions || [];
  const shown = topicPartitions.slice(0, SIMULATION_LIMITS.partitionRowsPerNode);
  const multipleTopics = new Set(topicPartitions.map((partition) => partition.topic)).size > 1;

  const handleToggleCrash = (e: React.MouseEvent) => {
    e.stopPropagation();
    window.dispatchEvent(
      new CustomEvent('sim:toggle-broker-crash', {
        detail: { brokerId },
      }),
    );
  };

  return (
    <div
      style={{
        background: isCrashed ? '#fef2f2' : '#ffffff',
        border: selected
          ? '2px solid #0284c7'
          : isCrashed
            ? '2px solid #ef4444'
            : isController
              ? '2px solid #3b82f6'
              : '1px solid #cbd5e1',
        borderRadius: '12px',
        padding: '12px 14px',
        width: '240px',
        boxShadow: selected
          ? '0 0 0 3px rgba(2, 132, 199, 0.25), 0 4px 12px rgba(0, 0, 0, 0.08)'
          : isCrashed
            ? '0 2px 8px rgba(239, 68, 68, 0.15)'
            : '0 2px 6px rgba(0, 0, 0, 0.05)',
        fontFamily: 'var(--font-sans)',
        cursor: 'pointer',
        transition: 'all 0.15s ease',
        position: 'relative',
        opacity: isCrashed ? 0.85 : 1,
      }}
    >
      {/* Input Port on Left (Publish from Producer or CDC) */}
      <Handle
        type="target"
        position={Position.Left}
        id="publish"
        style={{
          background: isCrashed ? '#94a3b8' : '#38bdf8',
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
          borderBottom: isCrashed ? '1px solid #fecaca' : '1px solid #f1f5f9',
          paddingBottom: '8px',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '1.25rem' }}>{isCrashed ? '💥' : '🖥️'}</span>
          <div>
            <div style={{ fontSize: '0.85rem', fontWeight: 800, color: '#0f172a' }}>
              Broker B{brokerId}
            </div>
            <div
              style={{
                fontSize: '0.675rem',
                color: isCrashed ? '#dc2626' : '#64748b',
                fontFamily: 'var(--font-mono)',
              }}
            >
              {data.host || `broker-${brokerId}:9092`}
            </div>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
          {isController && !isCrashed && (
            <span
              style={{
                fontSize: '0.6rem',
                fontWeight: 800,
                padding: '2px 5px',
                borderRadius: '4px',
                background: '#eff6ff',
                color: '#1d4ed8',
                border: '1px solid #bfdbfe',
              }}
              title="KRaft Active Controller"
            >
              CTRL
            </span>
          )}
          <span
            style={{
              fontSize: '0.625rem',
              fontWeight: 800,
              padding: '2px 6px',
              borderRadius: '4px',
              background: isCrashed ? '#fee2e2' : '#ecfdf5',
              color: isCrashed ? '#b91c1c' : '#059669',
              border: isCrashed ? '1px solid #fca5a5' : '1px solid #a7f3d0',
            }}
          >
            {isCrashed ? 'CRASHED' : 'ONLINE'}
          </span>
        </div>
      </div>

      {/* Partition Replicas hosted on this Broker */}
      <div style={{ marginBottom: '10px' }}>
        <div
          style={{
            fontSize: '0.65rem',
            fontWeight: 700,
            color: '#475569',
            marginBottom: '4px',
            display: 'flex',
            justifyContent: 'space-between',
          }}
        >
          <span>HOSTED REPLICAS:</span>
          <span>{topicPartitions.length} assigned</span>
        </div>

        {topicPartitions.length === 0 ? (
          <div
            style={{
              fontSize: '0.65rem',
              color: '#94a3b8',
              fontStyle: 'italic',
              background: '#f8fafc',
              padding: '4px 6px',
              borderRadius: '4px',
            }}
          >
            No active partition replicas
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
            {shown.map((tp) => (
              <div
                key={`${tp.topic}-${tp.partitionId}`}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  padding: '3px 6px',
                  borderRadius: '4px',
                  background: isCrashed ? '#fef2f2' : tp.isLeader ? '#eff6ff' : '#f8fafc',
                  border: tp.isLeader ? '1px solid #bfdbfe' : '1px solid #e2e8f0',
                  fontSize: '0.65rem',
                  fontFamily: 'var(--font-mono)',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                  <span
                    style={{
                      fontWeight: 800,
                      color: tp.isLeader ? '#1d4ed8' : '#334155',
                    }}
                  >
                    {multipleTopics ? `${tp.topic}:P${tp.partitionId}` : `P${tp.partitionId}`}
                  </span>
                  <span
                    style={{
                      fontSize: '0.575rem',
                      fontWeight: 700,
                      padding: '1px 4px',
                      borderRadius: '3px',
                      background: tp.isLeader ? '#dbeafe' : '#e2e8f0',
                      color: tp.isLeader ? '#1e40af' : '#475569',
                    }}
                  >
                    {tp.isLeader ? 'LEADER' : 'FOLLOWER'}
                  </span>
                </div>

                <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                  <span
                    style={{
                      fontSize: '0.575rem',
                      color: tp.isInIsr ? '#059669' : '#dc2626',
                      fontWeight: 700,
                    }}
                    title={tp.isInIsr ? 'In-Sync Replica' : 'Out-of-Sync Replica'}
                  >
                    {tp.isInIsr ? '✓ ISR' : '✗ OUT'}
                  </span>
                </div>
              </div>
            ))}
            {topicPartitions.length > shown.length && (
              <small>
                +{topicPartitions.length - shown.length} replicas · select broker to inspect all
              </small>
            )}
          </div>
        )}
      </div>

      {/* Quick Destroy / Revive Button */}
      <button
        type="button"
        onClick={handleToggleCrash}
        style={{
          width: '100%',
          padding: '5px 8px',
          borderRadius: '6px',
          border: isCrashed ? '1px solid #86efac' : '1px solid #fca5a5',
          background: isCrashed
            ? 'linear-gradient(135deg, #10b981 0%, #059669 100%)'
            : 'linear-gradient(135deg, #fee2e2 0%, #fecaca 100%)',
          color: isCrashed ? '#ffffff' : '#991b1b',
          fontSize: '0.7rem',
          fontWeight: 800,
          cursor: 'pointer',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          gap: '6px',
          boxShadow: isCrashed ? '0 1px 3px rgba(16, 185, 129, 0.2)' : 'none',
        }}
      >
        <span>{isCrashed ? '🩺 Revive Broker' : 'Crash broker'}</span>
      </button>

      {/* Output Port on Right (Dispatch to Consumers) */}
      <Handle
        type="source"
        position={Position.Right}
        id="dispatch"
        style={{
          background: isCrashed ? '#94a3b8' : '#fbbf24',
          width: 9,
          height: 9,
          border: '2px solid #ffffff',
        }}
      />
    </div>
  );
};

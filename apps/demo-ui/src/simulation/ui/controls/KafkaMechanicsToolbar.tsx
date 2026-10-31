import React from 'react';
import type { KafkaMechanicsConfig, SimProducerAcks } from '../../domain/types';

interface KafkaMechanicsToolbarProps {
  config: KafkaMechanicsConfig;
  onSetAcks: (acks: SimProducerAcks) => void;
  onSetPartitioning: (strategy: 'murmur2-key' | 'round-robin') => void;
  onToggleIdempotency: () => void;
  onToggleDlq: () => void;
  onDispatchDuplicate: () => void;
  onDispatchOrderedBurst: (count: number) => void;
  onDispatchPoisonPill: () => void;
  offsetMenu?: React.ReactNode;
}

export const KafkaMechanicsToolbar: React.FC<KafkaMechanicsToolbarProps> = ({
  config,
  onSetAcks,
  onSetPartitioning,
  onToggleIdempotency,
  onToggleDlq,
  onDispatchDuplicate,
  onDispatchOrderedBurst,
  onDispatchPoisonPill,
  offsetMenu,
}) => {
  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '8px 16px',
        background: '#f8fafc',
        borderBottom: '1px solid #e2e8f0',
        fontSize: '0.75rem',
        flexWrap: 'wrap',
        gap: '12px',
      }}
    >
      {/* Group 1: Producer Acks & Partitioner */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' }}>
        {/* Acks Selector */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <span style={{ fontWeight: 700, color: '#475569' }}>Producer ACKs:</span>
          <div
            style={{
              display: 'inline-flex',
              background: '#e2e8f0',
              borderRadius: '6px',
              padding: '2px',
              gap: '2px',
            }}
          >
            {[
              { val: '0' as const, label: 'acks=0', title: 'Fire & Forget: No broker ACK' },
              { val: '1' as const, label: 'acks=1', title: 'Leader ACK: Waits for partition leader' },
              {
                val: 'all' as const,
                label: 'acks=all',
                title: 'Full ISR: Waits for all In-Sync Replicas',
              },
            ].map((item) => {
              const active = config.producerAcks === item.val;
              return (
                <button
                  key={item.label}
                  onClick={() => onSetAcks(item.val)}
                  title={item.title}
                  style={{
                    padding: '3px 8px',
                    borderRadius: '4px',
                    border: 'none',
                    background: active ? '#ffffff' : 'transparent',
                    color: active ? '#0f172a' : '#64748b',
                    fontWeight: active ? 800 : 600,
                    fontSize: '0.7rem',
                    cursor: 'pointer',
                    boxShadow: active ? '0 1px 2px rgba(0,0,0,0.08)' : 'none',
                    transition: 'all 0.15s ease',
                  }}
                >
                  {item.label}
                </button>
              );
            })}
          </div>
        </div>

        {/* Partitioning Strategy */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <span style={{ fontWeight: 700, color: '#475569' }}>Partitioning:</span>
          <div
            style={{
              display: 'inline-flex',
              background: '#e2e8f0',
              borderRadius: '6px',
              padding: '2px',
              gap: '2px',
            }}
          >
            <button
              onClick={() => onSetPartitioning('murmur2-key')}
              title="Murmur2 Key Hashing: Guarantees strict per-customer FIFO ordering into single partition"
              style={{
                padding: '3px 8px',
                borderRadius: '4px',
                border: 'none',
                background: config.partitioningStrategy === 'murmur2-key' ? '#0284c7' : 'transparent',
                color: config.partitioningStrategy === 'murmur2-key' ? '#ffffff' : '#64748b',
                fontWeight: config.partitioningStrategy === 'murmur2-key' ? 800 : 600,
                fontSize: '0.7rem',
                cursor: 'pointer',
                transition: 'all 0.15s ease',
              }}
            >
              🔑 Murmur2 Key (FIFO)
            </button>
            <button
              onClick={() => onSetPartitioning('round-robin')}
              title="Round Robin: Scatters messages across partitions uniformly, breaking per-entity FIFO ordering!"
              style={{
                padding: '3px 8px',
                borderRadius: '4px',
                border: 'none',
                background: config.partitioningStrategy === 'round-robin' ? '#e11d48' : 'transparent',
                color: config.partitioningStrategy === 'round-robin' ? '#ffffff' : '#64748b',
                fontWeight: config.partitioningStrategy === 'round-robin' ? 800 : 600,
                fontSize: '0.7rem',
                cursor: 'pointer',
                transition: 'all 0.15s ease',
              }}
            >
              🎲 Round-Robin (No Key)
            </button>
          </div>
        </div>
      </div>

      {/* Group 2: Consumer Mechanics Toggles */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '12px', flexWrap: 'wrap' }}>
        {/* Idempotency Toggle */}
        <button
          onClick={onToggleIdempotency}
          title={
            config.consumerIdempotency
              ? 'Idempotency ON: Consumer deduplicates using processed_events table, skipping duplicate DB mutations.'
              : 'Idempotency OFF: Consumer blindly processes duplicate messages, leading to phantom duplicate mutations!'
          }
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            padding: '4px 10px',
            borderRadius: '6px',
            border: config.consumerIdempotency ? '1px solid #10b981' : '1px solid #f87171',
            background: config.consumerIdempotency ? '#ecfdf5' : '#fef2f2',
            color: config.consumerIdempotency ? '#065f46' : '#991b1b',
            fontWeight: 700,
            fontSize: '0.7rem',
            cursor: 'pointer',
          }}
        >
          <span>{config.consumerIdempotency ? '🛡️' : '🚨'}</span>
          <span>Idempotency: {config.consumerIdempotency ? 'ENABLED' : 'DISABLED'}</span>
        </button>

        {/* DLQ Routing Toggle */}
        <button
          onClick={onToggleDlq}
          title={
            config.dlqRoutingEnabled
              ? 'DLQ ON: Poison pill messages are redirected to Dead-Letter Queue to avoid stalling consumer partitions.'
              : 'DLQ OFF: Poison pills crash the consumer and cause Head-of-Line blocking (partition stalls indefinitely).'
          }
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            padding: '4px 10px',
            borderRadius: '6px',
            border: config.dlqRoutingEnabled ? '1px solid #3b82f6' : '1px solid #eab308',
            background: config.dlqRoutingEnabled ? '#eff6ff' : '#fefce8',
            color: config.dlqRoutingEnabled ? '#1e40af' : '#854d0e',
            fontWeight: 700,
            fontSize: '0.7rem',
            cursor: 'pointer',
          }}
        >
          <span>{config.dlqRoutingEnabled ? '📬' : '⛔'}</span>
          <span>DLQ Routing: {config.dlqRoutingEnabled ? 'ENABLED' : 'HEAD-OF-LINE'}</span>
        </button>

        {offsetMenu}
      </div>

      {/* Group 3: Experiment & Stress Test Triggers */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        <button
          onClick={() => onDispatchOrderedBurst(5)}
          title="Send a rapid burst of 5 ordered messages for customer U001 to demonstrate partition FIFO guarantees"
          style={{
            padding: '4px 10px',
            borderRadius: '6px',
            border: '1px solid #6366f1',
            background: '#eef2ff',
            color: '#4338ca',
            fontWeight: 700,
            fontSize: '0.7rem',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: '4px',
          }}
        >
          <span>⚡</span>
          <span>Burst 5 FIFO (U001)</span>
        </button>

        <button
          onClick={onDispatchDuplicate}
          title="Send duplicate event to test consumer idempotency filter vs duplicate DB write hazard"
          style={{
            padding: '4px 10px',
            borderRadius: '6px',
            border: '1px solid #f59e0b',
            background: '#fffbeb',
            color: '#b45309',
            fontWeight: 700,
            fontSize: '0.7rem',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: '4px',
          }}
        >
          <span>🔁</span>
          <span>Send Duplicate</span>
        </button>

        <button
          onClick={onDispatchPoisonPill}
          title="Dispatch corrupt poison-pill message to test DLQ routing vs Head-of-Line blocking"
          style={{
            padding: '4px 10px',
            borderRadius: '6px',
            border: '1px solid #ef4444',
            background: '#fef2f2',
            color: '#dc2626',
            fontWeight: 700,
            fontSize: '0.7rem',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: '4px',
          }}
        >
          <span>☠️</span>
          <span>Poison Pill</span>
        </button>
      </div>
    </div>
  );
};

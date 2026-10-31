import React from 'react';
import type { SimCustomNode } from '../../../domain/types';

interface ConsumerInspectorSectionProps {
  selectedNode: SimCustomNode;
  onUpdateData: (nodeId: string, partial: Record<string, unknown>) => void;
  onCrashConsumer?: (consumerId: string) => void;
  onReviveConsumer?: (consumerId: string) => void;
  onTogglePauseConsumer?: (consumerId: string) => void;
  onTogglePauseConsumerPartition?: (consumerId: string, partitionId: number) => void;
  onSetAssignorStrategy?: (strategy: 'round-robin' | 'range') => void;
  assignorStrategy?: 'round-robin' | 'range';
}

export const ConsumerInspectorSection: React.FC<ConsumerInspectorSectionProps> = ({
  selectedNode,
  onUpdateData,
  onCrashConsumer,
  onReviveConsumer,
  onTogglePauseConsumer,
  onTogglePauseConsumerPartition,
  onSetAssignorStrategy,
  assignorStrategy = 'round-robin',
}) => {
  const { id, data } = selectedNode;

  return (
    <>
      {/* Quick Polling & Backlog Controls */}
      <div
        style={{
          background: '#fffbeb',
          border: '1px solid #fde68a',
          borderRadius: '8px',
          padding: '10px',
        }}
      >
        <div
          style={{
            fontSize: '0.725rem',
            fontWeight: 800,
            color: '#92400e',
            marginBottom: '6px',
          }}
        >
          CONSUMER POLLING &amp; BACKLOG ACCUMULATION:
        </div>

        <button
          onClick={() => onTogglePauseConsumer?.(id)}
          style={{
            width: '100%',
            padding: '7px 10px',
            borderRadius: '6px',
            border: data.isPaused ? '1px solid #16a34a' : '1px solid #d97706',
            background: data.isPaused ? '#16a34a' : '#d97706',
            color: '#ffffff',
            fontWeight: 800,
            fontSize: '0.75rem',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: '6px',
            marginBottom: '8px',
          }}
        >
          <span>{data.isPaused ? '▶️' : '⏸️'}</span>
          <span>
            {data.isPaused
              ? 'Resume Polling (Drain Lag)'
              : 'Pause Polling (Accumulate Backlog)'}
          </span>
        </button>

        {/* Per-Partition Wire Controls */}
        <div style={{ marginTop: '6px' }}>
          <span style={{ fontSize: '0.65rem', fontWeight: 700, color: '#78350f' }}>
            Partition Polling Wires (Cut to stop individual partition):
          </span>

          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              gap: '4px',
              marginTop: '4px',
            }}
          >
            {((data.assignedPartitions as number[]) || []).map((p) => {
              const isCut = ((data.pausedPartitions as number[]) || []).includes(p);

              return (
                <div
                  key={p}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    padding: '4px 8px',
                    borderRadius: '4px',
                    background: isCut ? '#fee2e2' : '#ffffff',
                    border: isCut ? '1px solid #f87171' : '1px solid #e2e8f0',
                    fontSize: '0.7rem',
                    fontFamily: 'var(--font-mono)',
                  }}
                >
                  <span style={{ fontWeight: 800, color: isCut ? '#dc2626' : '#0f172a' }}>
                    P{p} {isCut ? '[✂️ CUT]' : '[🟢 POLLING]'}
                  </span>

                  <button
                    onClick={() => onTogglePauseConsumerPartition?.(id, p)}
                    style={{
                      padding: '2px 8px',
                      borderRadius: '4px',
                      border: isCut ? '1px solid #16a34a' : '1px solid #dc2626',
                      background: isCut ? '#f0fdf4' : '#fef2f2',
                      color: isCut ? '#16a34a' : '#dc2626',
                      fontSize: '0.625rem',
                      fontWeight: 700,
                      cursor: 'pointer',
                    }}
                  >
                    {isCut ? '🔌 Reconnect Wire' : '✂️ Cut Wire'}
                  </button>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      <div>
        <label
          style={{
            display: 'block',
            fontSize: '0.725rem',
            fontWeight: 700,
            color: '#475569',
            marginBottom: '4px',
          }}
        >
          Consumer Group ID:
        </label>
        <input
          type="text"
          value={(data.groupId as string) || 'loyalty-v1'}
          onChange={(e) => onUpdateData(id, { groupId: e.target.value })}
          style={{
            width: '100%',
            padding: '6px 10px',
            borderRadius: '6px',
            border: '1px solid #cbd5e1',
            fontSize: '0.75rem',
            fontFamily: 'var(--font-mono)',
            boxSizing: 'border-box',
          }}
        />

        <div style={{ display: 'flex', gap: '4px', marginTop: '6px', flexWrap: 'wrap' }}>
          <span style={{ fontSize: '0.625rem', color: '#64748b', alignSelf: 'center' }}>
            Presets:
          </span>
          {[
            { title: 'Loyalty Worker', group: 'loyalty-v1', icon: '👥' },
            { title: 'Analytics Consumer', group: 'analytics-v1', icon: '📈' },
            { title: 'Fraud Detector', group: 'fraud-detection', icon: '🛡️' },
            { title: 'Notification Service', group: 'notifications', icon: '🔔' },
          ].map((p) => (
            <button
              key={p.title}
              onClick={() =>
                onUpdateData(id, { title: p.title, groupId: p.group, icon: p.icon })
              }
              style={{
                padding: '3px 7px',
                borderRadius: '4px',
                border:
                  (data.groupId as string) === p.group
                    ? '1px solid #7c3aed'
                    : '1px solid #cbd5e1',
                background:
                  (data.groupId as string) === p.group ? '#f5f3ff' : '#ffffff',
                color: (data.groupId as string) === p.group ? '#7c3aed' : '#475569',
                fontSize: '0.625rem',
                fontWeight: 600,
                cursor: 'pointer',
              }}
            >
              {p.icon} {p.title}
            </button>
          ))}
        </div>
      </div>

      {/* Lifecycle & Fault Injection */}
      <div
        style={{
          background: '#fff1f2',
          border: '1px solid #fecdd3',
          borderRadius: '6px',
          padding: '10px',
        }}
      >
        <div
          style={{
            fontSize: '0.675rem',
            fontWeight: 800,
            color: '#be123c',
            marginBottom: '6px',
          }}
        >
          INSTANCE FAULT INJECTION:
        </div>
        {data.status === 'CRASHED' ? (
          <div>
            <div style={{ fontSize: '0.7rem', color: '#9f1239', marginBottom: '8px' }}>
              ⚠️ Consumer instance is currently CRASHED.
            </div>
            <button
              onClick={() => onReviveConsumer?.(id)}
              style={{
                width: '100%',
                padding: '6px 12px',
                borderRadius: '6px',
                border: '1px solid #86efac',
                background: '#16a34a',
                color: '#ffffff',
                fontWeight: 700,
                fontSize: '0.725rem',
                cursor: 'pointer',
              }}
            >
              🩺 Revive Consumer (Rejoin Group)
            </button>
          </div>
        ) : (
          <div>
            <button
              onClick={() => onCrashConsumer?.(id)}
              style={{
                width: '100%',
                padding: '6px 12px',
                borderRadius: '6px',
                border: '1px solid #f87171',
                background: '#e11d48',
                color: '#ffffff',
                fontWeight: 700,
                fontSize: '0.725rem',
                cursor: 'pointer',
              }}
            >
              💥 Crash Consumer (Trigger Fleet Rebalance)
            </button>
          </div>
        )}
      </div>

      {/* Assignor Strategy Toggle */}
      <div>
        <label
          style={{
            display: 'block',
            fontSize: '0.725rem',
            fontWeight: 700,
            color: '#475569',
            marginBottom: '4px',
          }}
        >
          Partition Assignor Strategy:
        </label>
        <div style={{ display: 'flex', gap: '6px' }}>
          <button
            onClick={() => onSetAssignorStrategy?.('round-robin')}
            style={{
              flex: 1,
              padding: '6px 8px',
              borderRadius: '6px',
              border:
                assignorStrategy === 'round-robin'
                  ? '1px solid #7c3aed'
                  : '1px solid #cbd5e1',
              background: assignorStrategy === 'round-robin' ? '#f5f3ff' : '#ffffff',
              color: assignorStrategy === 'round-robin' ? '#7c3aed' : '#475569',
              fontWeight: 700,
              fontSize: '0.725rem',
              cursor: 'pointer',
            }}
          >
            Round-Robin
          </button>
          <button
            onClick={() => onSetAssignorStrategy?.('range')}
            style={{
              flex: 1,
              padding: '6px 8px',
              borderRadius: '6px',
              border:
                assignorStrategy === 'range' ? '1px solid #7c3aed' : '1px solid #cbd5e1',
              background: assignorStrategy === 'range' ? '#f5f3ff' : '#ffffff',
              color: assignorStrategy === 'range' ? '#7c3aed' : '#475569',
              fontWeight: 700,
              fontSize: '0.725rem',
              cursor: 'pointer',
            }}
          >
            Range
          </button>
        </div>
      </div>

      {/* Processing Delay */}
      <div>
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            marginBottom: '4px',
          }}
        >
          <label style={{ fontSize: '0.725rem', fontWeight: 700, color: '#475569' }}>
            Processing Delay ({(data.processingDelayMs as number) || 120}ms):
          </label>
        </div>
        <input
          type="range"
          min="20"
          max="1000"
          step="20"
          value={(data.processingDelayMs as number) || 120}
          onChange={(e) => onUpdateData(id, { processingDelayMs: Number(e.target.value) })}
          style={{ width: '100%', marginBottom: '6px' }}
        />
      </div>
    </>
  );
};

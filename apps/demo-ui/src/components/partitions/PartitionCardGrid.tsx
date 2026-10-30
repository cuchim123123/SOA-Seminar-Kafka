import React from 'react';
import type { PartitionInfo } from '../../api/client';

interface PartitionCardGridProps {
  partitions: PartitionInfo[];
  partitionCount: number;
  loading: boolean;
  activeHighlight: number | null | undefined;
  selectedPartition: number;
  onSelectPartition: (partitionId: number) => void;
}

export const PartitionCardGrid: React.FC<PartitionCardGridProps> = ({
  partitions,
  partitionCount,
  loading,
  activeHighlight,
  selectedPartition,
  onSelectPartition,
}) => {
  return (
    <section className="cyber-card" style={{ padding: '20px 24px' }}>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: '14px',
          flexWrap: 'wrap',
          gap: '8px',
        }}
      >
        <div>
          <h3 style={{ fontSize: '0.95rem', fontWeight: 700, color: 'var(--text-main)' }}>
            Partition Matrix ({partitionCount} Partitions)
          </h3>
          <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
            Click any partition card below to inspect its live log offsets and stream messages.
          </span>
        </div>

        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
            fontSize: '0.75rem',
            fontFamily: 'var(--font-mono)',
          }}
        >
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
            <span
              style={{
                width: '10px',
                height: '10px',
                borderRadius: '2px',
                background: '#0284c7',
                display: 'inline-block',
              }}
            />
            Active Target
          </span>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
            <span
              style={{
                width: '10px',
                height: '10px',
                borderRadius: '2px',
                background: '#e0f2fe',
                border: '1px solid #0284c7',
                display: 'inline-block',
              }}
            />
            Selected for Peek
          </span>
        </div>
      </div>

      {loading && partitions.length === 0 ? (
        <div
          style={{
            padding: '28px',
            textAlign: 'center',
            color: 'var(--text-dim)',
            fontFamily: 'var(--font-mono)',
            fontSize: '0.85rem',
          }}
        >
          Reading broker partition log end offsets...
        </div>
      ) : (
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(190px, 1fr))',
            gap: '12px',
          }}
        >
          {partitions.map((p) => {
            const isTarget = activeHighlight === p.partition;
            const isSelected = selectedPartition === p.partition;
            const msgCount = BigInt(p.messageCount || '0');
            const highWatermark = BigInt(p.highWatermark || '0');

            return (
              <div
                key={p.partition}
                onClick={() => onSelectPartition(p.partition)}
                style={{
                  padding: '12px 14px',
                  borderRadius: 'var(--radius-md)',
                  background: isSelected ? '#f0f9ff' : '#ffffff',
                  border: isTarget
                    ? '2px solid #0284c7'
                    : isSelected
                      ? '1px solid #0284c7'
                      : '1px solid var(--border-subtle)',
                  boxShadow: isTarget
                    ? '0 0 0 3px rgba(2, 132, 199, 0.15)'
                    : 'var(--shadow-sm)',
                  cursor: 'pointer',
                  transition: 'all 0.15s ease',
                }}
              >
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    marginBottom: '8px',
                  }}
                >
                  <span
                    className={`badge-mono ${isSelected ? 'badge-cyan' : 'badge-violet'}`}
                    style={{ fontSize: '0.8rem', fontWeight: 700 }}
                  >
                    P #{p.partition}
                  </span>
                  {isTarget && (
                    <span
                      style={{
                        fontSize: '0.675rem',
                        background: '#0284c7',
                        color: '#ffffff',
                        padding: '1px 6px',
                        borderRadius: '3px',
                        fontWeight: 700,
                        textTransform: 'uppercase',
                        letterSpacing: '0.5px',
                      }}
                    >
                      Target
                    </span>
                  )}
                </div>

                <div
                  style={{
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '4px',
                    fontSize: '0.75rem',
                    fontFamily: 'var(--font-mono)',
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Messages:</span>
                    <span
                      style={{
                        color: msgCount > 0n ? '#0284c7' : 'var(--text-dim)',
                        fontWeight: 700,
                      }}
                    >
                      {msgCount.toString()}
                    </span>
                  </div>

                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Watermark:</span>
                    <span style={{ color: 'var(--text-main)', fontWeight: 600 }}>
                      {highWatermark.toString()}
                    </span>
                  </div>

                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Log Start:</span>
                    <span style={{ color: 'var(--text-dim)' }}>{p.logStartOffset}</span>
                  </div>

                  <div
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      borderTop: '1px solid var(--border-subtle)',
                      paddingTop: '4px',
                      marginTop: '2px',
                      fontSize: '0.7rem',
                    }}
                  >
                    <span style={{ color: 'var(--text-dim)' }}>Leader: Broker #{p.leader}</span>
                    <span style={{ color: 'var(--text-dim)' }}>
                      ISR: [{p.isr?.join(',') || p.leader}]
                    </span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
};

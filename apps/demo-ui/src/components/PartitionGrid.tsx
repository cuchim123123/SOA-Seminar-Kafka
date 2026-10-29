import React from 'react';
import type { TopicPartitionMetadata } from '../api/client';

interface PartitionGridProps {
  metadata: TopicPartitionMetadata | null;
  loading: boolean;
  highlightedPartition?: number | null;
}

export const PartitionGrid: React.FC<PartitionGridProps> = ({
  metadata,
  loading,
  highlightedPartition,
}) => {
  const partitions = metadata?.partitions || [];

  return (
    <section className="cyber-card" style={{ padding: '20px 24px', marginBottom: '20px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '8px' }}>
        <div>
          <h2 style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ color: 'var(--color-cyan)' }}>❖</span>
            Kafka 6-Partition Topic Matrix (<span className="badge-mono badge-cyan">{metadata?.topic || 'purchase.events'}</span>)
          </h2>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem', marginTop: '2px' }}>
            Direct observations from Kafka Admin API. Offsets are strictly scoped to (topic, partition) and monotonically incrementing.
          </p>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span className="badge-mono badge-violet">Total Partitions: {metadata?.partitionCount || 6}</span>
          <span className="badge-mono badge-emerald">Leader Broker: ID 1</span>
        </div>
      </div>

      {loading && !metadata ? (
        <div style={{ padding: '28px', textAlign: 'center', color: 'var(--text-dim)', fontFamily: 'var(--font-mono)', fontSize: '0.85rem' }}>
          Reading broker partition log end offsets...
        </div>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '12px' }}>
          {partitions.map((p) => {
            const isHighlighted = highlightedPartition === p.partition;
            const msgCount = BigInt(p.messageCount || '0');
            const startOffset = BigInt(p.logStartOffset || '0');
            const highWatermark = BigInt(p.highWatermark || '0');

            return (
              <div
                key={p.partition}
                style={{
                  padding: '14px',
                  borderRadius: 'var(--radius-md)',
                  background: isHighlighted ? '#f0f9ff' : '#ffffff',
                  border: isHighlighted ? '1px solid #0284c7' : '1px solid var(--border-subtle)',
                  boxShadow: 'var(--shadow-sm)',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
                  <span className="badge-mono badge-cyan" style={{ fontSize: '0.75rem' }}>
                    Partition #{p.partition}
                  </span>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', fontFamily: 'var(--font-mono)' }}>
                    Leader {p.leader}
                  </span>
                </div>

                {/* Offset Metrics */}
                <div style={{ display: 'flex', flexDirection: 'column', gap: '5px', fontSize: '0.775rem', fontFamily: 'var(--font-mono)' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Log Start:</span>
                    <span style={{ color: startOffset > 0n ? 'var(--color-amber)' : 'var(--text-body)', fontWeight: 600 }}>
                      {p.logStartOffset}
                    </span>
                  </div>

                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>High-Watermark:</span>
                    <span style={{ color: 'var(--color-emerald)', fontWeight: 600 }}>
                      {p.highWatermark}
                    </span>
                  </div>

                  <div style={{ display: 'flex', justifyContent: 'space-between', borderTop: '1px solid var(--border-subtle)', paddingTop: '5px', marginTop: '2px' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Messages:</span>
                    <span style={{ color: 'var(--color-violet)', fontWeight: 700 }}>
                      {msgCount.toString()}
                    </span>
                  </div>
                </div>

                {/* Visual Offset Bar */}
                <div
                  style={{
                    height: '5px',
                    width: '100%',
                    background: '#f1f5f9',
                    borderRadius: 'var(--radius-full)',
                    marginTop: '10px',
                    overflow: 'hidden',
                  }}
                >
                  <div
                    style={{
                      height: '100%',
                      width: `${Math.min(100, Math.max(6, Number(highWatermark % 100n)))}%`,
                      background: isHighlighted ? '#0284c7' : '#3b82f6',
                      borderRadius: 'var(--radius-full)',
                      transition: 'width 0.4s ease',
                    }}
                  />
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Mechanics Explanation Alert */}
      <div
        style={{
          marginTop: '14px',
          padding: '10px 14px',
          borderRadius: 'var(--radius-sm)',
          background: 'var(--color-violet-bg)',
          border: '1px solid var(--color-violet-border)',
          display: 'flex',
          alignItems: 'center',
          gap: '10px',
          fontSize: '0.8rem',
          color: '#475569',
        }}
      >
        <span style={{ fontSize: '1rem', color: 'var(--color-violet)' }}>ℹ</span>
        <span>
          <strong style={{ color: '#0f172a' }}>Key-Based Partitioning Invariant:</strong> Records for customer{' '}
          <code style={{ color: '#6d28d9', fontFamily: 'var(--font-mono)', fontWeight: 600 }}>U001</code> always map to the exact same partition by consistent hash of <code style={{ color: '#6d28d9', fontFamily: 'var(--font-mono)', fontWeight: 600 }}>customerId</code>. Within that partition, Kafka guarantees strictly ascending offsets and FIFO processing.
        </span>
      </div>
    </section>
  );
};

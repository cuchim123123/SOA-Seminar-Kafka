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
    <section className="cyber-card" style={{ padding: '24px', marginBottom: '24px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', flexWrap: 'wrap', gap: '8px' }}>
        <div>
          <h2 style={{ fontSize: '1.15rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ color: 'var(--color-cyan)' }}>❖</span>
            KAFKA 6-PARTITION TOPIC RACK (<span className="badge-mono badge-cyan">{metadata?.topic || 'purchase.events'}</span>)
          </h2>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem' }}>
            Direct observations from Kafka Admin API. Offsets are scoped to (topic, partition) and monotonically incrementing.
          </p>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span className="badge-mono badge-violet">Total Partitions: {metadata?.partitionCount || 6}</span>
          <span className="badge-mono badge-emerald">Broker Leader: ID 1</span>
        </div>
      </div>

      {loading && !metadata ? (
        <div style={{ padding: '32px', textAlign: 'center', color: 'var(--text-dim)', fontFamily: 'var(--font-mono)' }}>
          Reading broker partition log end offsets...
        </div>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '14px' }}>
          {partitions.map((p) => {
            const isHighlighted = highlightedPartition === p.partition;
            const msgCount = BigInt(p.messageCount || '0');
            const startOffset = BigInt(p.logStartOffset || '0');
            const highWatermark = BigInt(p.highWatermark || '0');

            return (
              <div
                key={p.partition}
                className="cyber-card"
                style={{
                  padding: '16px',
                  background: isHighlighted ? 'rgba(6, 182, 212, 0.12)' : 'var(--bg-card)',
                  borderColor: isHighlighted ? 'var(--color-cyan)' : 'var(--border-subtle)',
                  boxShadow: isHighlighted ? 'var(--shadow-neon-cyan)' : 'none',
                  transition: 'all 0.3s ease',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '10px' }}>
                  <span className="badge-mono badge-cyan" style={{ fontSize: '0.8rem' }}>
                    Partition #{p.partition}
                  </span>
                  <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)', fontFamily: 'var(--font-mono)' }}>
                    Leader {p.leader}
                  </span>
                </div>

                {/* Offset Metrics */}
                <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', fontSize: '0.8rem', fontFamily: 'var(--font-mono)' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-dim)' }}>Log Start:</span>
                    <span style={{ color: startOffset > 0n ? 'var(--color-amber)' : 'var(--text-main)', fontWeight: 600 }}>
                      {p.logStartOffset}
                    </span>
                  </div>

                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-dim)' }}>High-Watermark:</span>
                    <span style={{ color: 'var(--color-emerald)', fontWeight: 600 }}>
                      {p.highWatermark}
                    </span>
                  </div>

                  <div style={{ display: 'flex', justifyContent: 'space-between', borderTop: '1px solid var(--border-subtle)', paddingTop: '6px', marginTop: '2px' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Messages:</span>
                    <span style={{ color: 'var(--color-violet)', fontWeight: 700 }}>
                      {msgCount.toString()}
                    </span>
                  </div>
                </div>

                {/* Visual Offset Bar */}
                <div
                  style={{
                    height: '6px',
                    width: '100%',
                    background: 'rgba(255, 255, 255, 0.05)',
                    borderRadius: 'var(--radius-full)',
                    marginTop: '12px',
                    overflow: 'hidden',
                  }}
                >
                  <div
                    style={{
                      height: '100%',
                      width: `${Math.min(100, Math.max(8, Number(highWatermark % 100n)))}%`,
                      background: isHighlighted
                        ? 'linear-gradient(90deg, var(--color-cyan), var(--color-violet))'
                        : 'linear-gradient(90deg, #3b82f6, var(--color-cyan))',
                      borderRadius: 'var(--radius-full)',
                      transition: 'width 0.5s ease',
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
          marginTop: '16px',
          padding: '12px 16px',
          borderRadius: 'var(--radius-sm)',
          background: 'rgba(139, 92, 246, 0.08)',
          border: '1px solid rgba(139, 92, 246, 0.25)',
          display: 'flex',
          alignItems: 'center',
          gap: '12px',
          fontSize: '0.8rem',
          color: 'var(--text-muted)',
        }}
      >
        <span style={{ fontSize: '1.1rem', color: 'var(--color-violet)' }}>ℹ</span>
        <span>
          <strong style={{ color: 'var(--text-main)' }}>Key-Based Partitioning Invariant:</strong> Records for customer{' '}
          <code style={{ color: 'var(--color-cyan)', fontFamily: 'var(--font-mono)' }}>U001</code> always map to the exact same partition by consistent hash of <code style={{ color: 'var(--color-cyan)', fontFamily: 'var(--font-mono)' }}>customerId</code>. Within that partition, Kafka guarantees strictly ascending offsets and FIFO processing.
        </span>
      </div>
    </section>
  );
};

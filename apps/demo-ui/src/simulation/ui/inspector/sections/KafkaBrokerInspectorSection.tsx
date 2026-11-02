import React from 'react';
import type { SimCustomNode, SimKafkaBrokerData } from '../../../domain/types';
import type { SimPartition } from '../../../domain/kafka-cluster';

interface KafkaBrokerInspectorSectionProps {
  selectedNode: SimCustomNode;
  onUpdateData: (nodeId: string, partial: Record<string, unknown>) => void;
  onCrashBroker?: (brokerId: number) => void;
  onReviveBroker?: (brokerId: number) => void;
  onSetReplicationFactor?: (rf: number) => void;
  onSetMinIsr?: (minIsr: number) => void;
  partitionLogs?: SimPartition[];
  minInsyncReplicas?: number;
  replicationFactor?: number;
}

export const KafkaBrokerInspectorSection: React.FC<KafkaBrokerInspectorSectionProps> = ({
  selectedNode,
  onCrashBroker,
  onReviveBroker,
  onSetReplicationFactor,
  onSetMinIsr,
  minInsyncReplicas = 2,
  replicationFactor = 3,
}) => {
  const data = selectedNode.data as SimKafkaBrokerData;
  const brokerId = data.brokerId ?? 1;
  const isCrashed = data.status === 'CRASHED';
  const isController = Boolean(data.isController);
  const topicPartitions = data.topicPartitions || [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
      {/* Broker Identity & Failover Control */}
      <div
        style={{
          background: isCrashed ? '#fef2f2' : '#f0fdf4',
          border: isCrashed ? '1px solid #fecaca' : '1px solid #bbf7d0',
          borderRadius: '10px',
          padding: '12px',
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
          <div>
            <div
              style={{
                fontSize: '0.85rem',
                fontWeight: 800,
                color: isCrashed ? '#991b1b' : '#166534',
              }}
            >
              Kafka Broker Unit (B{brokerId})
            </div>
            <div style={{ fontSize: '0.675rem', color: '#64748b', fontFamily: 'var(--font-mono)' }}>
              {data.host || `broker-${brokerId}:9092`}
            </div>
          </div>
          <span
            style={{
              fontSize: '0.65rem',
              fontWeight: 800,
              padding: '2px 8px',
              borderRadius: '9999px',
              background: isCrashed ? '#fee2e2' : '#dcfce7',
              color: isCrashed ? '#b91c1c' : '#15803d',
              border: isCrashed ? '1px solid #fca5a5' : '1px solid #86efac',
            }}
          >
            {isCrashed ? 'OFFLINE (CRASHED)' : 'HEALTHY (ACTIVE)'}
          </span>
        </div>

        {isController && (
          <div
            style={{
              fontSize: '0.675rem',
              color: '#1d4ed8',
              background: '#eff6ff',
              border: '1px solid #bfdbfe',
              padding: '4px 8px',
              borderRadius: '6px',
              marginBottom: '10px',
              fontWeight: 700,
            }}
          >
            👑 KRaft Active Controller: Manages cluster metadata and partition leader elections.
          </div>
        )}

        {/* Failover / Destroy Button */}
        <button
          type="button"
          onClick={() => {
            if (isCrashed) {
              onReviveBroker?.(brokerId);
            } else {
              onCrashBroker?.(brokerId);
            }
          }}
          style={{
            width: '100%',
            padding: '8px',
            borderRadius: '6px',
            border: 'none',
            background: isCrashed
              ? 'linear-gradient(135deg, #10b981 0%, #059669 100%)'
              : 'linear-gradient(135deg, #ef4444 0%, #dc2626 100%)',
            color: '#ffffff',
            fontWeight: 800,
            fontSize: '0.75rem',
            cursor: 'pointer',
            boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
          }}
        >
          {isCrashed
            ? '🩺 Revive Broker (Re-sync to ISR)'
            : '💥 Destroy Broker (Trigger Leader Election)'}
        </button>
      </div>

      {/* In-Sync Replicas (ISR) & Quorum Setting */}
      <div
        style={{
          background: '#f8fafc',
          border: '1px solid #e2e8f0',
          borderRadius: '8px',
          padding: '10px',
          fontSize: '0.725rem',
          lineHeight: 1.4,
          color: '#475569',
        }}
      >
        <div
          style={{
            fontWeight: 800,
            color: '#1e293b',
            marginBottom: '8px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <span>🛡️ Replication Factor & ISR Controls:</span>
        </div>

        {/* Replication Factor Control */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            marginBottom: '8px',
          }}
        >
          <div>
            <span style={{ fontWeight: 700, color: '#334155' }}>Replication Factor:</span>
            <div style={{ fontSize: '0.625rem', color: '#64748b' }}>
              Copies per partition across brokers
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
            {[1, 2, 3].map((val) => (
              <button
                key={val}
                type="button"
                onClick={() => onSetReplicationFactor?.(val)}
                style={{
                  padding: '2px 8px',
                  borderRadius: '4px',
                  border: replicationFactor === val ? '1px solid #0284c7' : '1px solid #cbd5e1',
                  background: replicationFactor === val ? '#0284c7' : '#ffffff',
                  color: replicationFactor === val ? '#ffffff' : '#334155',
                  fontWeight: 800,
                  fontSize: '0.7rem',
                  cursor: 'pointer',
                }}
              >
                {val}
              </button>
            ))}
          </div>
        </div>

        {/* Min ISR Control */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            marginBottom: '6px',
          }}
        >
          <div>
            <span style={{ fontWeight: 700, color: '#334155' }}>
              Min. In-Sync Replicas (min.insync.replicas):
            </span>
            <div style={{ fontSize: '0.625rem', color: '#64748b' }}>
              Required ISR quorum for acks=all writes
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
            {[1, 2, 3].map((val) => (
              <button
                key={val}
                type="button"
                onClick={() => onSetMinIsr?.(val)}
                style={{
                  padding: '2px 8px',
                  borderRadius: '4px',
                  border: minInsyncReplicas === val ? '1px solid #10b981' : '1px solid #cbd5e1',
                  background: minInsyncReplicas === val ? '#10b981' : '#ffffff',
                  color: minInsyncReplicas === val ? '#ffffff' : '#334155',
                  fontWeight: 800,
                  fontSize: '0.7rem',
                  cursor: 'pointer',
                }}
              >
                {val}
              </button>
            ))}
          </div>
        </div>

        <div
          style={{
            fontSize: '0.65rem',
            color: '#64748b',
            marginTop: '4px',
            borderTop: '1px solid #e2e8f0',
            paddingTop: '6px',
          }}
        >
          If active ISR count drops below <strong>{minInsyncReplicas}</strong>,{' '}
          <code>acks=all</code> writes will be rejected with <code>NOT_ENOUGH_REPLICAS</code>.
        </div>
      </div>

      {/* Hosted Partition Replicas */}
      <div>
        <label
          style={{
            display: 'block',
            fontSize: '0.75rem',
            fontWeight: 800,
            color: '#334155',
            marginBottom: '6px',
          }}
        >
          Assigned Partition Replicas ({topicPartitions.length}):
        </label>
        {topicPartitions.length === 0 ? (
          <div
            style={{ fontSize: '0.7rem', color: '#94a3b8', fontStyle: 'italic', padding: '8px' }}
          >
            No partitions assigned to this broker.
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
            {topicPartitions.map((tp) => (
              <div
                key={`${tp.topic}-${tp.partitionId}`}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  padding: '8px 10px',
                  borderRadius: '6px',
                  background: isCrashed ? '#fef2f2' : tp.isLeader ? '#eff6ff' : '#ffffff',
                  border: tp.isLeader ? '1px solid #bfdbfe' : '1px solid #e2e8f0',
                  fontSize: '0.7rem',
                  fontFamily: 'var(--font-mono)',
                }}
              >
                <div>
                  <div style={{ fontWeight: 800, color: tp.isLeader ? '#1d4ed8' : '#334155' }}>
                    {tp.topic} : Partition P{tp.partitionId}
                  </div>
                  <div style={{ fontSize: '0.625rem', color: '#64748b' }}>
                    Replicas: [{tp.replicas.map((r) => `B${r}`).join(', ')}] • ISR: [
                    {tp.isr.map((r) => `B${r}`).join(', ')}]
                  </div>
                </div>

                <div
                  style={{
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'flex-end',
                    gap: '2px',
                  }}
                >
                  <span
                    style={{
                      fontSize: '0.6rem',
                      fontWeight: 800,
                      padding: '2px 6px',
                      borderRadius: '4px',
                      background: tp.isLeader ? '#dbeafe' : '#f1f5f9',
                      color: tp.isLeader ? '#1e40af' : '#475569',
                    }}
                  >
                    {tp.isLeader ? '★ LEADER' : 'FOLLOWER'}
                  </span>
                  <span
                    style={{
                      fontSize: '0.575rem',
                      fontWeight: 700,
                      color: tp.isInIsr ? '#059669' : '#dc2626',
                    }}
                  >
                    {tp.isInIsr ? '● In ISR' : '○ Out of ISR'}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

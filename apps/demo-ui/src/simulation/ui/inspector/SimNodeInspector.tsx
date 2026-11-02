import React from 'react';
import type { SimCustomNode } from '../../domain/types';
import type { SimPartition, SimBroker } from '../../domain/kafka-cluster';
import { ProducerInspectorSection } from './sections/ProducerInspectorSection';
import { KafkaClusterInspectorSection } from './sections/KafkaClusterInspectorSection';
import { KafkaBrokerInspectorSection } from './sections/KafkaBrokerInspectorSection';
import { ConsumerInspectorSection } from './sections/ConsumerInspectorSection';
import { OutboxCdcInspectorSection } from './sections/OutboxCdcInspectorSection';
import { DatabaseDlqInspectorSection } from './sections/DatabaseDlqInspectorSection';

interface SimNodeInspectorProps {
  selectedNode: SimCustomNode | null;
  onClose: () => void;
  onUpdateData: (nodeId: string, partial: Record<string, unknown>) => void;
  onDuplicateNode: (nodeId: string) => void;
  onDeleteNode: (nodeId: string) => void;
  onSetPartitionCount?: (count: number) => void;
  onSetReplicationFactor?: (rf: number) => void;
  onSetMinIsr?: (minIsr: number) => void;
  onSetAssignorStrategy?: (strategy: 'round-robin' | 'range') => void;
  onCrashBroker?: (brokerId: number) => void;
  onReviveBroker?: (brokerId: number) => void;
  onCrashConsumer?: (consumerId: string) => void;
  onReviveConsumer?: (consumerId: string) => void;
  onTogglePauseConsumer?: (consumerId: string) => void;
  onTogglePauseConsumerPartition?: (consumerId: string, partitionId: number) => void;
  onCreateClusterTopic?: (
    clusterNodeId: string,
    topicName: string,
    partitionCount: number,
    replicationFactor: number,
  ) => void;
  onSetActiveClusterTopic?: (clusterNodeId: string, topicName: string) => void;
  brokers?: SimBroker[];
  assignorStrategy?: 'round-robin' | 'range';
  partitionLogs?: SimPartition[];
  committedOffsets?: Map<string, number>;
  consumerGroups?: {
    groupId: string;
    assignorStrategy: 'round-robin' | 'range';
    members: Map<string, { id: string; assignedPartitions: number[] }>;
    committedNextOffsets: Map<string, number>;
  }[];
  allTopics?: {
    name: string;
    partitions: Map<number, unknown>;
    replicationFactor: number;
    minInsyncReplicas?: number;
  }[];
  minInsyncReplicas?: number;
  replicationFactor?: number;
}

export const SimNodeInspector: React.FC<SimNodeInspectorProps> = ({
  selectedNode,
  onClose,
  onUpdateData,
  onDuplicateNode,
  onDeleteNode,
  onSetPartitionCount,
  onSetReplicationFactor,
  onSetMinIsr,
  onSetAssignorStrategy,
  onCrashBroker,
  onReviveBroker,
  onCrashConsumer,
  onReviveConsumer,
  onTogglePauseConsumer,
  onTogglePauseConsumerPartition,
  onCreateClusterTopic,
  onSetActiveClusterTopic,
  brokers = [],
  assignorStrategy = 'round-robin',
  partitionLogs = [],
  committedOffsets = new Map(),
  consumerGroups = [],
  allTopics = [],
  minInsyncReplicas = 2,
  replicationFactor = 3,
}) => {
  if (!selectedNode) return null;

  const { id, data } = selectedNode;
  const kind = data.kind;

  return (
    <div
      style={{
        position: 'absolute',
        top: '16px',
        right: '16px',
        width: '380px',
        background: '#ffffff',
        border: '1px solid #cbd5e1',
        borderRadius: '12px',
        boxShadow: '0 8px 24px rgba(0, 0, 0, 0.12)',
        zIndex: 20,
        display: 'flex',
        flexDirection: 'column',
        maxHeight: 'calc(100% - 32px)',
        overflow: 'hidden',
        fontFamily: 'var(--font-sans)',
      }}
    >
      {/* Header */}
      <div
        style={{
          padding: '12px 16px',
          borderBottom: '1px solid #e2e8f0',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          background: '#f8fafc',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '1.25rem' }}>{data.icon || '⚙️'}</span>
          <div>
            <div style={{ fontSize: '0.875rem', fontWeight: 800, color: '#0f172a' }}>
              {data.title || 'Component Inspector'}
            </div>
            <div
              style={{
                fontSize: '0.675rem',
                color: '#64748b',
                fontFamily: 'var(--font-mono)',
              }}
            >
              ID: {id}
            </div>
          </div>
        </div>

        <button
          onClick={onClose}
          style={{
            border: 'none',
            background: 'none',
            fontSize: '1rem',
            cursor: 'pointer',
            color: '#64748b',
          }}
        >
          ✕
        </button>
      </div>

      {/* Body: Component-Specific Sections */}
      <div
        style={{
          padding: '16px',
          flex: 1,
          overflowY: 'auto',
          display: 'flex',
          flexDirection: 'column',
          gap: '14px',
        }}
      >
        {/* Universal Component Title / Name */}
        <div
          style={{
            background: '#f8fafc',
            padding: '10px',
            borderRadius: '8px',
            border: '1px solid #e2e8f0',
          }}
        >
          <label
            style={{
              display: 'block',
              fontSize: '0.725rem',
              fontWeight: 800,
              color: '#334155',
              marginBottom: '4px',
            }}
          >
            🏷️ COMPONENT TITLE / NAME:
          </label>
          <input
            type="text"
            value={(data.title as string) || ''}
            onChange={(e) => onUpdateData(id, { title: e.target.value })}
            placeholder="Give this component a custom name..."
            style={{
              width: '100%',
              padding: '7px 10px',
              borderRadius: '6px',
              border: '1px solid #cbd5e1',
              fontSize: '0.8rem',
              fontWeight: 700,
              color: '#0f172a',
              boxSizing: 'border-box',
              background: '#ffffff',
            }}
          />
        </div>

        {kind === 'producer' && (
          <ProducerInspectorSection selectedNode={selectedNode} onUpdateData={onUpdateData} />
        )}

        {kind === 'kafka-broker' && (
          <KafkaBrokerInspectorSection
            selectedNode={selectedNode}
            onUpdateData={onUpdateData}
            onCrashBroker={onCrashBroker}
            onReviveBroker={onReviveBroker}
            onSetReplicationFactor={onSetReplicationFactor}
            onSetMinIsr={onSetMinIsr}
            partitionLogs={partitionLogs}
            minInsyncReplicas={minInsyncReplicas}
            replicationFactor={replicationFactor}
          />
        )}

        {kind === 'kafka-cluster' && (
          <KafkaClusterInspectorSection
            selectedNode={selectedNode}
            onUpdateData={onUpdateData}
            onSetPartitionCount={onSetPartitionCount}
            onCrashBroker={onCrashBroker}
            onReviveBroker={onReviveBroker}
            onCreateClusterTopic={onCreateClusterTopic}
            onSetActiveClusterTopic={onSetActiveClusterTopic}
            brokers={brokers}
            partitionLogs={partitionLogs}
            committedOffsets={committedOffsets}
            consumerGroups={consumerGroups}
            allTopics={allTopics}
          />
        )}

        {kind === 'consumer' && (
          <ConsumerInspectorSection
            selectedNode={selectedNode}
            onUpdateData={onUpdateData}
            onCrashConsumer={onCrashConsumer}
            onReviveConsumer={onReviveConsumer}
            onTogglePauseConsumer={onTogglePauseConsumer}
            onTogglePauseConsumerPartition={onTogglePauseConsumerPartition}
            onSetAssignorStrategy={onSetAssignorStrategy}
            assignorStrategy={assignorStrategy}
          />
        )}

        {(kind === 'outbox-table' || kind === 'cdc-relay') && (
          <OutboxCdcInspectorSection selectedNode={selectedNode} onUpdateData={onUpdateData} />
        )}

        {(kind === 'database' || kind === 'dead-letter-queue') && (
          <DatabaseDlqInspectorSection selectedNode={selectedNode} onUpdateData={onUpdateData} />
        )}
      </div>

      {/* Action Buttons */}
      <div
        style={{
          padding: '12px 16px',
          borderTop: '1px solid #e2e8f0',
          background: '#f8fafc',
          display: 'flex',
          flexDirection: 'column',
          gap: '8px',
        }}
      >
        <button
          onClick={() => onDuplicateNode(id)}
          style={{
            width: '100%',
            padding: '8px 12px',
            borderRadius: '6px',
            border: '1px solid #0284c7',
            background: 'linear-gradient(135deg, #0284c7, #0369a1)',
            color: '#ffffff',
            fontWeight: 700,
            fontSize: '0.75rem',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: '6px',
          }}
        >
          <span>➕</span>
          <span>Duplicate Node (Rebalance Group)</span>
        </button>

        <button
          onClick={() => onDeleteNode(id)}
          style={{
            width: '100%',
            padding: '7px 12px',
            borderRadius: '6px',
            border: '1px solid #fca5a5',
            background: '#fef2f2',
            color: '#dc2626',
            fontWeight: 700,
            fontSize: '0.75rem',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: '6px',
          }}
        >
          <span>🗑️</span>
          <span>Delete Node</span>
        </button>
      </div>
    </div>
  );
};

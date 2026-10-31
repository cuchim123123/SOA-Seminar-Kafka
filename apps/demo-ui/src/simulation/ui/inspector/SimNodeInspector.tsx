import React, { useState } from 'react';
import type { SimCustomNode } from '../../domain/types';
import type { SimPartition, SimBroker } from '../../domain/kafka-cluster';

interface SimNodeInspectorProps {
  selectedNode: SimCustomNode | null;
  onClose: () => void;
  onUpdateData: (nodeId: string, partial: Record<string, unknown>) => void;
  onDuplicateNode: (nodeId: string) => void;
  onDeleteNode: (nodeId: string) => void;
  onSetPartitionCount?: (count: number) => void;
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
  allTopics?: { name: string; partitions: Map<number, unknown>; replicationFactor: number }[];
}

export const SimNodeInspector: React.FC<SimNodeInspectorProps> = ({
  selectedNode,
  onClose,
  onUpdateData,
  onDuplicateNode,
  onDeleteNode,
  onSetPartitionCount,
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
}) => {
  const [clusterTab, setClusterTab] = useState<'topics' | 'groups'>('topics');
  const [isCreatingTopic, setIsCreatingTopic] = useState(false);
  const [newTopicName, setNewTopicName] = useState('');
  const [newTopicPartitions, setNewTopicPartitions] = useState(4);
  const [newTopicRF, setNewTopicRF] = useState(3);

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
            <div style={{ fontSize: '0.675rem', color: '#64748b', fontFamily: 'var(--font-mono)' }}>
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

      {/* Body: Component-Specific Fields */}
      <div style={{ padding: '16px', flex: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: '14px' }}>
        {/* Universal Component Title / Name */}
        <div style={{ background: '#f8fafc', padding: '10px', borderRadius: '8px', border: '1px solid #e2e8f0' }}>
          <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 800, color: '#334155', marginBottom: '4px' }}>
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

        {/* PRODUCER CONFIG */}
        {kind === 'producer' && (
          <>
            <div>
              <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
                Customer Key (Murmur2 Target):
              </label>
              <select
                value={(data.messageKey as string) || 'U001'}
                onChange={(e) => onUpdateData(id, { messageKey: e.target.value })}
                style={{
                  width: '100%',
                  padding: '6px 10px',
                  borderRadius: '6px',
                  border: '1px solid #cbd5e1',
                  fontSize: '0.75rem',
                  fontFamily: 'var(--font-mono)',
                }}
              >
                <option value="U001">U001 (Murmur2 ➔ P3)</option>
                <option value="U002">U002 (Murmur2 ➔ P0)</option>
                <option value="U003">U003 (Murmur2 ➔ P2)</option>
                <option value="U004">U004 (Murmur2 ➔ P4)</option>
                <option value="U005">U005 (Murmur2 ➔ P3)</option>
                <option value="U006">U006 (Murmur2 ➔ P3)</option>
                <option value="POISON_PILL">POISON_PILL (Trigger DLQ Isolation)</option>
              </select>

              <div style={{ display: 'flex', gap: '4px', marginTop: '6px', flexWrap: 'wrap' }}>
                <span style={{ fontSize: '0.625rem', color: '#64748b', alignSelf: 'center' }}>Presets:</span>
                {[
                  { title: 'Purchase Producer', icon: '🚀' },
                  { title: 'Checkout API Gateway', icon: '🛒' },
                  { title: 'Mobile Client Ingest', icon: '📱' },
                  { title: 'IoT Telemetry Stream', icon: '📡' },
                ].map((p) => (
                  <button
                    key={p.title}
                    onClick={() => onUpdateData(id, { title: p.title, icon: p.icon })}
                    style={{
                      padding: '3px 7px',
                      borderRadius: '4px',
                      border: '1px solid #cbd5e1',
                      background: '#ffffff',
                      fontSize: '0.625rem',
                      fontWeight: 600,
                      color: '#475569',
                      cursor: 'pointer',
                    }}
                  >
                    {p.icon} {p.title}
                  </button>
                ))}
              </div>
            </div>

            <div>
              <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
                Target Kafka Topic:
              </label>
              <input
                type="text"
                value={(data.targetTopic as string) || 'purchases'}
                onChange={(e) => onUpdateData(id, { targetTopic: e.target.value })}
                placeholder="purchases"
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
            </div>
          </>
        )}

        {/* KAFKA CLUSTER CONFIG */}
        {kind === 'kafka-cluster' && (
          <>
            {/* Cluster Sub-Tabs */}
            <div style={{ display: 'flex', borderBottom: '1px solid #e2e8f0', gap: '4px', paddingBottom: '4px' }}>
              <button
                onClick={() => setClusterTab('topics')}
                style={{
                  flex: 1,
                  padding: '6px 8px',
                  borderRadius: '6px',
                  border: 'none',
                  background: clusterTab === 'topics' ? '#eff6ff' : 'transparent',
                  color: clusterTab === 'topics' ? '#1d4ed8' : '#64748b',
                  fontWeight: clusterTab === 'topics' ? 800 : 600,
                  fontSize: '0.75rem',
                  cursor: 'pointer',
                }}
              >
                📊 Topics &amp; Brokers
              </button>
              <button
                onClick={() => setClusterTab('groups')}
                style={{
                  flex: 1,
                  padding: '6px 8px',
                  borderRadius: '6px',
                  border: 'none',
                  background: clusterTab === 'groups' ? '#eff6ff' : 'transparent',
                  color: clusterTab === 'groups' ? '#1d4ed8' : '#64748b',
                  fontWeight: clusterTab === 'groups' ? 800 : 600,
                  fontSize: '0.75rem',
                  cursor: 'pointer',
                }}
              >
                👥 Consumer Groups ({consumerGroups.length})
              </button>
            </div>

            {clusterTab === 'topics' && (
              <>
                {/* Topic Switcher & Creator */}
                <div style={{ background: '#f8fafc', padding: '10px', borderRadius: '8px', border: '1px solid #e2e8f0' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                    <label style={{ fontSize: '0.725rem', fontWeight: 800, color: '#334155' }}>
                      KAFKA TOPIC MANAGEMENT:
                    </label>
                    <button
                      onClick={() => setIsCreatingTopic(!isCreatingTopic)}
                      style={{
                        padding: '2px 6px',
                        borderRadius: '4px',
                        border: '1px solid #0284c7',
                        background: '#f0f9ff',
                        color: '#0369a1',
                        fontSize: '0.625rem',
                        fontWeight: 700,
                        cursor: 'pointer',
                      }}
                    >
                      {isCreatingTopic ? 'Cancel' : '+ New Topic'}
                    </button>
                  </div>

                  {!isCreatingTopic ? (
                    <div>
                      <select
                        value={(data.activeTopic as string) || 'purchases'}
                        onChange={(e) => onSetActiveClusterTopic?.(id, e.target.value)}
                        style={{
                          width: '100%',
                          padding: '6px 8px',
                          borderRadius: '6px',
                          border: '1px solid #cbd5e1',
                          fontSize: '0.75rem',
                          fontFamily: 'var(--font-mono)',
                          background: '#ffffff',
                        }}
                      >
                        {allTopics.length > 0 ? (
                          allTopics.map((t) => (
                            <option key={t.name} value={t.name}>
                              {t.name} ({t.partitions.size} partitions, RF: {t.replicationFactor})
                            </option>
                          ))
                        ) : (
                          <>
                            <option value="purchases">purchases (4 partitions)</option>
                            <option value="loyalty.events">loyalty.events (3 partitions)</option>
                            <option value="purchases.DLQ">purchases.DLQ (1 partition)</option>
                          </>
                        )}
                      </select>
                    </div>
                  ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', marginTop: '6px' }}>
                      <input
                        type="text"
                        placeholder="Topic name (e.g. inventory.events)"
                        value={newTopicName}
                        onChange={(e) => setNewTopicName(e.target.value)}
                        style={{
                          padding: '5px 8px',
                          borderRadius: '4px',
                          border: '1px solid #cbd5e1',
                          fontSize: '0.725rem',
                          fontFamily: 'var(--font-mono)',
                        }}
                      />
                      <div style={{ display: 'flex', gap: '6px' }}>
                        <div style={{ flex: 1 }}>
                          <span style={{ fontSize: '0.625rem', color: '#64748b' }}>Partitions:</span>
                          <select
                            value={newTopicPartitions}
                            onChange={(e) => setNewTopicPartitions(Number(e.target.value))}
                            style={{ width: '100%', padding: '4px', fontSize: '0.7rem' }}
                          >
                            {[1, 2, 3, 4, 6].map((p) => (
                              <option key={p} value={p}>{p} Partitions</option>
                            ))}
                          </select>
                        </div>
                        <div style={{ flex: 1 }}>
                          <span style={{ fontSize: '0.625rem', color: '#64748b' }}>Replication:</span>
                          <select
                            value={newTopicRF}
                            onChange={(e) => setNewTopicRF(Number(e.target.value))}
                            style={{ width: '100%', padding: '4px', fontSize: '0.7rem' }}
                          >
                            {[1, 2, 3].map((r) => (
                              <option key={r} value={r}>RF: {r}</option>
                            ))}
                          </select>
                        </div>
                      </div>
                      <button
                        onClick={() => {
                          if (newTopicName.trim()) {
                            onCreateClusterTopic?.(id, newTopicName.trim(), newTopicPartitions, newTopicRF);
                            setNewTopicName('');
                            setIsCreatingTopic(false);
                          }
                        }}
                        style={{
                          padding: '6px',
                          borderRadius: '4px',
                          border: 'none',
                          background: '#0284c7',
                          color: '#ffffff',
                          fontWeight: 700,
                          fontSize: '0.725rem',
                          cursor: 'pointer',
                        }}
                      >
                        Create Topic in KRaft Metadata
                      </button>
                    </div>
                  )}
                </div>

                <div>
                  <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
                    Scale Partitions (Dynamic Rebalance):
                  </label>
                  <div style={{ display: 'flex', gap: '6px' }}>
                    {[1, 2, 3, 4, 6].map((pCnt) => (
                      <button
                        key={pCnt}
                        onClick={() => {
                          if (onSetPartitionCount) {
                            onSetPartitionCount(pCnt);
                          } else {
                            onUpdateData(id, { partitionCount: pCnt });
                          }
                        }}
                        style={{
                          flex: 1,
                          padding: '6px 0',
                          borderRadius: '6px',
                          border: (data.partitionCount as number) === pCnt ? '1px solid #7c3aed' : '1px solid #cbd5e1',
                          background: (data.partitionCount as number) === pCnt ? '#f5f3ff' : '#ffffff',
                          color: (data.partitionCount as number) === pCnt ? '#7c3aed' : '#475569',
                          fontWeight: 700,
                          fontSize: '0.725rem',
                          cursor: 'pointer',
                        }}
                      >
                        {pCnt}P
                      </button>
                    ))}
                  </div>
                </div>

                {/* Simulated KRaft Brokers with Crash / Revive */}
                <div>
                  <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 700, color: '#475569', marginBottom: '6px' }}>
                    KRaft Cluster Brokers &amp; Quorum:
                  </label>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                    {brokers.map((b) => {
                      const isOnline = b.status === 'ONLINE';

                      return (
                        <div
                          key={b.id}
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'space-between',
                            padding: '6px 10px',
                            background: isOnline ? '#f0fdf4' : '#fef2f2',
                            border: isOnline ? '1px solid #bbf7d0' : '1px solid #fecaca',
                            borderRadius: '6px',
                            fontSize: '0.725rem',
                            fontFamily: 'var(--font-mono)',
                          }}
                        >
                          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                            <span style={{ fontWeight: 800, color: isOnline ? '#166534' : '#991b1b' }}>
                              Broker B{b.id}
                            </span>
                            {b.isController && (
                              <span
                                style={{
                                  fontSize: '0.6rem',
                                  fontWeight: 800,
                                  background: '#eff6ff',
                                  color: '#1d4ed8',
                                  padding: '1px 5px',
                                  borderRadius: '3px',
                                  border: '1px solid #bfdbfe',
                                }}
                              >
                                Controller
                              </span>
                            )}
                          </div>

                          {isOnline ? (
                            <button
                              onClick={() => onCrashBroker?.(b.id)}
                              style={{
                                padding: '3px 8px',
                                borderRadius: '4px',
                                border: '1px solid #f87171',
                                background: '#ffffff',
                                color: '#dc2626',
                                fontSize: '0.625rem',
                                fontWeight: 700,
                                cursor: 'pointer',
                              }}
                            >
                              💥 Crash
                            </button>
                          ) : (
                            <button
                              onClick={() => onReviveBroker?.(b.id)}
                              style={{
                                padding: '3px 8px',
                                borderRadius: '4px',
                                border: '1px solid #86efac',
                                background: '#ffffff',
                                color: '#16a34a',
                                fontSize: '0.625rem',
                                fontWeight: 700,
                                cursor: 'pointer',
                              }}
                            >
                              🩺 Revive
                            </button>
                          )}
                        </div>
                      );
                    })}
                  </div>
                </div>

                {/* Append-Only Partition Record Tapes */}
                <div style={{ marginTop: '8px' }}>
                  <div style={{ fontSize: '0.725rem', fontWeight: 800, color: '#0f172a', marginBottom: '6px' }}>
                    APPEND-ONLY LOG TAPES (Topic: {(data.activeTopic as string) || 'purchases'}):
                  </div>

                  <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                    {partitionLogs.map((part) => {
                      const pId = part.partitionId;
                      const activeTopic = (data.activeTopic as string) || 'purchases';
                      const committed = committedOffsets.get(`${activeTopic}:${pId}`) ?? 0;
                      const lag = Math.max(0, part.logEndOffset - committed);
                      const leaderOffline = part.leaderBrokerId === -1;

                      return (
                        <div
                          key={pId}
                          style={{
                            background: '#f8fafc',
                            border: leaderOffline ? '1px solid #f87171' : lag > 0 ? '1px solid #fde68a' : '1px solid #e2e8f0',
                            borderRadius: '6px',
                            padding: '8px',
                          }}
                        >
                          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                            <span style={{ fontWeight: 800, fontSize: '0.75rem', color: '#0f172a' }}>
                              Partition P{pId}
                            </span>
                            <span
                              style={{
                                fontSize: '0.65rem',
                                fontWeight: 700,
                                color: lag > 0 ? '#b45309' : '#16a34a',
                              }}
                            >
                              {lag > 0 ? `⚠️ Lag: ${lag}` : '✅ 0 Lag'}
                            </span>
                          </div>

                          <div style={{ display: 'flex', gap: '4px', overflowX: 'auto', padding: '4px 0' }}>
                            {part.records.length === 0 ? (
                              <span style={{ fontSize: '0.675rem', color: '#94a3b8', fontStyle: 'italic' }}>
                                Log empty (LEO: 0)
                              </span>
                            ) : (
                              part.records.slice(-5).map((rec) => (
                                <div
                                  key={rec.offset}
                                  style={{
                                    padding: '3px 6px',
                                    borderRadius: '4px',
                                    background: rec.offset >= committed ? '#fef3c7' : '#ecfdf5',
                                    border: rec.offset >= committed ? '1px solid #f59e0b' : '1px solid #10b981',
                                    fontSize: '0.625rem',
                                    fontFamily: 'var(--font-mono)',
                                    color: rec.offset >= committed ? '#b45309' : '#047857',
                                    whiteSpace: 'nowrap',
                                  }}
                                >
                                  #{rec.offset} ({rec.message.key})
                                </div>
                              ))
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </>
            )}

            {clusterTab === 'groups' && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div style={{ fontSize: '0.725rem', fontWeight: 800, color: '#0f172a' }}>
                  ACTIVE CONSUMER GROUPS IN CLUSTER:
                </div>

                {consumerGroups.length === 0 ? (
                  <div style={{ padding: '12px', background: '#f8fafc', borderRadius: '6px', fontSize: '0.75rem', color: '#64748b', textAlign: 'center' }}>
                    No consumer groups registered yet. Add a Consumer to the canvas!
                  </div>
                ) : (
                  consumerGroups.map((g) => {
                    const memberEntries = Array.from(g.members.entries());
                    let totalLag = 0;
                    for (const part of partitionLogs) {
                      const comm = g.committedNextOffsets.get(`purchases:${part.partitionId}`) ?? 0;
                      totalLag += Math.max(0, part.logEndOffset - comm);
                    }

                    return (
                      <div
                        key={g.groupId}
                        style={{
                          background: '#f8fafc',
                          border: '1px solid #cbd5e1',
                          borderRadius: '8px',
                          padding: '10px',
                        }}
                      >
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                          <span style={{ fontWeight: 800, fontSize: '0.8rem', color: '#7c3aed', fontFamily: 'var(--font-mono)' }}>
                            {g.groupId}
                          </span>
                          <span
                            style={{
                              fontSize: '0.625rem',
                              fontWeight: 800,
                              padding: '2px 6px',
                              borderRadius: '4px',
                              background: totalLag > 0 ? '#fff7ed' : '#ecfdf5',
                              color: totalLag > 0 ? '#c2410c' : '#059669',
                              border: totalLag > 0 ? '1px solid #fdba74' : '1px solid #a7f3d0',
                            }}
                          >
                            {totalLag > 0 ? `Total Lag: ${totalLag}` : 'Caught Up'}
                          </span>
                        </div>

                        <div style={{ fontSize: '0.7rem', color: '#475569', marginBottom: '4px' }}>
                          Assignor: <strong>{g.assignorStrategy}</strong> | Members: <strong>{memberEntries.length}</strong>
                        </div>

                        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', marginTop: '6px' }}>
                          {memberEntries.map(([mId, inst]) => (
                            <div
                              key={mId}
                              style={{
                                display: 'flex',
                                justifyContent: 'space-between',
                                fontSize: '0.675rem',
                                fontFamily: 'var(--font-mono)',
                                background: '#ffffff',
                                padding: '4px 8px',
                                borderRadius: '4px',
                                border: '1px solid #e2e8f0',
                              }}
                            >
                              <span style={{ color: '#0f172a' }}>{mId}</span>
                              <span style={{ color: '#7c3aed', fontWeight: 700 }}>
                                {inst.assignedPartitions.length > 0
                                  ? inst.assignedPartitions.map((p) => `P${p}`).join(', ')
                                  : 'Idle'}
                              </span>
                            </div>
                          ))}
                        </div>
                      </div>
                    );
                  })
                )}
              </div>
            )}
          </>
        )}

        {/* CONSUMER CONFIG */}
        {kind === 'consumer' && (
          <>
            {/* Quick Polling & Backlog Controls */}
            <div style={{ background: '#fffbeb', border: '1px solid #fde68a', borderRadius: '8px', padding: '10px' }}>
              <div style={{ fontSize: '0.725rem', fontWeight: 800, color: '#92400e', marginBottom: '6px' }}>
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
                <span>{data.isPaused ? 'Resume Polling (Drain Lag)' : 'Pause Polling (Accumulate Backlog)'}</span>
              </button>

              {/* Per-Partition Wire Controls */}
              <div style={{ marginTop: '6px' }}>
                <span style={{ fontSize: '0.65rem', fontWeight: 700, color: '#78350f' }}>
                  Partition Polling Wires (Cut to stop individual partition):
                </span>

                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', marginTop: '4px' }}>
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
              <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
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
                <span style={{ fontSize: '0.625rem', color: '#64748b', alignSelf: 'center' }}>Presets:</span>
                {[
                  { title: 'Loyalty Worker', group: 'loyalty-v1', icon: '👥' },
                  { title: 'Analytics Consumer', group: 'analytics-v1', icon: '📈' },
                  { title: 'Fraud Detector', group: 'fraud-detection', icon: '🛡️' },
                  { title: 'Notification Service', group: 'notifications', icon: '🔔' },
                ].map((p) => (
                  <button
                    key={p.title}
                    onClick={() => onUpdateData(id, { title: p.title, groupId: p.group, icon: p.icon })}
                    style={{
                      padding: '3px 7px',
                      borderRadius: '4px',
                      border: (data.groupId as string) === p.group ? '1px solid #7c3aed' : '1px solid #cbd5e1',
                      background: (data.groupId as string) === p.group ? '#f5f3ff' : '#ffffff',
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
            <div style={{ background: '#fff1f2', border: '1px solid #fecdd3', borderRadius: '6px', padding: '10px' }}>
              <div style={{ fontSize: '0.675rem', fontWeight: 800, color: '#be123c', marginBottom: '6px' }}>
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
              <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
                Partition Assignor Strategy:
              </label>
              <div style={{ display: 'flex', gap: '6px' }}>
                <button
                  onClick={() => onSetAssignorStrategy?.('round-robin')}
                  style={{
                    flex: 1,
                    padding: '6px 8px',
                    borderRadius: '6px',
                    border: assignorStrategy === 'round-robin' ? '1px solid #7c3aed' : '1px solid #cbd5e1',
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
                    border: assignorStrategy === 'range' ? '1px solid #7c3aed' : '1px solid #cbd5e1',
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
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
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
        )}

        {/* OUTBOX TABLE CONFIG */}
        {kind === 'outbox-table' && (
          <>
            <div>
              <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
                Transactional Outbox Table:
              </label>
              <input
                type="text"
                value={(data.tableName as string) || 'outbox_events'}
                onChange={(e) => onUpdateData(id, { tableName: e.target.value })}
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
            </div>

            <div style={{ background: '#fff7ed', border: '1px solid #fdba74', borderRadius: '8px', padding: '10px' }}>
              <div style={{ fontSize: '0.725rem', fontWeight: 800, color: '#c2410c', marginBottom: '6px' }}>
                OUTBOX BUFFER STATUS:
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px', fontSize: '0.75rem' }}>
                <span style={{ color: '#64748b' }}>Pending Rows:</span>
                <span style={{ fontWeight: 800, color: '#ea580c', fontFamily: 'var(--font-mono)' }}>
                  {Number(data.pendingRecordsCount) || 0}
                </span>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.75rem' }}>
                <span style={{ color: '#64748b' }}>Published to Kafka:</span>
                <span style={{ fontWeight: 700, color: '#16a34a', fontFamily: 'var(--font-mono)' }}>
                  {Number(data.publishedRecordsCount) || 0}
                </span>
              </div>
            </div>

            <div style={{ background: '#f8fafc', padding: '10px', borderRadius: '6px', border: '1px solid #e2e8f0', fontSize: '0.675rem', color: '#64748b', lineHeight: 1.4 }}>
              💡 <strong>Transactional Outbox Pattern</strong>: Dual-write atomicity between database operations and Kafka message publishing without two-phase commit (2PC). Events are buffered in this table and polled by a CDC Relay.
            </div>
          </>
        )}

        {/* CDC RELAY CONFIG */}
        {kind === 'cdc-relay' && (
          <>
            <div>
              <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
                CDC Connector Engine:
              </label>
              <select
                value={(data.relayType as string) || 'debezium'}
                onChange={(e) => onUpdateData(id, { relayType: e.target.value })}
                style={{
                  width: '100%',
                  padding: '6px 10px',
                  borderRadius: '6px',
                  border: '1px solid #cbd5e1',
                  fontSize: '0.75rem',
                }}
              >
                <option value="debezium">Debezium CDC (Postgres WAL)</option>
                <option value="polling-publisher">Polling Outbox Relay (SELECT ... FOR UPDATE)</option>
              </select>
            </div>

            <div>
              <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
                Poll Interval (ms):
              </label>
              <input
                type="number"
                min="50"
                max="2000"
                step="50"
                value={(data.pollIntervalMs as number) || 150}
                onChange={(e) => onUpdateData(id, { pollIntervalMs: Number(e.target.value) })}
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
            </div>

            <div style={{ background: '#ecfeff', border: '1px solid #a5f3fc', borderRadius: '8px', padding: '10px' }}>
              <div style={{ fontSize: '0.725rem', fontWeight: 800, color: '#0e7490', marginBottom: '6px' }}>
                CDC STREAMING METRICS:
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.75rem' }}>
                <span style={{ color: '#64748b' }}>Events Streamed:</span>
                <span style={{ fontWeight: 800, color: '#0891b2', fontFamily: 'var(--font-mono)' }}>
                  {Number(data.streamedCount) || 0}
                </span>
              </div>
            </div>
          </>
        )}

        {/* DEAD-LETTER QUEUE CONFIG */}
        {kind === 'dead-letter-queue' && (
          <>
            <div>
              <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
                DLQ Topic Name:
              </label>
              <input
                type="text"
                value={(data.topicName as string) || 'purchases.DLQ'}
                onChange={(e) => onUpdateData(id, { topicName: e.target.value })}
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
            </div>

            <div style={{ background: '#fef2f2', border: '1px solid #fecaca', borderRadius: '8px', padding: '10px' }}>
              <div style={{ fontSize: '0.725rem', fontWeight: 800, color: '#b91c1c', marginBottom: '6px' }}>
                ISOLATED POISON-PILL MESSAGES:
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.85rem' }}>
                <span style={{ color: '#64748b' }}>Dead Letters:</span>
                <span style={{ fontWeight: 800, color: '#dc2626', fontFamily: 'var(--font-mono)' }}>
                  {Number(data.deadLetterCount) || 0}
                </span>
              </div>
            </div>
          </>
        )}

        {/* DATABASE CONFIG */}
        {kind === 'database' && (
          <>
            <div>
              <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
                Storage Engine / Projection Model:
              </label>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px', marginBottom: '8px' }}>
                {[
                  { id: 'postgres', label: 'PostgreSQL', icon: '🗄️', engine: 'PostgreSQL 16', defTitle: 'PostgreSQL DB' },
                  { id: 'mongo', label: 'MongoDB', icon: '🍃', engine: 'MongoDB 7', defTitle: 'MongoDB Projection' },
                  { id: 'redis', label: 'Redis', icon: '⚡', engine: 'Redis 7', defTitle: 'Redis Cache Sink' },
                  { id: 'elastic', label: 'Elasticsearch', icon: '🔍', engine: 'Elasticsearch 8', defTitle: 'Elasticsearch Index' },
                  { id: 'clickhouse', label: 'ClickHouse', icon: '📊', engine: 'ClickHouse OLAP', defTitle: 'ClickHouse Analytics' },
                ].map((eng) => {
                  const isSelected = (data.engine as string)?.includes(eng.id) || (data.engine as string) === eng.engine;

                  return (
                    <button
                      key={eng.id}
                      onClick={() =>
                        onUpdateData(id, {
                          engine: eng.engine,
                          icon: eng.icon,
                          title: eng.defTitle,
                        })
                      }
                      style={{
                        padding: '4px 8px',
                        borderRadius: '6px',
                        border: isSelected ? '1px solid #10b981' : '1px solid #cbd5e1',
                        background: isSelected ? '#ecfdf5' : '#ffffff',
                        color: isSelected ? '#047857' : '#475569',
                        fontWeight: isSelected ? 800 : 600,
                        fontSize: '0.675rem',
                        cursor: 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '4px',
                      }}
                    >
                      <span>{eng.icon}</span>
                      <span>{eng.label}</span>
                    </button>
                  );
                })}
              </div>

              <input
                type="text"
                value={(data.engine as string) || 'PostgreSQL 16'}
                onChange={(e) => onUpdateData(id, { engine: e.target.value })}
                placeholder="Custom engine name..."
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
            </div>

            <div>
              <label style={{ display: 'block', fontSize: '0.725rem', fontWeight: 700, color: '#475569', marginBottom: '4px' }}>
                Database / Collection Name:
              </label>
              <input
                type="text"
                value={(data.databaseName as string) || 'loyalty_db'}
                onChange={(e) => onUpdateData(id, { databaseName: e.target.value })}
                placeholder="e.g. loyalty_db, analytics_projections..."
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
            </div>

            <div style={{ background: '#f8fafc', padding: '8px 10px', borderRadius: '6px', border: '1px solid #e2e8f0' }}>
              <div style={{ fontSize: '0.675rem', color: '#64748b', marginBottom: '4px' }}>Committed Records:</div>
              <div style={{ fontSize: '0.85rem', fontWeight: 800, color: '#059669', fontFamily: 'var(--font-mono)' }}>
                {Number(data.committedRecordCount) || 0}
              </div>
            </div>
          </>
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

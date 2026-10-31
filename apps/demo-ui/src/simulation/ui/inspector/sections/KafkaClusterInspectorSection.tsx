import React, { useState } from 'react';
import type { SimCustomNode } from '../../../domain/types';
import type { SimPartition, SimBroker } from '../../../domain/kafka-cluster';

interface KafkaClusterInspectorSectionProps {
  selectedNode: SimCustomNode;
  onUpdateData: (nodeId: string, partial: Record<string, unknown>) => void;
  onSetPartitionCount?: (count: number) => void;
  onCrashBroker?: (brokerId: number) => void;
  onReviveBroker?: (brokerId: number) => void;
  onCreateClusterTopic?: (
    clusterNodeId: string,
    topicName: string,
    partitionCount: number,
    replicationFactor: number,
  ) => void;
  onSetActiveClusterTopic?: (clusterNodeId: string, topicName: string) => void;
  brokers?: SimBroker[];
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

export const KafkaClusterInspectorSection: React.FC<KafkaClusterInspectorSectionProps> = ({
  selectedNode,
  onUpdateData,
  onSetPartitionCount,
  onCrashBroker,
  onReviveBroker,
  onCreateClusterTopic,
  onSetActiveClusterTopic,
  brokers = [],
  partitionLogs = [],
  committedOffsets = new Map(),
  consumerGroups = [],
  allTopics = [],
}) => {
  const { id, data } = selectedNode;
  const [clusterTab, setClusterTab] = useState<'topics' | 'groups'>('topics');
  const [isCreatingTopic, setIsCreatingTopic] = useState(false);
  const [newTopicName, setNewTopicName] = useState('');
  const [newTopicPartitions, setNewTopicPartitions] = useState(4);
  const [newTopicRF, setNewTopicRF] = useState(3);

  return (
    <>
      {/* Cluster Sub-Tabs */}
      <div
        style={{
          display: 'flex',
          borderBottom: '1px solid #e2e8f0',
          gap: '4px',
          paddingBottom: '4px',
        }}
      >
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
          <div
            style={{
              background: '#f8fafc',
              padding: '10px',
              borderRadius: '8px',
              border: '1px solid #e2e8f0',
            }}
          >
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginBottom: '6px',
              }}
            >
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
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '6px',
                  marginTop: '6px',
                }}
              >
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
                        <option key={p} value={p}>
                          {p} Partitions
                        </option>
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
                        <option key={r} value={r}>
                          RF: {r}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
                <button
                  onClick={() => {
                    if (newTopicName.trim()) {
                      onCreateClusterTopic?.(
                        id,
                        newTopicName.trim(),
                        newTopicPartitions,
                        newTopicRF,
                      );
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
            <label
              style={{
                display: 'block',
                fontSize: '0.725rem',
                fontWeight: 700,
                color: '#475569',
                marginBottom: '4px',
              }}
            >
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
                    border:
                      (data.partitionCount as number) === pCnt
                        ? '1px solid #7c3aed'
                        : '1px solid #cbd5e1',
                    background:
                      (data.partitionCount as number) === pCnt ? '#f5f3ff' : '#ffffff',
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
            <label
              style={{
                display: 'block',
                fontSize: '0.725rem',
                fontWeight: 700,
                color: '#475569',
                marginBottom: '6px',
              }}
            >
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
                      <span
                        style={{
                          fontWeight: 800,
                          color: isOnline ? '#166534' : '#991b1b',
                        }}
                      >
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
            <div
              style={{
                fontSize: '0.725rem',
                fontWeight: 800,
                color: '#0f172a',
                marginBottom: '6px',
              }}
            >
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
                      border: leaderOffline
                        ? '1px solid #f87171'
                        : lag > 0
                          ? '1px solid #fde68a'
                          : '1px solid #e2e8f0',
                      borderRadius: '6px',
                      padding: '8px',
                    }}
                  >
                    <div
                      style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        marginBottom: '4px',
                      }}
                    >
                      <span
                        style={{
                          fontWeight: 800,
                          fontSize: '0.75rem',
                          color: '#0f172a',
                        }}
                      >
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

                    <div
                      style={{
                        display: 'flex',
                        gap: '4px',
                        overflowX: 'auto',
                        padding: '4px 0',
                      }}
                    >
                      {part.records.length === 0 ? (
                        <span
                          style={{
                            fontSize: '0.675rem',
                            color: '#94a3b8',
                            fontStyle: 'italic',
                          }}
                        >
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
                              border:
                                rec.offset >= committed
                                  ? '1px solid #f59e0b'
                                  : '1px solid #10b981',
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
            <div
              style={{
                padding: '12px',
                background: '#f8fafc',
                borderRadius: '6px',
                fontSize: '0.75rem',
                color: '#64748b',
                textAlign: 'center',
              }}
            >
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
                  <div
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      marginBottom: '6px',
                    }}
                  >
                    <span
                      style={{
                        fontWeight: 800,
                        fontSize: '0.8rem',
                        color: '#7c3aed',
                        fontFamily: 'var(--font-mono)',
                      }}
                    >
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
                    Assignor: <strong>{g.assignorStrategy}</strong> | Members:{' '}
                    <strong>{memberEntries.length}</strong>
                  </div>

                  <div
                    style={{
                      display: 'flex',
                      flexDirection: 'column',
                      gap: '4px',
                      marginTop: '6px',
                    }}
                  >
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
  );
};

import React, { useState, useEffect, useCallback } from 'react';
import { api } from '../api/client';
import type {
  ConsumerGroupDescriptionDto,
  ConsumerGroupLag,
  ConsumerStatusDto,
  RebalanceEventLogDto,
  TopicPartitionMetadata,
} from '../api/client';

interface ScalingRebalanceStudioProps {
  topicMetadata: TopicPartitionMetadata | null;
  loyaltyLag: ConsumerGroupLag | null;
  analyticsLag: ConsumerGroupLag | null;
  onRefreshTelemetry: () => Promise<void>;
}

export const ScalingRebalanceStudio: React.FC<ScalingRebalanceStudioProps> = ({
  topicMetadata,
  loyaltyLag,
  analyticsLag,
  onRefreshTelemetry,
}) => {
  const [selectedGroup, setSelectedGroup] = useState<'loyalty-v1' | 'analytics-v1'>('loyalty-v1');
  const [groupDescription, setGroupDescription] = useState<ConsumerGroupDescriptionDto | null>(null);
  const [loyaltyStatus, setLoyaltyStatus] = useState<ConsumerStatusDto | null>(null);
  const [rebalanceHistory, setRebalanceHistory] = useState<RebalanceEventLogDto[]>([]);
  const [isScaling, setIsScaling] = useState<boolean>(false);
  const [isKilling, setIsKilling] = useState<boolean>(false);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);

  // Fetch real group description and rebalance logs
  const fetchGroupState = useCallback(async () => {
    try {
      const [desc, loyaltyRaw, hist] = await Promise.all([
        api.describeConsumerGroup(selectedGroup, 'purchase.events').catch(() => null),
        api.getLoyaltyConsumerStatus().catch(() => null),
        api.getLoyaltyRebalanceHistory().catch(() => []),
      ]);

      if (desc) {
        setGroupDescription(desc);
      }

      if (loyaltyRaw) {
        setLoyaltyStatus(loyaltyRaw as ConsumerStatusDto);
      }

      if (hist && Array.isArray(hist)) {
        setRebalanceHistory(hist);
      }
    } catch (err: unknown) {
      console.error('Error fetching group state:', err);
    }
  }, [selectedGroup]);

  useEffect(() => {
    fetchGroupState();
    const interval = setInterval(fetchGroupState, 1500);
    return () => clearInterval(interval);
  }, [fetchGroupState]);

  // Handle scaling up/down
  const handleScale = async (targetCount: number) => {
    setIsScaling(true);
    setStatusMessage(`Scaling ${selectedGroup} to ${targetCount} worker instances...`);
    try {
      await api.scaleLoyaltyConsumer(targetCount);
      setStatusMessage(`Scaled to ${targetCount} instances. Kafka rebalancing in progress...`);
      await onRefreshTelemetry();
      setTimeout(fetchGroupState, 500);
      setTimeout(fetchGroupState, 2000);
    } catch (err: unknown) {
      setStatusMessage(`Scale error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setIsScaling(false);
    }
  };

  // Handle killing a replica instance (crash demo)
  const handleKillReplica = async (instanceId?: string) => {
    setIsKilling(true);
    setStatusMessage(`Simulating ungraceful crash on worker instance...`);
    try {
      const res = await api.killLoyaltyReplica(instanceId);
      setStatusMessage(
        `[Chaos Triggered] ${res.killedInstanceId} abruptly terminated! Kafka detecting leave/timeout and rebalancing...`,
      );
      await onRefreshTelemetry();
      setTimeout(fetchGroupState, 500);
      setTimeout(fetchGroupState, 2000);
    } catch (err: unknown) {
      setStatusMessage(`Kill error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setIsKilling(false);
    }
  };

  // Determine current active instance count
  const activeCount =
    selectedGroup === 'loyalty-v1'
      ? loyaltyStatus?.instanceCount ?? groupDescription?.members.length ?? 1
      : groupDescription?.members.length ?? 1;

  // Active lag based on selected group
  const activeLag = selectedGroup === 'loyalty-v1' ? loyaltyLag : analyticsLag;

  // Color assignments per member
  const getMemberColorClass = (clientId: string): { bg: string; text: string; border: string; badge: string } => {
    if (clientId.includes('replica-1')) {
      return {
        bg: 'var(--color-cyan-bg)',
        text: 'var(--color-cyan)',
        border: 'var(--color-cyan-border)',
        badge: 'badge-cyan',
      };
    }
    if (clientId.includes('replica-2')) {
      return {
        bg: 'var(--color-amber-bg)',
        text: 'var(--color-amber)',
        border: 'var(--color-amber-border)',
        badge: 'badge-amber',
      };
    }
    if (clientId.includes('replica-3')) {
      return {
        bg: 'var(--color-crimson-bg)',
        text: 'var(--color-crimson)',
        border: 'var(--color-crimson-border)',
        badge: 'badge-crimson',
      };
    }
    // Primary worker
    return {
      bg: 'var(--color-violet-bg)',
      text: 'var(--color-violet)',
      border: 'var(--color-violet-border)',
      badge: 'badge-violet',
    };
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {/* 1. Header & Group Selector Deck */}
      <div
        className="cyber-card"
        style={{
          padding: '20px 24px',
          display: 'flex',
          flexDirection: 'column',
          gap: '16px',
          background: 'linear-gradient(135deg, var(--bg-surface) 0%, var(--bg-subtle) 100%)',
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: '12px',
          }}
        >
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              <span style={{ fontSize: '1.4rem' }}>⚖️</span>
              <h2 style={{ fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-main)' }}>
                Dynamic Consumer Scaling &amp; Rebalance Studio
              </h2>
              <span className="badge-mono badge-cyan">KAFKA ASSIGNMENT ENGINE</span>
            </div>
            <p style={{ fontSize: '0.875rem', color: 'var(--text-muted)', marginTop: '4px' }}>
              Scale consumer group members dynamically. Observe real-time partition reassignments,
              group state changes (<code>Stable</code> → <code>PreparingRebalance</code>), and zero-loss crash recovery.
            </p>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)', fontWeight: 600 }}>GROUP:</span>
            <button
              onClick={() => setSelectedGroup('loyalty-v1')}
              className={`badge-mono ${selectedGroup === 'loyalty-v1' ? 'badge-violet' : ''}`}
              style={{
                cursor: 'pointer',
                padding: '6px 14px',
                border: '1px solid var(--border-medium)',
                background: selectedGroup === 'loyalty-v1' ? 'var(--color-violet-bg)' : 'var(--bg-surface)',
                borderRadius: 'var(--radius-sm)',
                fontWeight: 700,
              }}
            >
              loyalty-v1 (Scalable)
            </button>
            <button
              onClick={() => setSelectedGroup('analytics-v1')}
              className={`badge-mono ${selectedGroup === 'analytics-v1' ? 'badge-amber' : ''}`}
              style={{
                cursor: 'pointer',
                padding: '6px 14px',
                border: '1px solid var(--border-medium)',
                background: selectedGroup === 'analytics-v1' ? 'var(--color-amber-bg)' : 'var(--bg-surface)',
                borderRadius: 'var(--radius-sm)',
                fontWeight: 700,
              }}
            >
              analytics-v1 (Baseline)
            </button>
          </div>
        </div>

        {/* Status Indicators Bar */}
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
            gap: '12px',
            paddingTop: '8px',
            borderTop: '1px solid var(--border-subtle)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Group State:</span>
            <span
              className={`badge-mono ${
                groupDescription?.state === 'Stable'
                  ? 'badge-emerald'
                  : groupDescription?.state === 'PreparingRebalance'
                    ? 'badge-amber'
                    : 'badge-cyan'
              }`}
            >
              ● {groupDescription?.state || 'CONNECTING...'}
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Protocol:</span>
            <span className="badge-mono badge-cyan" style={{ fontSize: '0.75rem' }}>
              {groupDescription?.protocol || 'RoundRobinAssigner'}
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Active Members:</span>
            <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 700, color: 'var(--text-main)' }}>
              {groupDescription?.members.length ?? 0} instance(s)
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Rebalances Observed:</span>
            <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 700, color: 'var(--color-violet)' }}>
              {loyaltyStatus?.totalRebalanceCount ?? rebalanceHistory.length}
            </span>
          </div>
        </div>

        {statusMessage && (
          <div
            style={{
              padding: '8px 14px',
              backgroundColor: 'var(--color-cyan-bg)',
              border: '1px solid var(--color-cyan-border)',
              borderRadius: 'var(--radius-sm)',
              fontSize: '0.825rem',
              color: 'var(--color-cyan)',
              fontFamily: 'var(--font-mono)',
            }}
          >
            ℹ️ {statusMessage}
          </div>
        )}
      </div>

      {/* 2. Direct Scaling & Chaos Control Center (for loyalty-v1) */}
      {selectedGroup === 'loyalty-v1' ? (
        <div
          className="cyber-card"
          style={{
            padding: '20px 24px',
            display: 'flex',
            flexDirection: 'column',
            gap: '16px',
            borderLeft: '4px solid var(--color-violet)',
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '10px' }}>
            <div>
              <h3 style={{ fontSize: '1rem', fontWeight: 700, color: 'var(--text-main)' }}>
                Fleet Scaling &amp; Chaos Injection Controls
              </h3>
              <p style={{ fontSize: '0.825rem', color: 'var(--text-muted)', marginTop: '2px' }}>
                Adjust active consumer members in <code>loyalty-v1</code>. Kafka broker assigns partitions automatically.
              </p>
            </div>

            <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
              <button
                onClick={() => handleScale(1)}
                disabled={isScaling || activeCount === 1}
                className="badge-mono"
                style={{
                  padding: '8px 16px',
                  cursor: activeCount === 1 ? 'not-allowed' : 'pointer',
                  border: '1px solid var(--border-medium)',
                  background: activeCount === 1 ? 'var(--color-violet-bg)' : 'var(--bg-surface)',
                  color: activeCount === 1 ? 'var(--color-violet)' : 'var(--text-main)',
                  borderRadius: 'var(--radius-sm)',
                  fontWeight: 700,
                  opacity: activeCount === 1 ? 1 : 0.9,
                }}
              >
                1 Worker (100% Load)
              </button>

              <button
                onClick={() => handleScale(2)}
                disabled={isScaling || activeCount === 2}
                className="badge-mono"
                style={{
                  padding: '8px 16px',
                  cursor: activeCount === 2 ? 'not-allowed' : 'pointer',
                  border: '1px solid var(--border-medium)',
                  background: activeCount === 2 ? 'var(--color-cyan-bg)' : 'var(--bg-surface)',
                  color: activeCount === 2 ? 'var(--color-cyan)' : 'var(--text-main)',
                  borderRadius: 'var(--radius-sm)',
                  fontWeight: 700,
                  opacity: activeCount === 2 ? 1 : 0.9,
                }}
              >
                2 Workers (50/50 Split)
              </button>

              <button
                onClick={() => handleScale(3)}
                disabled={isScaling || activeCount === 3}
                className="badge-mono"
                style={{
                  padding: '8px 16px',
                  cursor: activeCount === 3 ? 'not-allowed' : 'pointer',
                  border: '1px solid var(--border-medium)',
                  background: activeCount === 3 ? 'var(--color-amber-bg)' : 'var(--bg-surface)',
                  color: activeCount === 3 ? 'var(--color-amber)' : 'var(--text-main)',
                  borderRadius: 'var(--radius-sm)',
                  fontWeight: 700,
                  opacity: activeCount === 3 ? 1 : 0.9,
                }}
              >
                3 Workers (3-Way Split)
              </button>

              {activeCount > 1 && (
                <button
                  onClick={() => handleKillReplica()}
                  disabled={isKilling}
                  className="badge-mono"
                  style={{
                    padding: '8px 16px',
                    cursor: isKilling ? 'not-allowed' : 'pointer',
                    border: '1px solid var(--color-crimson-border)',
                    background: 'var(--color-crimson-bg)',
                    color: 'var(--color-crimson)',
                    borderRadius: 'var(--radius-sm)',
                    fontWeight: 700,
                  }}
                  title="Abruptly kills worker replica process. Demonstrates session timeout failure detection and partition recovery."
                >
                  ⚡ Kill Worker Instance (Chaos Crash)
                </button>
              )}
            </div>
          </div>
        </div>
      ) : (
        <div
          className="cyber-card"
          style={{
            padding: '16px 20px',
            backgroundColor: 'var(--color-amber-bg)',
            border: '1px solid var(--color-amber-border)',
            borderRadius: 'var(--radius-lg)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ fontSize: '1.2rem' }}>ℹ️</span>
            <div>
              <span style={{ fontWeight: 700, color: 'var(--color-amber)' }}>Independent Group Isolation:</span>{' '}
              <span style={{ fontSize: '0.85rem', color: 'var(--text-body)' }}>
                <code>analytics-v1</code> operates as an independent consumer group. Scaling <code>loyalty-v1</code>{' '}
                never triggers rebalances or delays on <code>analytics-v1</code>.
              </span>
            </div>
          </div>
        </div>
      )}

      {/* 3. Real-Time Partition Assignment Grid (10 Partitions) */}
      <div className="cyber-card" style={{ padding: '20px 24px' }}>
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            marginBottom: '16px',
            flexWrap: 'wrap',
            gap: '10px',
          }}
        >
          <div>
            <h3 style={{ fontSize: '1.05rem', fontWeight: 700, color: 'var(--text-main)' }}>
              Live Partition Assignment Matrix (Kafka RoundRobinAssigner)
            </h3>
            <p style={{ fontSize: '0.825rem', color: 'var(--text-muted)', marginTop: '2px' }}>
              Each of the 10 partitions can only be owned by <strong>one</strong> worker member at any time within this group.
            </p>
          </div>

          <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>MEMBER LEGEND:</span>
            {groupDescription?.members.map((m, idx) => {
              const color = getMemberColorClass(m.clientId);
              return (
                <span
                  key={m.memberId}
                  className={`badge-mono ${color.badge}`}
                  style={{ fontSize: '0.7rem', padding: '3px 8px' }}
                >
                  Worker #{idx + 1} ({m.assignedPartitions.length}P)
                </span>
              );
            })}
          </div>
        </div>

        {/* 10-Partition Grid */}
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))',
            gap: '12px',
          }}
        >
          {Array.from({ length: topicMetadata?.partitionCount ?? 10 }).map((_, partitionIndex) => {
            // Find which member owns this partition
            const ownerMember = groupDescription?.members.find((m) =>
              m.assignedPartitions.includes(partitionIndex),
            );
            const ownerIndex = ownerMember
              ? (groupDescription?.members.indexOf(ownerMember) ?? 0) + 1
              : 0;

            const partitionMeta = topicMetadata?.partitions.find(
              (p) => p.partition === partitionIndex,
            );
            const partitionLagInfo = activeLag?.partitions.find(
              (p) => p.partition === partitionIndex,
            );

            const color = ownerMember
              ? getMemberColorClass(ownerMember.clientId)
              : {
                  bg: 'var(--bg-subtle)',
                  text: 'var(--text-muted)',
                  border: 'var(--border-subtle)',
                  badge: 'badge-mono',
                };

            return (
              <div
                key={partitionIndex}
                style={{
                  backgroundColor: color.bg,
                  border: `2px solid ${color.border}`,
                  borderRadius: 'var(--radius-md)',
                  padding: '12px',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '8px',
                  transition: 'all 0.3s ease',
                  boxShadow: 'var(--shadow-sm)',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span
                    style={{
                      fontFamily: 'var(--font-mono)',
                      fontSize: '0.9rem',
                      fontWeight: 800,
                      color: 'var(--text-main)',
                    }}
                  >
                    Partition {partitionIndex}
                  </span>
                  <span
                    className={`badge-mono ${color.badge}`}
                    style={{ fontSize: '0.65rem', padding: '2px 6px' }}
                  >
                    {ownerIndex > 0 ? `W#${ownerIndex}` : 'UNASSIGNED'}
                  </span>
                </div>

                <div
                  style={{
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '4px',
                    fontSize: '0.725rem',
                    fontFamily: 'var(--font-mono)',
                    color: 'var(--text-body)',
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>High-Water:</span>
                    <span style={{ fontWeight: 600 }}>{partitionMeta?.highWatermark ?? '0'}</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Committed:</span>
                    <span style={{ fontWeight: 600 }}>
                      {partitionLagInfo?.committedOffset ?? 'none'}
                    </span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Lag:</span>
                    <span
                      style={{
                        fontWeight: 700,
                        color:
                          partitionLagInfo?.lag === 'UNCOMMITTED'
                            ? 'var(--color-amber)'
                            : Number(partitionLagInfo?.lag ?? 0) > 0
                              ? 'var(--color-crimson)'
                              : 'var(--color-emerald)',
                      }}
                    >
                      {partitionLagInfo?.lag ?? '0'}
                    </span>
                  </div>
                </div>

                <div
                  style={{
                    fontSize: '0.65rem',
                    color: 'var(--text-muted)',
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    borderTop: '1px solid rgba(0,0,0,0.06)',
                    paddingTop: '6px',
                  }}
                  title={ownerMember?.clientId ?? 'None'}
                >
                  {ownerMember ? ownerMember.clientId : 'Unassigned'}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* 4. Active Fleet Member Details */}
      <div className="cyber-card" style={{ padding: '20px 24px' }}>
        <h3 style={{ fontSize: '1.05rem', fontWeight: 700, color: 'var(--text-main)', marginBottom: '14px' }}>
          Active Consumer Group Fleet ({groupDescription?.members.length ?? 0} Connected Members)
        </h3>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '14px' }}>
          {groupDescription?.members.map((member, idx) => {
            const color = getMemberColorClass(member.clientId);
            const isPrimary = !member.clientId.includes('replica');

            return (
              <div
                key={member.memberId}
                style={{
                  border: `1px solid ${color.border}`,
                  backgroundColor: color.bg,
                  borderRadius: 'var(--radius-md)',
                  padding: '16px',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '10px',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                  <div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span style={{ fontWeight: 700, fontSize: '0.95rem', color: 'var(--text-main)' }}>
                        {isPrimary ? 'Primary Worker Instance' : `Worker Replica #${idx}`}
                      </span>
                      <span className={`badge-mono ${color.badge}`} style={{ fontSize: '0.65rem' }}>
                        {isPrimary ? 'PRIMARY' : 'REPLICA'}
                      </span>
                    </div>
                    <div
                      style={{
                        fontFamily: 'var(--font-mono)',
                        fontSize: '0.75rem',
                        color: 'var(--text-muted)',
                        marginTop: '2px',
                      }}
                    >
                      Client ID: <strong>{member.clientId}</strong>
                    </div>
                  </div>

                  {!isPrimary && selectedGroup === 'loyalty-v1' && (
                    <button
                      onClick={() => handleKillReplica(member.clientId)}
                      disabled={isKilling}
                      className="badge-mono"
                      style={{
                        padding: '4px 8px',
                        cursor: 'pointer',
                        background: 'var(--color-crimson-bg)',
                        border: '1px solid var(--color-crimson-border)',
                        color: 'var(--color-crimson)',
                        borderRadius: 'var(--radius-sm)',
                        fontSize: '0.7rem',
                      }}
                      title="Kill this specific replica instance"
                    >
                      ✕ Kill
                    </button>
                  )}
                </div>

                <div
                  style={{
                    fontSize: '0.725rem',
                    fontFamily: 'var(--font-mono)',
                    color: 'var(--text-muted)',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '2px',
                  }}
                >
                  <div>Host: {member.clientHost}</div>
                  <div
                    style={{
                      whiteSpace: 'nowrap',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                    }}
                    title={member.memberId}
                  >
                    Member ID: {member.memberId}
                  </div>
                </div>

                <div style={{ borderTop: '1px solid rgba(0,0,0,0.06)', paddingTop: '8px' }}>
                  <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-body)', marginBottom: '6px' }}>
                    Assigned Partitions ({member.assignedPartitions.length}):
                  </div>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                    {member.assignedPartitions.length > 0 ? (
                      member.assignedPartitions.map((p) => (
                        <span
                          key={p}
                          className="badge-mono"
                          style={{
                            backgroundColor: 'var(--bg-surface)',
                            border: '1px solid var(--border-medium)',
                            padding: '2px 6px',
                            fontSize: '0.7rem',
                            fontWeight: 700,
                          }}
                        >
                          P{p}
                        </span>
                      ))
                    ) : (
                      <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)', fontStyle: 'italic' }}>
                        No partitions currently assigned
                      </span>
                    )}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* 5. Real-Time Kafka Rebalance Event Log */}
      <div className="cyber-card" style={{ padding: '20px 24px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '14px' }}>
          <div>
            <h3 style={{ fontSize: '1.05rem', fontWeight: 700, color: 'var(--text-main)' }}>
              Real Kafka Rebalance Event Stream
            </h3>
            <p style={{ fontSize: '0.825rem', color: 'var(--text-muted)', marginTop: '2px' }}>
              Timestamped broker-coordinated rebalance events captured directly from KafkaJS consumer lifecycle hooks.
            </p>
          </div>
          <span className="badge-mono badge-cyan">{rebalanceHistory.length} EVENTS RECORDED</span>
        </div>

        <div
          style={{
            maxHeight: '320px',
            overflowY: 'auto',
            display: 'flex',
            flexDirection: 'column',
            gap: '8px',
          }}
        >
          {rebalanceHistory.length > 0 ? (
            rebalanceHistory.map((ev) => {
              const isJoin = ev.eventType === 'GROUP_JOIN' || ev.eventType === 'MEMBER_JOINED';
              const isKill = ev.eventType === 'MEMBER_KILLED';
              const isReb = ev.eventType === 'REBALANCING';

              return (
                <div
                  key={ev.id}
                  style={{
                    padding: '10px 14px',
                    borderRadius: 'var(--radius-sm)',
                    border: '1px solid var(--border-subtle)',
                    backgroundColor: isKill
                      ? 'var(--color-crimson-bg)'
                      : isReb
                        ? 'var(--color-amber-bg)'
                        : isJoin
                          ? 'var(--color-emerald-bg)'
                          : 'var(--bg-surface)',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '4px',
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span
                        className={`badge-mono ${
                          isKill
                            ? 'badge-crimson'
                            : isReb
                              ? 'badge-amber'
                              : isJoin
                                ? 'badge-emerald'
                                : 'badge-cyan'
                        }`}
                        style={{ fontSize: '0.65rem' }}
                      >
                        {ev.eventType}
                      </span>
                      <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.75rem', fontWeight: 600 }}>
                        {ev.clientId}
                      </span>
                      {ev.durationMs !== undefined && (
                        <span style={{ fontSize: '0.75rem', color: 'var(--color-emerald)', fontWeight: 600 }}>
                          ⏱️ {ev.durationMs}ms
                        </span>
                      )}
                    </div>
                    <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                      {new Date(ev.timestamp).toLocaleTimeString()}
                    </span>
                  </div>

                  <div style={{ fontSize: '0.8rem', color: 'var(--text-body)' }}>{ev.details}</div>

                  {ev.assignedPartitions && ev.assignedPartitions.length > 0 && (
                    <div style={{ display: 'flex', gap: '4px', alignItems: 'center', marginTop: '2px' }}>
                      <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>Assigned:</span>
                      {ev.assignedPartitions.map((p) => (
                        <span
                          key={p}
                          style={{
                            padding: '1px 5px',
                            background: 'var(--bg-surface)',
                            border: '1px solid var(--border-subtle)',
                            borderRadius: '3px',
                            fontSize: '0.65rem',
                            fontFamily: 'var(--font-mono)',
                            fontWeight: 700,
                          }}
                        >
                          P{p}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              );
            })
          ) : (
            <div style={{ textAlign: 'center', padding: '30px', color: 'var(--text-muted)', fontSize: '0.875rem' }}>
              No rebalance events captured yet. Use the scaling controls above to trigger real Kafka partition reassignment.
            </div>
          )}
        </div>
      </div>

      {/* 6. Kafka Architectural Invariants Summary */}
      <div
        className="cyber-card"
        style={{
          padding: '16px 20px',
          borderLeft: '4px solid var(--color-cyan)',
          backgroundColor: 'var(--bg-surface)',
          display: 'flex',
          flexDirection: 'column',
          gap: '8px',
        }}
      >
        <h4 style={{ fontSize: '0.9rem', fontWeight: 700, color: 'var(--text-main)' }}>
          Kafka Rebalance &amp; Partition Exclusivity Invariants:
        </h4>
        <ul style={{ fontSize: '0.825rem', color: 'var(--text-muted)', paddingLeft: '20px', lineHeight: 1.6 }}>
          <li>
            <strong>Strict Ordering Guarantee:</strong> Kafka guarantees ordering <em>only within a single partition</em>.
            Because each partition is assigned to exactly one worker instance in a consumer group, processing for a single
            customer key remains strictly ordered across scaling events.
          </li>
          <li>
            <strong>RoundRobinAssigner Dynamics:</strong> Partitions 0-9 are evenly distributed across available members
            (e.g. 10 → 5+5 → 4+3+3). When a worker instance leaves or dies, remaining members take over the orphaned partitions.
          </li>
          <li>
            <strong>Zero Loss &amp; Exactly-Once Database Effects:</strong> Consumers commit offset $N+1$ only <em>after</em> the
            database transaction containing customer projection and <code>processed_events</code> insertion succeeds.
          </li>
        </ul>
      </div>
    </div>
  );
};

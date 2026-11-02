import React, { useState } from 'react';
import type { SimEngine } from '../../engine/sim-engine';
import type { KafkaMechanicsConfig, SimCommitMode, SimOffsetResetPolicy } from '../../domain/types';

interface CurrentOffsetMenuProps {
  engine: SimEngine;
  config: KafkaMechanicsConfig;
  onSetCommitMode: (mode: SimCommitMode) => void;
  onSetAutoOffsetReset: (policy: SimOffsetResetPolicy) => void;
  onSeek: (groupId: string, partitionId: number, nextOffset: number) => boolean;
  onResetGroup: (groupId: string, policy: SimOffsetResetPolicy) => void;
}

const segBtn = (active: boolean, color: string): React.CSSProperties => ({
  padding: '3px 8px',
  borderRadius: '4px',
  border: 'none',
  background: active ? color : 'transparent',
  color: active ? '#ffffff' : '#64748b',
  fontWeight: active ? 800 : 600,
  fontSize: '0.7rem',
  cursor: 'pointer',
});

const miniBtn: React.CSSProperties = {
  padding: '1px 6px',
  borderRadius: '4px',
  border: '1px solid #cbd5e1',
  background: '#ffffff',
  fontSize: '0.68rem',
  fontWeight: 700,
  cursor: 'pointer',
};

const Segmented: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <div
    style={{
      display: 'inline-flex',
      background: '#e2e8f0',
      borderRadius: '6px',
      padding: '2px',
      gap: '2px',
    }}
  >
    {children}
  </div>
);

export const CurrentOffsetMenu: React.FC<CurrentOffsetMenuProps> = ({
  engine,
  config,
  onSetCommitMode,
  onSetAutoOffsetReset,
  onSeek,
  onResetGroup,
}) => {
  const [open, setOpen] = useState(false);
  const groupIds = Array.from(engine.consumerGroups.keys());
  const [selectedGroup, setSelectedGroup] = useState<string>(groupIds[0] ?? 'loyalty-v1');
  const groupId = groupIds.includes(selectedGroup) ? selectedGroup : (groupIds[0] ?? selectedGroup);
  const group = engine.consumerGroups.get(groupId);
  const topicName = group?.topicName ?? 'purchases';
  const partitions = engine.kafkaCluster.getAllPartitions(topicName);

  const rows = partitions.map((p) => {
    const committed = group?.getCommittedNextOffset(topicName, p.partitionId) ?? 0;
    return {
      id: p.partitionId,
      leo: p.logEndOffset,
      committed,
      lag: Math.max(0, p.logEndOffset - committed),
      inFlight: group?.isPartitionInFlight(p.partitionId) ?? false,
    };
  });
  const totalLag = rows.reduce((sum, r) => sum + r.lag, 0);

  return (
    <div style={{ position: 'relative' }}>
      <button
        id="current-offset-menu-toggle"
        onClick={() => setOpen(!open)}
        title="Inspect and manipulate consumer group offsets"
        style={{
          padding: '4px 10px',
          borderRadius: '6px',
          border: '1px solid #0d9488',
          background: open ? '#0d9488' : '#f0fdfa',
          color: open ? '#ffffff' : '#115e59',
          fontWeight: 700,
          fontSize: '0.7rem',
          cursor: 'pointer',
        }}
      >
        📊 Offsets · Lag {totalLag} · {config.commitMode === 'manual' ? 'Manual' : 'Auto'} ▾
      </button>

      {open && (
        <div
          id="current-offset-menu"
          style={{
            position: 'absolute',
            top: '110%',
            right: 0,
            zIndex: 50,
            width: '420px',
            padding: '12px',
            background: '#ffffff',
            border: '1px solid #cbd5e1',
            borderRadius: '10px',
            boxShadow: '0 12px 32px rgba(15,23,42,0.18)',
            display: 'flex',
            flexDirection: 'column',
            gap: '10px',
          }}
        >
          <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap', alignItems: 'center' }}>
            <span style={{ fontWeight: 700, color: '#475569' }}>Group (topic {topicName}):</span>
            <select
              value={groupId}
              onChange={(e) => setSelectedGroup(e.target.value)}
              style={{ fontSize: '0.7rem', padding: '2px 4px' }}
            >
              {groupIds.map((g) => (
                <option key={g} value={g}>
                  {g}
                </option>
              ))}
            </select>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <span style={{ fontWeight: 700, color: '#475569' }}>Commit:</span>
            <Segmented>
              <button
                style={segBtn(config.commitMode === 'manual', '#0d9488')}
                onClick={() => onSetCommitMode('manual')}
                title="Offset N+1 is committed only after the handler finishes (at-least-once)"
              >
                🛡️ Manual (N+1)
              </button>
              <button
                style={segBtn(config.commitMode === 'auto', '#d97706')}
                onClick={() => onSetCommitMode('auto')}
                title="Offset advances at poll time, before processing (messages can be lost on failure)"
              >
                ⚡ Auto
              </button>
            </Segmented>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <span style={{ fontWeight: 700, color: '#475569' }}>auto.offset.reset:</span>
            <Segmented>
              <button
                style={segBtn(config.autoOffsetReset === 'earliest', '#0284c7')}
                onClick={() => onSetAutoOffsetReset('earliest')}
                title="Groups with no committed offset start at 0"
              >
                earliest
              </button>
              <button
                style={segBtn(config.autoOffsetReset === 'latest', '#7c3aed')}
                onClick={() => onSetAutoOffsetReset('latest')}
                title="Groups with no committed offset start at the next new record"
              >
                latest
              </button>
            </Segmented>
          </div>

          <table style={{ width: '100%', fontSize: '0.72rem', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ color: '#64748b', textAlign: 'left' }}>
                <th>Part.</th>
                <th>LEO</th>
                <th>Next read</th>
                <th>Lag</th>
                <th>Seek</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.id} style={{ borderTop: '1px solid #f1f5f9' }}>
                  <td style={{ fontWeight: 700 }}>P{r.id}</td>
                  <td>#{r.leo}</td>
                  <td>#{r.committed}</td>
                  <td style={{ color: r.lag > 0 ? '#dc2626' : '#059669', fontWeight: 700 }}>
                    {r.lag}
                  </td>
                  <td style={{ display: 'flex', gap: '3px', padding: '2px 0' }}>
                    <button
                      style={miniBtn}
                      disabled={r.inFlight}
                      title={r.inFlight ? 'Record in flight' : 'Seek back by one'}
                      onClick={() => onSeek(groupId, r.id, r.committed - 1)}
                    >
                      −1
                    </button>
                    <button
                      style={miniBtn}
                      disabled={r.inFlight}
                      title="Seek to offset 0 (replay)"
                      onClick={() => onSeek(groupId, r.id, 0)}
                    >
                      ⏮
                    </button>
                    <button
                      style={miniBtn}
                      disabled={r.inFlight}
                      title="Seek forward by one (skip a record)"
                      onClick={() => onSeek(groupId, r.id, r.committed + 1)}
                    >
                      +1
                    </button>
                    <button
                      style={miniBtn}
                      disabled={r.inFlight}
                      title="Seek to LEO (skip backlog)"
                      onClick={() => onSeek(groupId, r.id, r.leo)}
                    >
                      ⏭
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          <div style={{ display: 'flex', gap: '8px' }}>
            <button style={miniBtn} onClick={() => onResetGroup(groupId, 'earliest')}>
              ⏮ Reset group to earliest
            </button>
            <button style={miniBtn} onClick={() => onResetGroup(groupId, 'latest')}>
              ⏭ Reset group to latest
            </button>
          </div>
          <div style={{ fontSize: '0.65rem', color: '#94a3b8' }}>
            Offsets are per (group, topic, partition). Lag = LEO − next read offset. Seeks are
            rejected while a record is in flight.
          </div>
        </div>
      )}
    </div>
  );
};

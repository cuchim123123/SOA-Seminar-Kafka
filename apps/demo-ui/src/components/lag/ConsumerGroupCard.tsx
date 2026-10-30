import React, { useState } from 'react';
import type { ConsumerGroupLag, ConsumerStatusDto } from '../../api/client';

const LATENCY_PRESETS = [0, 100, 300, 500, 1000];

interface ConsumerGroupCardProps {
  groupId: string;
  databaseName: string;
  themeColor: 'emerald' | 'cyan' | 'violet';
  lag: ConsumerGroupLag | null;
  status: ConsumerStatusDto | null;
  onTogglePause: () => Promise<void>;
  onApplyDelay: (delayMs: number) => Promise<void>;
}

export const ConsumerGroupCard: React.FC<ConsumerGroupCardProps> = ({
  groupId,
  databaseName,
  themeColor,
  lag,
  status,
  onTogglePause,
  onApplyDelay,
}) => {
  const isPaused = status?.isPaused ?? false;
  const currentDelay = status?.processingDelayMs ?? 0;

  const [toggling, setToggling] = useState<boolean>(false);
  const [delayInput, setDelayInput] = useState<string>(String(currentDelay));
  const [updatingDelay, setUpdatingDelay] = useState<boolean>(false);

  const handleToggle = async () => {
    setToggling(true);
    try {
      await onTogglePause();
    } finally {
      setToggling(false);
    }
  };

  const handleApply = async (msValue?: number) => {
    const delay = typeof msValue === 'number' ? msValue : Math.max(0, parseInt(delayInput, 10) || 0);
    setUpdatingDelay(true);
    try {
      await onApplyDelay(delay);
      setDelayInput(String(delay));
    } finally {
      setUpdatingDelay(false);
    }
  };

  const getAccentColor = () => {
    switch (themeColor) {
      case 'cyan':
        return 'var(--color-cyan)';
      case 'violet':
        return 'var(--color-violet)';
      default:
        return 'var(--color-emerald)';
    }
  };

  return (
    <div
      style={{
        padding: '20px',
        borderRadius: 'var(--radius-md)',
        background: isPaused ? '#fffbeb' : '#ffffff',
        border: isPaused ? '1px solid #fde68a' : '1px solid var(--border-subtle)',
        boxShadow: 'var(--shadow-sm)',
      }}
    >
      {/* Header & Status */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span
            style={{
              width: '10px',
              height: '10px',
              borderRadius: '50%',
              background: isPaused ? 'var(--color-amber)' : getAccentColor(),
              display: 'inline-block',
            }}
          />
          <span style={{ fontWeight: 800, fontSize: '1rem', color: 'var(--text-main)', fontFamily: 'var(--font-mono)' }}>
            {groupId}
          </span>
          <span className="badge-mono badge-ghost" style={{ fontSize: '0.7rem' }}>
            {databaseName}
          </span>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span className={`badge-mono ${isPaused ? 'badge-amber' : 'badge-emerald'}`}>
            {isPaused ? 'PAUSED' : 'RUNNING'}
          </span>

          <button
            onClick={handleToggle}
            disabled={toggling}
            className={`btn-cyber ${isPaused ? 'btn-emerald' : 'btn-amber'}`}
            style={{ padding: '4px 10px', fontSize: '0.75rem' }}
            title={isPaused ? 'Resume consumer to drain lag' : 'Pause consumer to observe lag accumulation'}
          >
            {toggling ? '...' : isPaused ? '▶ Resume' : '⏸ Pause'}
          </button>
        </div>
      </div>

      {/* Latency Simulator Section */}
      <div
        style={{
          padding: '12px 14px',
          borderRadius: 'var(--radius-sm)',
          background: currentDelay > 0 ? '#f0fdf4' : 'var(--bg-subtle)',
          border: currentDelay > 0 ? '1px solid #bbf7d0' : '1px solid var(--border-subtle)',
          marginBottom: '14px',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
          <span style={{ fontSize: '0.775rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '5px' }}>
            ⏱ Simulated Consumer Latency:
          </span>
          <span
            className={`badge-mono ${currentDelay > 0 ? 'badge-emerald' : 'badge-cyan'}`}
            style={{ fontSize: '0.75rem' }}
          >
            {currentDelay === 0 ? '0ms (Real-time)' : `${currentDelay}ms / msg`}
          </span>
        </div>

        {/* Direct Input & Quick Presets */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap', marginBottom: '8px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
            <input
              type="number"
              min="0"
              max="10000"
              step="50"
              value={delayInput}
              onChange={(e) => setDelayInput(e.target.value)}
              disabled={updatingDelay}
              placeholder="ms"
              style={{
                width: '80px',
                padding: '4px 8px',
                fontSize: '0.8rem',
                fontFamily: 'var(--font-mono)',
                borderRadius: 'var(--radius-sm)',
                border: '1px solid var(--border-subtle)',
              }}
            />
            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>ms</span>
          </div>

          <button
            type="button"
            onClick={() => handleApply()}
            disabled={updatingDelay}
            className="btn-cyber btn-cyan"
            style={{ padding: '4px 10px', fontSize: '0.75rem' }}
          >
            {updatingDelay ? '...' : 'Apply'}
          </button>
        </div>

        {/* Preset Buttons */}
        <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
          {LATENCY_PRESETS.map((preset) => (
            <button
              key={preset}
              type="button"
              onClick={() => handleApply(preset)}
              disabled={updatingDelay}
              className={`btn-cyber ${currentDelay === preset ? 'btn-emerald' : 'btn-ghost'}`}
              style={{ padding: '2px 8px', fontSize: '0.725rem' }}
            >
              {preset === 0 ? '0ms (Realtime)' : `${preset}ms`}
            </button>
          ))}
        </div>
      </div>

      {/* Lag Counter Badge */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          marginBottom: '14px',
          padding: '10px 14px',
          background: isPaused ? '#fef3c7' : 'var(--bg-subtle)',
          borderRadius: 'var(--radius-sm)',
        }}
      >
        <div>
          <span style={{ fontSize: '0.8rem', color: isPaused ? '#92400e' : 'var(--text-muted)', fontWeight: 600 }}>
            Total Unprocessed Lag:
          </span>
          {isPaused && (
            <div style={{ fontSize: '0.7rem', color: '#b45309', fontWeight: 500 }}>
              ⏸ Consumer paused — accumulating backlog
            </div>
          )}
        </div>
        <div style={{ textAlign: 'right' }}>
          <span style={{ fontSize: '1.25rem', fontWeight: 800, color: getAccentColor(), fontFamily: 'var(--font-mono)' }}>
            {lag?.totalLag || '0'} <span style={{ fontSize: '0.75rem', fontWeight: 400, color: 'var(--text-muted)' }}>records</span>
          </span>
        </div>
      </div>

      {/* Per-Partition Lag Breakdown */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
        <span style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase' }}>
          Partition Offsets &amp; Backlog:
        </span>
        {lag?.partitions.map((pl) => {
          const lagNum = Number(pl.lag || '0');
          return (
            <div key={pl.partition} style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--text-body)', marginBottom: '3px' }}>
                <span>P#{pl.partition} (Committed: {pl.committedOffset ?? 'Uncommitted'})</span>
                <span style={{ color: lagNum > 0 ? 'var(--color-amber)' : 'var(--text-muted)', fontWeight: 700 }}>
                  Lag: {pl.lag}
                </span>
              </div>
              <div style={{ height: '6px', background: '#f1f5f9', borderRadius: '3px', overflow: 'hidden' }}>
                <div
                  style={{
                    height: '100%',
                    width: `${Math.min(100, lagNum * 12)}%`,
                    background: lagNum > 0 ? '#d97706' : '#059669',
                    transition: 'width 0.3s ease',
                  }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

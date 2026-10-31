import React from 'react';
import type { SystemBlueprint } from '../../blueprints/systemBlueprints';

interface SimPlaybackBarProps {
  status: 'STOPPED' | 'RUNNING' | 'PAUSED';
  virtualTimeMs: number;
  speedMultiplier: number;
  onPlay: () => void;
  onPause: () => void;
  onStep: () => void;
  onReset: () => void;
  onSetSpeed: (speed: number) => void;
  onDispatchMessage: () => void;
  onDispatchPoisonPill?: () => void;
  onLoadDefaultGraph: () => void;
  blueprints?: SystemBlueprint[];
  activeBlueprintId?: string;
  onOpenBlueprintModal?: () => void;
  onSelectBlueprint?: (blueprintId: string) => void;
}

export const SimPlaybackBar: React.FC<SimPlaybackBarProps> = ({
  status,
  virtualTimeMs,
  speedMultiplier,
  onPlay,
  onPause,
  onStep,
  onReset,
  onSetSpeed,
  onDispatchMessage,
  onDispatchPoisonPill,
  onLoadDefaultGraph,
  blueprints = [],
  activeBlueprintId,
  onOpenBlueprintModal,
  onSelectBlueprint,
}) => {
  const seconds = (virtualTimeMs / 1000).toFixed(2);
  const activeBlueprint = blueprints.find((b) => b.id === activeBlueprintId);

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '10px 16px',
        background: '#ffffff',
        borderBottom: '1px solid #e2e8f0',
        flexWrap: 'wrap',
        gap: '12px',
        boxShadow: '0 1px 4px rgba(0,0,0,0.03)',
      }}
    >
      {/* Left: Playback Controls */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        {status === 'RUNNING' ? (
          <button
            onClick={onPause}
            style={{
              padding: '6px 14px',
              borderRadius: '6px',
              border: '1px solid #f59e0b',
              background: '#fffbeb',
              color: '#d97706',
              fontWeight: 700,
              fontSize: '0.75rem',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
            }}
          >
            <span>⏸</span>
            <span>Pause</span>
          </button>
        ) : (
          <button
            onClick={onPlay}
            style={{
              padding: '6px 14px',
              borderRadius: '6px',
              border: '1px solid #059669',
              background: 'linear-gradient(135deg, #059669, #047857)',
              color: '#ffffff',
              fontWeight: 700,
              fontSize: '0.75rem',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
            }}
          >
            <span>▶</span>
            <span>Run Simulation</span>
          </button>
        )}

        <button
          onClick={onStep}
          style={{
            padding: '6px 12px',
            borderRadius: '6px',
            border: '1px solid #cbd5e1',
            background: '#ffffff',
            color: '#334155',
            fontWeight: 700,
            fontSize: '0.75rem',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
          }}
          title="Advance to next discrete event"
        >
          <span>⏭</span>
          <span>Step (1 Event)</span>
        </button>

        <button
          onClick={onReset}
          style={{
            padding: '6px 12px',
            borderRadius: '6px',
            border: '1px solid #cbd5e1',
            background: '#ffffff',
            color: '#64748b',
            fontWeight: 700,
            fontSize: '0.75rem',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
          }}
        >
          <span>🔄</span>
          <span>Reset</span>
        </button>

        {/* Speed Selector */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '4px', marginLeft: '6px' }}>
          <span style={{ fontSize: '0.675rem', fontWeight: 700, color: '#64748b' }}>Speed:</span>
          {[0.5, 1.0, 2.0].map((s) => (
            <button
              key={s}
              onClick={() => onSetSpeed(s)}
              style={{
                padding: '3px 8px',
                borderRadius: '4px',
                border: speedMultiplier === s ? '1px solid #0284c7' : '1px solid #e2e8f0',
                background: speedMultiplier === s ? '#f0f9ff' : '#ffffff',
                color: speedMultiplier === s ? '#0284c7' : '#64748b',
                fontWeight: 700,
                fontSize: '0.675rem',
                cursor: 'pointer',
              }}
            >
              {s}x
            </button>
          ))}
        </div>
      </div>

      {/* Middle: Virtual Time Clock Badge */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          background: '#f8fafc',
          padding: '4px 12px',
          borderRadius: '6px',
          border: '1px solid #e2e8f0',
          fontFamily: 'var(--font-mono)',
        }}
      >
        <span style={{ fontSize: '0.675rem', color: '#64748b' }}>Virtual Clock:</span>
        <span style={{ fontSize: '0.8rem', fontWeight: 800, color: '#0f172a' }}>{seconds}s</span>
        <span
          style={{
            fontSize: '0.6rem',
            fontWeight: 800,
            padding: '2px 5px',
            borderRadius: '4px',
            background: status === 'RUNNING' ? '#ecfdf5' : '#f1f5f9',
            color: status === 'RUNNING' ? '#059669' : '#64748b',
          }}
        >
          {status}
        </span>
      </div>

      {/* Right: Message Actions & Starters */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        <button
          onClick={onDispatchMessage}
          style={{
            padding: '6px 14px',
            borderRadius: '6px',
            border: '1px solid #0284c7',
            background: 'linear-gradient(135deg, #0284c7, #2563eb)',
            color: '#ffffff',
            fontWeight: 700,
            fontSize: '0.75rem',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            boxShadow: '0 2px 4px rgba(2, 132, 199, 0.25)',
          }}
        >
          <span>📨</span>
          <span>Send 1 Message</span>
        </button>

        {onDispatchPoisonPill && (
          <button
            onClick={onDispatchPoisonPill}
            style={{
              padding: '6px 12px',
              borderRadius: '6px',
              border: '1px solid #f87171',
              background: '#fef2f2',
              color: '#dc2626',
              fontWeight: 700,
              fontSize: '0.75rem',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              boxShadow: '0 2px 4px rgba(220, 38, 38, 0.15)',
            }}
            title="Dispatch a poison-pill payload to test Dead-Letter Queue (DLQ) routing"
          >
            <span>☠️</span>
            <span>Poison Pill</span>
          </button>
        )}

        {/* System Architecture Blueprint Selector */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          {blueprints.length > 0 && onSelectBlueprint && (
            <select
              value={activeBlueprintId || ''}
              onChange={(e) => onSelectBlueprint(e.target.value)}
              style={{
                padding: '6px 10px',
                borderRadius: '6px',
                border: '1px solid #cbd5e1',
                background: '#f8fafc',
                color: '#1e293b',
                fontWeight: 600,
                fontSize: '0.75rem',
                cursor: 'pointer',
                outline: 'none',
              }}
              title="Quick select architectural blueprint"
            >
              {blueprints.map((bp) => (
                <option key={bp.id} value={bp.id}>
                  {bp.icon} {bp.name} ({bp.nodeCount} nodes)
                </option>
              ))}
            </select>
          )}

          <button
            onClick={onOpenBlueprintModal || onLoadDefaultGraph}
            style={{
              padding: '6px 14px',
              borderRadius: '6px',
              border: '1px solid #4f46e5',
              background: 'linear-gradient(135deg, #6366f1, #4f46e5)',
              color: '#ffffff',
              fontWeight: 700,
              fontSize: '0.75rem',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              boxShadow: '0 2px 4px rgba(79, 70, 229, 0.25)',
              transition: 'all 0.15s ease',
            }}
            title="Browse and select system architecture blueprints (Full Production, Minimal, Outbox CDC, Scaling Fleet, DLQ)"
          >
            <span>📐</span>
            <span>Blueprints</span>
            {activeBlueprint && (
              <span
                style={{
                  fontSize: '0.65rem',
                  background: 'rgba(255, 255, 255, 0.25)',
                  padding: '1px 6px',
                  borderRadius: '4px',
                  fontWeight: 700,
                  whiteSpace: 'nowrap',
                }}
              >
                {activeBlueprint.badge}
              </span>
            )}
            <span style={{ fontSize: '0.7rem' }}>▾</span>
          </button>
        </div>
      </div>
    </div>
  );
};

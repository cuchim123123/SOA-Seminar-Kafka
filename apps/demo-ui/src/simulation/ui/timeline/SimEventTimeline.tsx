import React, { useState } from 'react';
import type { SimEvent } from '../../domain/events';

interface SimEventTimelineProps {
  events: SimEvent[];
}

export const SimEventTimeline: React.FC<SimEventTimelineProps> = ({ events }) => {
  const [isOpen, setIsOpen] = useState(true);

  return (
    <div
      style={{
        background: '#ffffff',
        borderTop: '1px solid #e2e8f0',
        display: 'flex',
        flexDirection: 'column',
        height: isOpen ? '160px' : '36px',
        transition: 'height 0.2s ease',
        zIndex: 10,
        boxShadow: '0 -2px 8px rgba(0,0,0,0.03)',
      }}
    >
      {/* Timeline Header Bar */}
      <div
        onClick={() => setIsOpen(!isOpen)}
        style={{
          padding: '8px 16px',
          background: '#f8fafc',
          borderBottom: isOpen ? '1px solid #e2e8f0' : 'none',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          cursor: 'pointer',
          userSelect: 'none',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '0.9rem' }}>⏱️</span>
          <span style={{ fontSize: '0.75rem', fontWeight: 800, color: '#0f172a' }}>
            SIMULATION EVENT TIMELINE
          </span>
          <span
            style={{
              fontSize: '0.65rem',
              fontWeight: 700,
              padding: '1px 6px',
              borderRadius: '10px',
              background: '#e0f2fe',
              color: '#0369a1',
              fontFamily: 'var(--font-mono)',
            }}
          >
            {events.length} events
          </span>
        </div>

        <span style={{ fontSize: '0.75rem', color: '#64748b' }}>
          {isOpen ? '▼ Collapse' : '▲ Expand'}
        </span>
      </div>

      {/* Events Stream */}
      {isOpen && (
        <div
          style={{
            flex: 1,
            overflowY: 'auto',
            padding: '8px 16px',
            fontFamily: 'var(--font-mono)',
            fontSize: '0.7rem',
            display: 'flex',
            flexDirection: 'column',
            gap: '6px',
          }}
        >
          {events.length === 0 ? (
            <div style={{ color: '#94a3b8', textAlign: 'center', padding: '16px 0' }}>
              No simulation events yet. Click &quot;Send 1 Message&quot; to begin.
            </div>
          ) : (
            events.map((ev) => {
              const seconds = (ev.scheduledTime / 1000).toFixed(3);

              const badgeColor =
                ev.type === 'DISPATCH_MESSAGE'
                  ? '#38bdf8'
                  : ev.type === 'APPEND_PARTITION'
                    ? '#a855f7'
                    : ev.type === 'START_PROCESSING'
                      ? '#fbbf24'
                      : ev.type === 'COMPLETE_PROCESSING'
                        ? '#059669'
                        : '#10b981';

              return (
                <div
                  key={ev.id}
                  style={{
                    display: 'flex',
                    alignItems: 'baseline',
                    gap: '10px',
                    borderBottom: '1px solid #f8fafc',
                    paddingBottom: '4px',
                  }}
                >
                  <span style={{ color: '#64748b', minWidth: '60px' }}>{seconds}s</span>
                  <span
                    style={{
                      fontSize: '0.625rem',
                      fontWeight: 700,
                      padding: '1px 5px',
                      borderRadius: '3px',
                      background: `${badgeColor}22`,
                      color: badgeColor,
                      border: `1px solid ${badgeColor}44`,
                      minWidth: '85px',
                      textAlign: 'center',
                    }}
                  >
                    {ev.type}
                  </span>
                  <span style={{ color: '#334155', flex: 1 }}>{ev.description}</span>
                </div>
              );
            })
          )}
        </div>
      )}
    </div>
  );
};

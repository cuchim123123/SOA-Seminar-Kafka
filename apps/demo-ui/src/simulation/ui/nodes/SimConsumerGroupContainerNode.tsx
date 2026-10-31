import React from 'react';
import type { NodeProps } from '@xyflow/react';
import type { SimConsumerGroupContainerData } from '../../domain/types';

export const SimConsumerGroupContainerNode: React.FC<NodeProps> = ({ data }) => {
  const d = data as unknown as SimConsumerGroupContainerData;
  const groupId = d.groupId || 'consumer-group';
  const memberCount = d.memberCount || 1;
  const assignedPartitions = d.assignedPartitions || [];
  const assignorStrategy = d.assignorStrategy || 'range';
  const isPaused = Boolean(d.isPaused);

  const partitionsText =
    assignedPartitions.length > 0
      ? assignedPartitions.map((p) => `P${p}`).join(', ')
      : 'None (Unassigned)';

  // Theme accent based on group name
  const isLoyalty = groupId.includes('loyalty');
  const isAnalytics = groupId.includes('analytics');
  const accentColor = isLoyalty ? '#059669' : isAnalytics ? '#0284c7' : '#7c3aed';

  return (
    <div
      style={{
        width: '100%',
        height: '100%',
        background: `${accentColor}0d`,
        borderRadius: '16px',
        border: `2px dashed ${accentColor}88`,
        boxShadow: 'none',
        padding: '12px 18px',
        boxSizing: 'border-box',
        cursor: 'grab',
        userSelect: 'none',
        position: 'relative',
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'space-between',
        fontFamily: 'var(--font-sans, system-ui, sans-serif)',
      }}
    >
      {/* Container Header Bar */}
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'flex-start',
          gap: '6px',
          borderBottom: '1px solid #e2e8f0',
          paddingBottom: '8px',
          background: 'transparent',
        }}
      >
        {/* Left: Group Identity */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div
            style={{
              width: '26px',
              height: '26px',
              borderRadius: '6px',
              background: `${accentColor}18`,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              fontSize: '0.85rem',
            }}
          >
            👥
          </div>
          <div>
            <div
              style={{
                fontSize: '0.825rem',
                fontWeight: 800,
                color: '#0f172a',
                letterSpacing: '-0.01em',
                whiteSpace: 'nowrap',
              }}
            >
              Consumer Group: <span style={{ color: accentColor }}>{groupId}</span>
            </div>
          </div>
        </div>

        {/* Right: Badges */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap' }}>
          {isPaused && (
            <span
              style={{
                fontSize: '0.65rem',
                fontWeight: 700,
                padding: '2px 8px',
                borderRadius: '9999px',
                background: '#fef3c7',
                color: '#d97706',
                border: '1px solid #fde68a',
              }}
            >
              ⏸️ GROUP PAUSED
            </span>
          )}

          <span
            style={{
              fontSize: '0.65rem',
              fontWeight: 700,
              padding: '2px 8px',
              borderRadius: '9999px',
              background: '#ecfdf5',
              color: '#059669',
              border: '1px solid #a7f3d0',
            }}
          >
            {memberCount} Active Worker{memberCount > 1 ? 's' : ''}
          </span>

          <span
            style={{
              fontSize: '0.65rem',
              fontWeight: 700,
              padding: '2px 8px',
              borderRadius: '9999px',
              background: '#f1f5f9',
              color: '#334155',
              border: '1px solid #cbd5e1',
            }}
          >
            Assigned: [{partitionsText}]
          </span>

          <span
            style={{
              fontSize: '0.65rem',
              fontWeight: 700,
              padding: '2px 8px',
              borderRadius: '9999px',
              background: '#f0f9ff',
              color: '#0284c7',
              border: '1px solid #bae6fd',
            }}
          >
            Assignor: {assignorStrategy}
          </span>
        </div>
      </div>

      {/* Subtle bottom watermark label */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'flex-end',
          alignItems: 'center',
          marginTop: 'auto',
          paddingTop: '6px',
        }}
      >
        <span
          style={{
            fontSize: '0.6rem',
            fontWeight: 800,
            textTransform: 'uppercase',
            letterSpacing: '0.08em',
            color: '#94a3b8',
            opacity: 0.8,
          }}
        >
          KAFKA GROUP OFFSET COMMIT DOMAIN (INDEPENDENT HIGH-WATERMARK)
        </span>
      </div>
    </div>
  );
};

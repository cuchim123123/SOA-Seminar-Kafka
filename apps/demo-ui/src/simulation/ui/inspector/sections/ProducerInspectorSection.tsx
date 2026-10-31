import React from 'react';
import type { SimCustomNode } from '../../../domain/types';

interface ProducerInspectorSectionProps {
  selectedNode: SimCustomNode;
  onUpdateData: (nodeId: string, partial: Record<string, unknown>) => void;
}

export const ProducerInspectorSection: React.FC<ProducerInspectorSectionProps> = ({
  selectedNode,
  onUpdateData,
}) => {
  const { id, data } = selectedNode;

  return (
    <>
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
          <span style={{ fontSize: '0.625rem', color: '#64748b', alignSelf: 'center' }}>
            Presets:
          </span>
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
        <label
          style={{
            display: 'block',
            fontSize: '0.725rem',
            fontWeight: 700,
            color: '#475569',
            marginBottom: '4px',
          }}
        >
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
  );
};

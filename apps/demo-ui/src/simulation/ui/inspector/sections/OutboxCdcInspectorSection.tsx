import React from 'react';
import type { SimCustomNode } from '../../../domain/types';

interface OutboxCdcInspectorSectionProps {
  selectedNode: SimCustomNode;
  onUpdateData: (nodeId: string, partial: Record<string, unknown>) => void;
}

export const OutboxCdcInspectorSection: React.FC<OutboxCdcInspectorSectionProps> = ({
  selectedNode,
  onUpdateData,
}) => {
  const { id, data } = selectedNode;
  const kind = data.kind;

  if (kind === 'outbox-table') {
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

        <div
          style={{
            background: '#fff7ed',
            border: '1px solid #fdba74',
            borderRadius: '8px',
            padding: '10px',
          }}
        >
          <div
            style={{
              fontSize: '0.725rem',
              fontWeight: 800,
              color: '#c2410c',
              marginBottom: '6px',
            }}
          >
            OUTBOX BUFFER STATUS:
          </div>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              marginBottom: '4px',
              fontSize: '0.75rem',
            }}
          >
            <span style={{ color: '#64748b' }}>Pending Rows:</span>
            <span
              style={{
                fontWeight: 800,
                color: '#ea580c',
                fontFamily: 'var(--font-mono)',
              }}
            >
              {Number(data.pendingRecordsCount) || 0}
            </span>
          </div>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              fontSize: '0.75rem',
            }}
          >
            <span style={{ color: '#64748b' }}>Published to Kafka:</span>
            <span
              style={{
                fontWeight: 700,
                color: '#16a34a',
                fontFamily: 'var(--font-mono)',
              }}
            >
              {Number(data.publishedRecordsCount) || 0}
            </span>
          </div>
        </div>

        <div
          style={{
            background: '#f8fafc',
            padding: '10px',
            borderRadius: '6px',
            border: '1px solid #e2e8f0',
            fontSize: '0.675rem',
            color: '#64748b',
            lineHeight: 1.4,
          }}
        >
          💡 <strong>Transactional Outbox Pattern</strong>: Dual-write atomicity between database
          operations and Kafka message publishing without two-phase commit (2PC). Events are
          buffered in this table and tailed by the CDC relay model.
        </div>
      </>
    );
  }

  if (kind === 'cdc-relay') {
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
            CDC Connector Engine:
          </label>
          <p className="sim-muted">
            Debezium CDC (simplified model). This does not launch a real connector.
          </p>
        </div>
        <p className="sim-muted">
          Fixed transfer timing is simulated. Poll interval and batch-size settings do not control
          this model; no polling publisher is provided.
        </p>
        <label>
          Target Kafka topic
          <input
            aria-label="CDC target topic"
            value={String(data.targetTopic ?? 'purchases')}
            onChange={(event) => onUpdateData(id, { targetTopic: event.target.value })}
          />
        </label>

        <div
          style={{
            background: '#ecfeff',
            border: '1px solid #a5f3fc',
            borderRadius: '8px',
            padding: '10px',
          }}
        >
          <div
            style={{
              fontSize: '0.725rem',
              fontWeight: 800,
              color: '#0e7490',
              marginBottom: '6px',
            }}
          >
            CDC STREAMING METRICS:
          </div>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              fontSize: '0.75rem',
            }}
          >
            <span style={{ color: '#64748b' }}>Events Streamed:</span>
            <span
              style={{
                fontWeight: 800,
                color: '#0891b2',
                fontFamily: 'var(--font-mono)',
              }}
            >
              {Number(data.streamedCount) || 0}
            </span>
          </div>
        </div>
      </>
    );
  }

  return null;
};

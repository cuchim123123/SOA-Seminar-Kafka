import React from 'react';
import type { SimCustomNode } from '../../../domain/types';

interface DatabaseDlqInspectorSectionProps {
  selectedNode: SimCustomNode;
  onUpdateData: (nodeId: string, partial: Record<string, unknown>) => void;
}

export const DatabaseDlqInspectorSection: React.FC<DatabaseDlqInspectorSectionProps> = ({
  selectedNode,
  onUpdateData,
}) => {
  const { id, data } = selectedNode;
  const kind = data.kind;

  if (kind === 'dead-letter-queue') {
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

        <div
          style={{
            background: '#fef2f2',
            border: '1px solid #fecaca',
            borderRadius: '8px',
            padding: '10px',
          }}
        >
          <div
            style={{
              fontSize: '0.725rem',
              fontWeight: 800,
              color: '#b91c1c',
              marginBottom: '6px',
            }}
          >
            ISOLATED POISON-PILL MESSAGES:
          </div>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              fontSize: '0.85rem',
            }}
          >
            <span style={{ color: '#64748b' }}>Dead Letters:</span>
            <span
              style={{
                fontWeight: 800,
                color: '#dc2626',
                fontFamily: 'var(--font-mono)',
              }}
            >
              {Number(data.deadLetterCount) || 0}
            </span>
          </div>
        </div>
      </>
    );
  }

  if (kind === 'database') {
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
            Storage Engine / Projection Model:
          </label>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px', marginBottom: '8px' }}>
            {[
              {
                id: 'postgres',
                label: 'PostgreSQL',
                icon: '🗄️',
                engine: 'PostgreSQL 16',
                defTitle: 'PostgreSQL DB',
              },
              {
                id: 'mongo',
                label: 'MongoDB',
                icon: '🍃',
                engine: 'MongoDB 7',
                defTitle: 'MongoDB Projection',
              },
              {
                id: 'redis',
                label: 'Redis',
                icon: '⚡',
                engine: 'Redis 7',
                defTitle: 'Redis Cache Sink',
              },
              {
                id: 'elastic',
                label: 'Elasticsearch',
                icon: '🔍',
                engine: 'Elasticsearch 8',
                defTitle: 'Elasticsearch Index',
              },
              {
                id: 'clickhouse',
                label: 'ClickHouse',
                icon: '📊',
                engine: 'ClickHouse OLAP',
                defTitle: 'ClickHouse Analytics',
              },
            ].map((eng) => {
              const isSelected =
                (data.engine as string)?.includes(eng.id) ||
                (data.engine as string) === eng.engine;

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
          <label
            style={{
              display: 'block',
              fontSize: '0.725rem',
              fontWeight: 700,
              color: '#475569',
              marginBottom: '4px',
            }}
          >
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

        <div
          style={{
            background: '#f8fafc',
            padding: '8px 10px',
            borderRadius: '6px',
            border: '1px solid #e2e8f0',
          }}
        >
          <div style={{ fontSize: '0.675rem', color: '#64748b', marginBottom: '4px' }}>
            Committed Records:
          </div>
          <div
            style={{
              fontSize: '0.85rem',
              fontWeight: 800,
              color: '#059669',
              fontFamily: 'var(--font-mono)',
            }}
          >
            {Number(data.committedRecordCount) || 0}
          </div>
        </div>
      </>
    );
  }

  return null;
};

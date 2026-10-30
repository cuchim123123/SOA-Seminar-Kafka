import React from 'react';
import type { BrokerMessageRecord } from '../../api/client';

export interface PurchaseDetails {
  purchaseId: string | null;
  eventType: string;
  isInvalidated: boolean;
  customerSeq: string | null;
  amountCents: number | null;
  reason: string | null;
}

interface PartitionMessageTableProps {
  messages: BrokerMessageRecord[];
  selectedOffsets: string[];
  selectableOffsets: string[];
  allSelectableChecked: boolean;
  expandedOffset: string | null;
  isInvalidating: boolean;
  onToggleSelectAll: () => void;
  onToggleSelectOffset: (offset: string) => void;
  onToggleExpandOffset: (offset: string) => void;
  onSingleInvalidate: (purchaseId: string) => void;
  onCopy: (text: string, label: string) => void;
  getPurchaseDetails: (msg: BrokerMessageRecord) => PurchaseDetails;
}

export const PartitionMessageTable: React.FC<PartitionMessageTableProps> = ({
  messages,
  selectedOffsets,
  selectableOffsets,
  allSelectableChecked,
  expandedOffset,
  isInvalidating,
  onToggleSelectAll,
  onToggleSelectOffset,
  onToggleExpandOffset,
  onSingleInvalidate,
  onCopy,
  getPurchaseDetails,
}) => {
  return (
    <div
      style={{
        overflowX: 'auto',
        border: '1px solid var(--border-subtle)',
        borderRadius: 'var(--radius-md)',
      }}
    >
      <table
        style={{
          width: '100%',
          borderCollapse: 'collapse',
          fontSize: '0.8rem',
          textAlign: 'left',
          fontFamily: 'var(--font-sans)',
        }}
      >
        <thead>
          <tr
            style={{
              background: 'var(--bg-subtle)',
              borderBottom: '1px solid var(--border-subtle)',
              color: 'var(--text-muted)',
              fontSize: '0.75rem',
              textTransform: 'uppercase',
              letterSpacing: '0.04em',
            }}
          >
            <th style={{ width: '42px', padding: '10px 10px', textAlign: 'center' }}>
              <input
                type="checkbox"
                aria-label="Select all completed purchases on partition"
                title={
                  selectableOffsets.length > 0
                    ? 'Select / Deselect all completed purchases'
                    : 'No completed purchases available to select'
                }
                checked={allSelectableChecked}
                onChange={onToggleSelectAll}
                disabled={selectableOffsets.length === 0}
                style={{
                  cursor: selectableOffsets.length > 0 ? 'pointer' : 'not-allowed',
                  width: '15px',
                  height: '15px',
                  accentColor: '#e11d48',
                }}
              />
            </th>
            <th style={{ padding: '10px 12px' }}>Offset</th>
            <th style={{ padding: '10px 12px' }}>Key (CustomerId)</th>
            <th style={{ padding: '10px 12px' }}>Event Type</th>
            <th style={{ padding: '10px 12px' }}>Timestamp</th>
            <th style={{ padding: '10px 12px' }}>Payload Summary</th>
            <th style={{ padding: '10px 14px', textAlign: 'right' }}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {messages.map((msg) => {
            const {
              purchaseId,
              isInvalidated,
              customerSeq,
              amountCents,
              reason,
            } = getPurchaseDetails(msg);
            const isSelected = selectedOffsets.includes(msg.offset);
            const isExpanded = expandedOffset === msg.offset;

            let timeDisplay = msg.timestamp;
            try {
              const d = new Date(Number(msg.timestamp));
              timeDisplay = !isNaN(d.getTime()) ? d.toLocaleTimeString() : msg.timestamp;
            } catch {
              // ignore
            }

            return (
              <React.Fragment key={msg.offset}>
                <tr
                  style={{
                    borderBottom: '1px solid var(--border-subtle)',
                    background: isSelected
                      ? '#fff1f2'
                      : isExpanded
                        ? '#f8fafc'
                        : isInvalidated
                          ? '#fffbeb'
                          : '#ffffff',
                    borderLeft: isSelected
                      ? '3px solid #e11d48'
                      : isInvalidated
                        ? '3px solid #f59e0b'
                        : '3px solid transparent',
                    transition: 'background 0.15s ease',
                  }}
                >
                  {/* Checkbox column */}
                  <td style={{ textAlign: 'center', padding: '10px 10px' }}>
                    {purchaseId && !isInvalidated ? (
                      <input
                        type="checkbox"
                        aria-label={`Select message at offset ${msg.offset}`}
                        checked={isSelected}
                        onChange={() => onToggleSelectOffset(msg.offset)}
                        style={{
                          cursor: 'pointer',
                          width: '15px',
                          height: '15px',
                          accentColor: '#e11d48',
                        }}
                      />
                    ) : (
                      <span style={{ color: 'var(--text-dim)', fontSize: '0.75rem' }}>-</span>
                    )}
                  </td>

                  <td
                    style={{
                      padding: '10px 12px',
                      fontFamily: 'var(--font-mono)',
                      fontWeight: 600,
                    }}
                  >
                    <span
                      className="badge-mono badge-cyan"
                      onClick={() => onCopy(msg.offset, 'Offset')}
                      title="Click to copy offset"
                      style={{ cursor: 'pointer' }}
                    >
                      #{msg.offset}
                    </span>
                  </td>
                  <td
                    style={{
                      padding: '10px 12px',
                      fontFamily: 'var(--font-mono)',
                      fontWeight: 600,
                    }}
                  >
                    {msg.key ? (
                      <span
                        onClick={() => onCopy(msg.key!, 'Customer ID')}
                        title="Click to copy Customer ID"
                        style={{
                          color: '#0284c7',
                          cursor: 'pointer',
                          textDecoration: 'underline dotted',
                        }}
                      >
                        {msg.key}
                      </span>
                    ) : (
                      <span style={{ color: 'var(--text-dim)' }}>[Null Key]</span>
                    )}
                  </td>
                  <td style={{ padding: '10px 12px' }}>
                    <span
                      className={`badge-mono ${isInvalidated ? 'badge-amber' : 'badge-emerald'}`}
                    >
                      {isInvalidated ? '↩ PurchaseInvalidated' : '✓ PurchaseCompleted'}
                    </span>
                  </td>
                  <td
                    style={{
                      padding: '10px 12px',
                      color: 'var(--text-muted)',
                      fontSize: '0.75rem',
                      fontFamily: 'var(--font-mono)',
                    }}
                  >
                    {timeDisplay}
                  </td>
                  <td
                    style={{
                      padding: '10px 12px',
                      color: 'var(--text-body)',
                      fontSize: '0.775rem',
                    }}
                  >
                    <span>
                      {customerSeq && (
                        <span style={{ marginRight: '6px' }}>
                          Seq{' '}
                          <strong style={{ fontFamily: 'var(--font-mono)' }}>
                            #{customerSeq}
                          </strong>
                        </span>
                      )}
                      {amountCents !== null && (
                        <span style={{ color: 'var(--color-emerald)', fontWeight: 600, marginRight: '6px' }}>
                          ${(amountCents / 100).toFixed(2)}
                        </span>
                      )}
                      {reason && (
                        <span style={{ color: 'var(--color-amber)', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
                          [{reason}]
                        </span>
                      )}
                      {purchaseId && (
                        <span
                          onClick={() => onCopy(purchaseId, 'Purchase ID')}
                          title="Click to copy Purchase ID"
                          style={{
                            fontSize: '0.7rem',
                            color: 'var(--text-muted)',
                            fontFamily: 'var(--font-mono)',
                            marginLeft: '4px',
                            cursor: 'pointer',
                          }}
                        >
                          ({purchaseId.slice(0, 14)}...)
                        </span>
                      )}
                    </span>
                  </td>
                  <td style={{ padding: '10px 14px', textAlign: 'right', whiteSpace: 'nowrap' }}>
                    {purchaseId && !isInvalidated ? (
                      <button
                        type="button"
                        disabled={isInvalidating}
                        onClick={() => onSingleInvalidate(purchaseId)}
                        style={{
                          padding: '4px 10px',
                          borderRadius: 'var(--radius-sm)',
                          border: '1px solid #fecdd3',
                          background: '#fff1f2',
                          color: '#e11d48',
                          fontSize: '0.725rem',
                          fontWeight: 600,
                          cursor: isInvalidating ? 'not-allowed' : 'pointer',
                          marginRight: '6px',
                          display: 'inline-flex',
                          alignItems: 'center',
                          gap: '3px',
                          transition: 'all 0.15s ease',
                        }}
                        title={`1-click Invalidate purchase ${purchaseId}`}
                      >
                        <span>↩</span> Invalidate
                      </button>
                    ) : isInvalidated ? (
                      <span
                        className="badge-mono badge-amber"
                        style={{ marginRight: '6px', fontSize: '0.675rem' }}
                      >
                        Invalidated
                      </span>
                    ) : null}

                    <button
                      type="button"
                      onClick={() => onToggleExpandOffset(msg.offset)}
                      style={{
                        padding: '4px 10px',
                        borderRadius: 'var(--radius-sm)',
                        border: '1px solid var(--border-medium)',
                        background: '#ffffff',
                        color: 'var(--color-primary)',
                        fontSize: '0.725rem',
                        fontWeight: 600,
                        cursor: 'pointer',
                      }}
                    >
                      {isExpanded ? 'Hide' : 'Inspect'}
                    </button>
                  </td>
                </tr>

                {isExpanded && (
                  <tr
                    style={{
                      background: '#f8fafc',
                      borderBottom: '1px solid var(--border-subtle)',
                    }}
                  >
                    <td colSpan={7} style={{ padding: '14px 18px' }}>
                      <div
                        style={{
                          display: 'flex',
                          justifyContent: 'space-between',
                          alignItems: 'center',
                          marginBottom: '8px',
                        }}
                      >
                        <span
                          style={{
                            fontSize: '0.75rem',
                            fontWeight: 700,
                            color: 'var(--text-muted)',
                            fontFamily: 'var(--font-mono)',
                          }}
                        >
                          RECORD #{msg.offset} (PARTITION #{msg.partition}) — FULL KAFKA ENVELOPE
                        </span>
                        <button
                          type="button"
                          onClick={() =>
                            onCopy(
                              JSON.stringify(msg.payload, null, 2),
                              'Full Envelope JSON',
                            )
                          }
                          style={{
                            fontSize: '0.725rem',
                            padding: '3px 10px',
                            borderRadius: 'var(--radius-sm)',
                            border: '1px solid var(--border-medium)',
                            background: '#ffffff',
                            cursor: 'pointer',
                            fontWeight: 600,
                          }}
                        >
                          📋 Copy JSON
                        </button>
                      </div>

                      <pre
                        style={{
                          background: '#0f172a',
                          color: '#38bdf8',
                          padding: '12px 14px',
                          borderRadius: 'var(--radius-sm)',
                          fontSize: '0.75rem',
                          fontFamily: 'var(--font-mono)',
                          overflowX: 'auto',
                          margin: 0,
                          maxHeight: '260px',
                        }}
                      >
                        {JSON.stringify(msg.payload, null, 2)}
                      </pre>
                    </td>
                  </tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};

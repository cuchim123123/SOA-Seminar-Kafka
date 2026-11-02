import React, { useState } from 'react';
import type { SimCustomNode, SimConsumerData } from '../../domain/types';

interface PoisonPillModalProps {
  isOpen: boolean;
  onClose: () => void;
  consumerNodes: SimCustomNode[];
  onInject: (options: {
    targetFailedConsumerIds?: string[];
    targetFailedGroupIds?: string[];
  }) => void;
}

export const PoisonPillModal: React.FC<PoisonPillModalProps> = ({
  isOpen,
  onClose,
  consumerNodes,
  onInject,
}) => {
  const [selectedTargetType, setSelectedTargetType] = useState<
    'ALL' | 'SPECIFIC_CONSUMERS' | 'SPECIFIC_GROUPS'
  >('SPECIFIC_CONSUMERS');
  const [selectedConsumerIds, setSelectedConsumerIds] = useState<string[]>([]);
  const [selectedGroupIds, setSelectedGroupIds] = useState<string[]>([]);

  if (!isOpen) return null;

  // Extract distinct consumer groups
  const groups = Array.from(
    new Set(
      consumerNodes.map((n) => {
        const d = n.data as SimConsumerData;
        return d.groupId || 'loyalty-v1';
      }),
    ),
  );

  const toggleConsumer = (id: string) => {
    setSelectedConsumerIds((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id],
    );
  };

  const toggleGroup = (groupId: string) => {
    setSelectedGroupIds((prev) =>
      prev.includes(groupId) ? prev.filter((x) => x !== groupId) : [...prev, groupId],
    );
  };

  const handleConfirm = () => {
    if (selectedTargetType === 'SPECIFIC_CONSUMERS') {
      if (selectedConsumerIds.length === 0) {
        alert('Please select at least one worker to fail on, or choose "All Workers".');
        return;
      }
      onInject({ targetFailedConsumerIds: selectedConsumerIds });
    } else if (selectedTargetType === 'SPECIFIC_GROUPS') {
      if (selectedGroupIds.length === 0) {
        alert('Please select at least one consumer group to fail on, or choose "All Workers".');
        return;
      }
      onInject({ targetFailedGroupIds: selectedGroupIds });
    } else {
      onInject({});
    }
    onClose();
  };

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        backgroundColor: 'rgba(15, 23, 42, 0.65)',
        backdropFilter: 'blur(3px)',
        zIndex: 9999,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '16px',
      }}
      onClick={onClose}
    >
      <div
        style={{
          background: '#ffffff',
          borderRadius: '16px',
          boxShadow: '0 20px 25px -5px rgba(0, 0, 0, 0.2), 0 10px 10px -5px rgba(0, 0, 0, 0.1)',
          maxWidth: '520px',
          width: '100%',
          overflow: 'hidden',
          border: '1px solid #f87171',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div
          style={{
            background: 'linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%)',
            padding: '16px 20px',
            borderBottom: '1px solid #fecaca',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <span style={{ fontSize: '1.5rem' }}>☠️</span>
            <div>
              <h3 style={{ margin: 0, fontSize: '1.1rem', fontWeight: 800, color: '#991b1b' }}>
                Inject Poison Pill Record
              </h3>
              <p style={{ margin: 0, fontSize: '0.75rem', color: '#b91c1c' }}>
                Select target worker(s) to simulate schema/deserialization failure
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            style={{
              background: 'transparent',
              border: 'none',
              fontSize: '1.25rem',
              color: '#991b1b',
              cursor: 'pointer',
              lineHeight: 1,
            }}
          >
            ✕
          </button>
        </div>

        {/* Content */}
        <div style={{ padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
          <div
            style={{
              fontSize: '0.8rem',
              color: '#475569',
              background: '#f8fafc',
              border: '1px solid #e2e8f0',
              padding: '10px 12px',
              borderRadius: '8px',
              lineHeight: 1.4,
            }}
          >
            💡 <strong>Kafka Mechanics Insight:</strong> A record may fail processing in one
            consumer group (e.g. invalid points formula for Loyalty) while succeeding in another
            (e.g. gross revenue aggregation for Analytics).
          </div>

          {/* Mode Selector */}
          <div>
            <label
              style={{
                display: 'block',
                fontSize: '0.75rem',
                fontWeight: 700,
                color: '#334155',
                marginBottom: '8px',
              }}
            >
              Target Failure Scope:
            </label>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '8px' }}>
              <button
                type="button"
                onClick={() => setSelectedTargetType('SPECIFIC_CONSUMERS')}
                style={{
                  padding: '8px 10px',
                  borderRadius: '8px',
                  border:
                    selectedTargetType === 'SPECIFIC_CONSUMERS'
                      ? '2px solid #ef4444'
                      : '1px solid #cbd5e1',
                  background: selectedTargetType === 'SPECIFIC_CONSUMERS' ? '#fef2f2' : '#ffffff',
                  color: selectedTargetType === 'SPECIFIC_CONSUMERS' ? '#991b1b' : '#475569',
                  fontWeight: selectedTargetType === 'SPECIFIC_CONSUMERS' ? 800 : 600,
                  fontSize: '0.75rem',
                  cursor: 'pointer',
                }}
              >
                Specific Worker(s)
              </button>
              <button
                type="button"
                onClick={() => setSelectedTargetType('SPECIFIC_GROUPS')}
                style={{
                  padding: '8px 10px',
                  borderRadius: '8px',
                  border:
                    selectedTargetType === 'SPECIFIC_GROUPS'
                      ? '2px solid #ef4444'
                      : '1px solid #cbd5e1',
                  background: selectedTargetType === 'SPECIFIC_GROUPS' ? '#fef2f2' : '#ffffff',
                  color: selectedTargetType === 'SPECIFIC_GROUPS' ? '#991b1b' : '#475569',
                  fontWeight: selectedTargetType === 'SPECIFIC_GROUPS' ? 800 : 600,
                  fontSize: '0.75rem',
                  cursor: 'pointer',
                }}
              >
                Consumer Group
              </button>
              <button
                type="button"
                onClick={() => setSelectedTargetType('ALL')}
                style={{
                  padding: '8px 10px',
                  borderRadius: '8px',
                  border: selectedTargetType === 'ALL' ? '2px solid #ef4444' : '1px solid #cbd5e1',
                  background: selectedTargetType === 'ALL' ? '#fef2f2' : '#ffffff',
                  color: selectedTargetType === 'ALL' ? '#991b1b' : '#475569',
                  fontWeight: selectedTargetType === 'ALL' ? 800 : 600,
                  fontSize: '0.75rem',
                  cursor: 'pointer',
                }}
              >
                All Workers (Global)
              </button>
            </div>
          </div>

          {/* Worker List */}
          {selectedTargetType === 'SPECIFIC_CONSUMERS' && (
            <div>
              <label
                style={{
                  display: 'block',
                  fontSize: '0.75rem',
                  fontWeight: 700,
                  color: '#334155',
                  marginBottom: '8px',
                }}
              >
                Select Active Worker(s) to Fail:
              </label>
              {consumerNodes.length === 0 ? (
                <div
                  style={{
                    fontSize: '0.75rem',
                    color: '#94a3b8',
                    fontStyle: 'italic',
                    padding: '12px',
                    textAlign: 'center',
                  }}
                >
                  No consumer worker nodes currently on canvas. Add a consumer from the palette
                  first.
                </div>
              ) : (
                <div
                  style={{
                    maxHeight: '180px',
                    overflowY: 'auto',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '6px',
                  }}
                >
                  {consumerNodes.map((node) => {
                    const data = node.data as SimConsumerData;
                    const isSelected = selectedConsumerIds.includes(node.id);
                    return (
                      <div
                        key={node.id}
                        onClick={() => toggleConsumer(node.id)}
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'space-between',
                          padding: '8px 12px',
                          borderRadius: '8px',
                          border: isSelected ? '1.5px solid #ef4444' : '1px solid #e2e8f0',
                          background: isSelected ? '#fef2f2' : '#ffffff',
                          cursor: 'pointer',
                          transition: 'all 0.15s ease',
                        }}
                      >
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={() => {}} // Handled by container onClick
                            style={{ cursor: 'pointer', accentColor: '#ef4444' }}
                          />
                          <div>
                            <div style={{ fontSize: '0.8rem', fontWeight: 700, color: '#1e293b' }}>
                              {data.title || node.id}
                            </div>
                            <div
                              style={{
                                fontSize: '0.7rem',
                                color: '#64748b',
                                fontFamily: 'var(--font-mono)',
                              }}
                            >
                              Group: <strong>{data.groupId || 'loyalty-v1'}</strong> • Node ID:{' '}
                              {node.id}
                            </div>
                          </div>
                        </div>
                        <span
                          style={{
                            fontSize: '0.65rem',
                            padding: '2px 6px',
                            borderRadius: '4px',
                            background: isSelected ? '#fee2e2' : '#f1f5f9',
                            color: isSelected ? '#b91c1c' : '#475569',
                            fontWeight: 700,
                          }}
                        >
                          {isSelected ? 'WILL FAIL' : 'PASS'}
                        </span>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          )}

          {/* Group List */}
          {selectedTargetType === 'SPECIFIC_GROUPS' && (
            <div>
              <label
                style={{
                  display: 'block',
                  fontSize: '0.75rem',
                  fontWeight: 700,
                  color: '#334155',
                  marginBottom: '8px',
                }}
              >
                Select Consumer Group(s) to Fail:
              </label>
              {groups.length === 0 ? (
                <div
                  style={{
                    fontSize: '0.75rem',
                    color: '#94a3b8',
                    fontStyle: 'italic',
                    padding: '12px',
                    textAlign: 'center',
                  }}
                >
                  No consumer groups currently on canvas.
                </div>
              ) : (
                <div
                  style={{
                    maxHeight: '180px',
                    overflowY: 'auto',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '6px',
                  }}
                >
                  {groups.map((grp) => {
                    const isSelected = selectedGroupIds.includes(grp);
                    const memberCount = consumerNodes.filter(
                      (n) => (n.data as SimConsumerData).groupId === grp,
                    ).length;
                    return (
                      <div
                        key={grp}
                        onClick={() => toggleGroup(grp)}
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'space-between',
                          padding: '8px 12px',
                          borderRadius: '8px',
                          border: isSelected ? '1.5px solid #ef4444' : '1px solid #e2e8f0',
                          background: isSelected ? '#fef2f2' : '#ffffff',
                          cursor: 'pointer',
                        }}
                      >
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={() => {}}
                            style={{ cursor: 'pointer', accentColor: '#ef4444' }}
                          />
                          <div>
                            <div style={{ fontSize: '0.8rem', fontWeight: 700, color: '#1e293b' }}>
                              {grp}
                            </div>
                            <div style={{ fontSize: '0.7rem', color: '#64748b' }}>
                              {memberCount} worker instance{memberCount > 1 ? 's' : ''} in group
                            </div>
                          </div>
                        </div>
                        <span
                          style={{
                            fontSize: '0.65rem',
                            padding: '2px 6px',
                            borderRadius: '4px',
                            background: isSelected ? '#fee2e2' : '#f1f5f9',
                            color: isSelected ? '#b91c1c' : '#475569',
                            fontWeight: 700,
                          }}
                        >
                          {isSelected ? 'GROUP FAILS' : 'GROUP PASSES'}
                        </span>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          )}

          {selectedTargetType === 'ALL' && (
            <div
              style={{
                background: '#fff1f2',
                border: '1px dashed #f43f5e',
                borderRadius: '8px',
                padding: '12px',
                fontSize: '0.75rem',
                color: '#be123c',
                lineHeight: 1.4,
              }}
            >
              ⚠️ <strong>Global Poison Pill:</strong> Any consumer worker without a Dead-Letter
              Queue (DLQ) will experience Head-of-Line Blocking and freeze its assigned partition
              offset.
            </div>
          )}
        </div>

        {/* Footer */}
        <div
          style={{
            padding: '12px 20px',
            background: '#f8fafc',
            borderTop: '1px solid #e2e8f0',
            display: 'flex',
            justifyContent: 'flex-end',
            gap: '8px',
          }}
        >
          <button
            type="button"
            onClick={onClose}
            style={{
              padding: '8px 14px',
              borderRadius: '8px',
              border: '1px solid #cbd5e1',
              background: '#ffffff',
              color: '#475569',
              fontWeight: 600,
              fontSize: '0.75rem',
              cursor: 'pointer',
            }}
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={handleConfirm}
            style={{
              padding: '8px 18px',
              borderRadius: '8px',
              border: 'none',
              background: 'linear-gradient(135deg, #ef4444 0%, #dc2626 100%)',
              color: '#ffffff',
              fontWeight: 700,
              fontSize: '0.75rem',
              cursor: 'pointer',
              boxShadow: '0 2px 6px rgba(239, 68, 68, 0.4)',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
            }}
          >
            <span>☠️ Inject Poison Pill</span>
          </button>
        </div>
      </div>
    </div>
  );
};

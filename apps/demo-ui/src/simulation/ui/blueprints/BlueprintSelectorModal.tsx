import React, { useEffect, useState } from 'react';
import type { SystemBlueprint } from '../../blueprints/systemBlueprints';

interface BlueprintSelectorModalProps {
  isOpen: boolean;
  onClose: () => void;
  blueprints: SystemBlueprint[];
  activeBlueprintId?: string;
  onSelectBlueprint: (blueprintId: string) => void;
}

export const BlueprintSelectorModal: React.FC<BlueprintSelectorModalProps> = ({
  isOpen,
  onClose,
  blueprints,
  activeBlueprintId,
  onSelectBlueprint,
}) => {
  const [selectedCategory, setSelectedCategory] = useState<string>('ALL');

  // Close on Escape key
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };
    if (isOpen) {
      window.addEventListener('keydown', handleKeyDown);
    }
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const categories = [
    { id: 'ALL', label: 'All Architectures' },
    { id: 'Enterprise', label: 'Full Production' },
    { id: 'Beginner', label: 'Minimal & Core' },
    { id: 'Intermediate', label: 'CDC & Fleet Scaling' },
    { id: 'Advanced', label: 'DLQ & Fault Tolerance' },
  ];

  const filteredBlueprints = blueprints.filter((b) => {
    if (selectedCategory === 'ALL') return true;
    return b.complexity === selectedCategory;
  });

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        backgroundColor: 'rgba(15, 23, 42, 0.65)',
        backdropFilter: 'blur(4px)',
        zIndex: 9999,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '20px',
        animation: 'fadeIn 0.15s ease-out',
      }}
      onClick={onClose}
    >
      <div
        style={{
          background: '#ffffff',
          borderRadius: '16px',
          width: '100%',
          maxWidth: '960px',
          maxHeight: '90vh',
          display: 'flex',
          flexDirection: 'column',
          boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.25), 0 0 0 1px rgba(0, 0, 0, 0.05)',
          overflow: 'hidden',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div
          style={{
            padding: '20px 24px',
            borderBottom: '1px solid #e2e8f0',
            display: 'flex',
            alignItems: 'flex-start',
            justifyContent: 'space-between',
            background: 'linear-gradient(to right, #f8fafc, #ffffff)',
          }}
        >
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}>
              <span style={{ fontSize: '1.4rem' }}>📐</span>
              <h2
                style={{
                  margin: 0,
                  fontSize: '1.25rem',
                  fontWeight: 800,
                  color: '#0f172a',
                  letterSpacing: '-0.02em',
                }}
              >
                System Architecture Blueprints
              </h2>
              <span
                style={{
                  fontSize: '0.7rem',
                  fontWeight: 700,
                  padding: '2px 8px',
                  borderRadius: '12px',
                  background: '#e0f2fe',
                  color: '#0369a1',
                }}
              >
                {blueprints.length} Presets Available
              </span>
            </div>
            <p style={{ margin: 0, fontSize: '0.85rem', color: '#64748b', lineHeight: 1.4 }}>
              Select a pre-wired streaming topology to load directly onto the canvas. Instantly explore full production systems or minimal pipelines.
            </p>
          </div>
          <button
            onClick={onClose}
            style={{
              background: '#f1f5f9',
              border: 'none',
              borderRadius: '8px',
              width: '32px',
              height: '32px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              fontSize: '1rem',
              color: '#64748b',
              cursor: 'pointer',
              transition: 'background 0.2s',
            }}
            title="Close modal (Esc)"
          >
            ✕
          </button>
        </div>

        {/* Filter Pills */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
            padding: '12px 24px',
            background: '#f8fafc',
            borderBottom: '1px solid #e2e8f0',
            overflowX: 'auto',
          }}
        >
          <span style={{ fontSize: '0.75rem', fontWeight: 700, color: '#64748b', marginRight: '4px' }}>
            Filter:
          </span>
          {categories.map((cat) => (
            <button
              key={cat.id}
              onClick={() => setSelectedCategory(cat.id)}
              style={{
                padding: '4px 12px',
                borderRadius: '20px',
                border:
                  selectedCategory === cat.id
                    ? '1px solid #0284c7'
                    : '1px solid #cbd5e1',
                background: selectedCategory === cat.id ? '#0284c7' : '#ffffff',
                color: selectedCategory === cat.id ? '#ffffff' : '#475569',
                fontSize: '0.75rem',
                fontWeight: 600,
                cursor: 'pointer',
                whiteSpace: 'nowrap',
                transition: 'all 0.15s ease',
              }}
            >
              {cat.label}
            </button>
          ))}
        </div>

        {/* Blueprint Cards List */}
        <div
          style={{
            padding: '20px 24px',
            overflowY: 'auto',
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(420px, 1fr))',
            gap: '16px',
            background: '#f8fafc',
            flex: 1,
          }}
        >
          {filteredBlueprints.map((blueprint) => {
            const isActive = activeBlueprintId === blueprint.id;
            return (
              <div
                key={blueprint.id}
                style={{
                  background: '#ffffff',
                  borderRadius: '12px',
                  border: isActive ? '2px solid #0284c7' : '1px solid #e2e8f0',
                  boxShadow: isActive
                    ? '0 10px 15px -3px rgba(2, 132, 199, 0.15)'
                    : '0 1px 3px rgba(0, 0, 0, 0.05)',
                  padding: '18px',
                  display: 'flex',
                  flexDirection: 'column',
                  justifyContent: 'space-between',
                  gap: '12px',
                  transition: 'transform 0.15s ease, box-shadow 0.15s ease',
                }}
              >
                <div>
                  {/* Card Header */}
                  <div
                    style={{
                      display: 'flex',
                      alignItems: 'flex-start',
                      justifyContent: 'space-between',
                      gap: '8px',
                      marginBottom: '8px',
                    }}
                  >
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span style={{ fontSize: '1.4rem' }}>{blueprint.icon}</span>
                      <div>
                        <h3
                          style={{
                            margin: 0,
                            fontSize: '0.95rem',
                            fontWeight: 800,
                            color: '#0f172a',
                          }}
                        >
                          {blueprint.name}
                        </h3>
                        <span style={{ fontSize: '0.72rem', color: '#64748b' }}>
                          {blueprint.nodeCount} Components • {blueprint.edgeCount} Flow Wires
                        </span>
                      </div>
                    </div>

                    <span
                      style={{
                        fontSize: '0.68rem',
                        fontWeight: 700,
                        padding: '3px 8px',
                        borderRadius: '6px',
                        background: blueprint.badgeColor.bg,
                        color: blueprint.badgeColor.text,
                        border: `1px solid ${blueprint.badgeColor.border}`,
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {blueprint.badge}
                    </span>
                  </div>

                  {/* Description */}
                  <p
                    style={{
                      margin: '0 0 10px 0',
                      fontSize: '0.8rem',
                      color: '#475569',
                      lineHeight: 1.45,
                    }}
                  >
                    {blueprint.description}
                  </p>

                  {/* Highlights Bullet List */}
                  <div
                    style={{
                      background: '#f8fafc',
                      borderRadius: '8px',
                      padding: '10px 12px',
                      border: '1px solid #f1f5f9',
                      display: 'flex',
                      flexDirection: 'column',
                      gap: '4px',
                    }}
                  >
                    {blueprint.highlights.slice(0, 3).map((hl, idx) => (
                      <div
                        key={idx}
                        style={{
                          fontSize: '0.72rem',
                          color: '#334155',
                          display: 'flex',
                          alignItems: 'flex-start',
                          gap: '6px',
                        }}
                      >
                        <span style={{ color: '#059669', fontWeight: 800 }}>✓</span>
                        <span style={{ lineHeight: 1.35 }}>{hl}</span>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Card Action */}
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    marginTop: '4px',
                    paddingTop: '10px',
                    borderTop: '1px solid #f1f5f9',
                  }}
                >
                  <span
                    style={{
                      fontSize: '0.7rem',
                      fontWeight: 700,
                      color:
                        blueprint.complexity === 'Enterprise'
                          ? '#1d4ed8'
                          : blueprint.complexity === 'Advanced'
                            ? '#dc2626'
                            : blueprint.complexity === 'Intermediate'
                              ? '#d97706'
                              : '#059669',
                    }}
                  >
                    ● {blueprint.complexity}
                  </span>

                  <button
                    onClick={() => {
                      onSelectBlueprint(blueprint.id);
                      onClose();
                    }}
                    style={{
                      padding: '7px 16px',
                      borderRadius: '6px',
                      border: isActive ? '1px solid #0284c7' : '1px solid #0284c7',
                      background: isActive
                        ? '#e0f2fe'
                        : 'linear-gradient(135deg, #0284c7, #2563eb)',
                      color: isActive ? '#0369a1' : '#ffffff',
                      fontWeight: 700,
                      fontSize: '0.75rem',
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '6px',
                      boxShadow: isActive ? 'none' : '0 2px 4px rgba(2, 132, 199, 0.25)',
                      transition: 'all 0.15s ease',
                    }}
                  >
                    <span>{isActive ? '✓ Active Canvas' : 'Load Blueprint'}</span>
                    <span>→</span>
                  </button>
                </div>
              </div>
            );
          })}
        </div>

        {/* Footer */}
        <div
          style={{
            padding: '14px 24px',
            borderTop: '1px solid #e2e8f0',
            background: '#ffffff',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <span style={{ fontSize: '0.75rem', color: '#64748b' }}>
            Tip: You can customize, duplicate, or delete any components on the canvas after loading a blueprint.
          </span>
          <button
            onClick={onClose}
            style={{
              padding: '6px 16px',
              borderRadius: '6px',
              border: '1px solid #cbd5e1',
              background: '#ffffff',
              color: '#334155',
              fontSize: '0.75rem',
              fontWeight: 700,
              cursor: 'pointer',
            }}
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
};

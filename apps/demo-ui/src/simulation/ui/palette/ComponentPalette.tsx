import React, { useState } from 'react';
import { COMPONENT_REGISTRY } from '../../registry/component-registry';
import type { ComponentCategory, ComponentMetadata } from '../../domain/types';

interface ComponentPaletteProps {
  onDragStart: (event: React.DragEvent, kind: string) => void;
  isOpen: boolean;
  onToggle: () => void;
}

export const ComponentPalette: React.FC<ComponentPaletteProps> = ({
  onDragStart,
  isOpen,
  onToggle,
}) => {
  const [search, setSearch] = useState('');

  const filtered = COMPONENT_REGISTRY.filter(
    (c) =>
      c.title.toLowerCase().includes(search.toLowerCase()) ||
      c.description.toLowerCase().includes(search.toLowerCase()),
  );

  const categories: Array<{ id: ComponentCategory; label: string; icon: string }> = [
    { id: 'messaging', label: 'Messaging & Brokers', icon: '📨' },
    { id: 'application', label: 'Application & Workers', icon: '⚙️' },
    { id: 'storage', label: 'Storage & DB', icon: '🗄️' },
  ];

  if (!isOpen) {
    return (
      <button
        onClick={onToggle}
        style={{
          position: 'absolute',
          top: '12px',
          left: '12px',
          zIndex: 10,
          padding: '8px 12px',
          background: '#ffffff',
          border: '1px solid #cbd5e1',
          borderRadius: '8px',
          boxShadow: '0 2px 6px rgba(0,0,0,0.1)',
          cursor: 'pointer',
          fontWeight: 700,
          fontSize: '0.75rem',
          display: 'flex',
          alignItems: 'center',
          gap: '6px',
        }}
      >
        <span>📦</span>
        <span>Palette</span>
      </button>
    );
  }

  return (
    <div
      style={{
        width: '260px',
        background: '#ffffff',
        borderRight: '1px solid #e2e8f0',
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        zIndex: 10,
        boxShadow: '2px 0 8px rgba(0,0,0,0.04)',
      }}
    >
      {/* Header */}
      <div
        style={{
          padding: '12px 14px',
          borderBottom: '1px solid #e2e8f0',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          background: '#f8fafc',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <span style={{ fontSize: '1.1rem' }}>📦</span>
          <span style={{ fontWeight: 800, fontSize: '0.85rem', color: '#0f172a' }}>
            Component Palette
          </span>
        </div>
        <button
          onClick={onToggle}
          style={{
            border: 'none',
            background: 'none',
            cursor: 'pointer',
            fontSize: '0.85rem',
            color: '#64748b',
          }}
        >
          ✕
        </button>
      </div>

      {/* Search Bar */}
      <div style={{ padding: '10px 14px', borderBottom: '1px solid #f1f5f9' }}>
        <input
          type="text"
          placeholder="🔍 Search components..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{
            width: '100%',
            padding: '6px 10px',
            borderRadius: '6px',
            border: '1px solid #cbd5e1',
            fontSize: '0.75rem',
            fontFamily: 'var(--font-sans)',
            boxSizing: 'border-box',
          }}
        />
      </div>

      {/* Categorized List */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '12px 14px' }}>
        {categories.map((cat) => {
          const items = filtered.filter((c) => c.category === cat.id);
          if (items.length === 0) return null;

          return (
            <div key={cat.id} style={{ marginBottom: '16px' }}>
              <div
                style={{
                  fontSize: '0.675rem',
                  fontWeight: 800,
                  color: '#64748b',
                  letterSpacing: '0.04em',
                  marginBottom: '8px',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px',
                }}
              >
                <span>{cat.icon}</span>
                <span>{cat.label.toUpperCase()}</span>
              </div>

              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                {items.map((item: ComponentMetadata) => (
                  <div
                    key={item.kind}
                    draggable
                    onDragStart={(e) => onDragStart(e, item.kind)}
                    style={{
                      padding: '8px 10px',
                      borderRadius: '8px',
                      border: '1px solid #e2e8f0',
                      background: '#ffffff',
                      cursor: 'grab',
                      transition: 'all 0.15s ease',
                      boxShadow: '0 1px 3px rgba(0,0,0,0.03)',
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.borderColor = '#0284c7';
                      e.currentTarget.style.transform = 'translateY(-1px)';
                      e.currentTarget.style.boxShadow = '0 3px 6px rgba(0,0,0,0.06)';
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.borderColor = '#e2e8f0';
                      e.currentTarget.style.transform = 'translateY(0)';
                      e.currentTarget.style.boxShadow = '0 1px 3px rgba(0,0,0,0.03)';
                    }}
                  >
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '3px' }}>
                      <span style={{ fontSize: '1.1rem' }}>{item.icon}</span>
                      <span style={{ fontSize: '0.775rem', fontWeight: 700, color: '#0f172a' }}>
                        {item.title}
                      </span>
                    </div>
                    <div style={{ fontSize: '0.675rem', color: '#64748b', lineHeight: 1.3 }}>
                      {item.description}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>

      {/* Footer Hint */}
      <div
        style={{
          padding: '10px 14px',
          borderTop: '1px solid #e2e8f0',
          background: '#f8fafc',
          fontSize: '0.675rem',
          color: '#64748b',
          textAlign: 'center',
        }}
      >
        💡 Drag components onto the canvas and connect handles to simulate message flow.
      </div>
    </div>
  );
};

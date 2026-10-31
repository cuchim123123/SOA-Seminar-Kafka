import React from 'react';

export type TabId = 'simulation' | 'benchmark' | 'guide';

interface NavigationProps {
  activeTab: TabId;
  onTabChange: (tab: TabId) => void;
  isConsumerPaused?: boolean;
  totalPartitions?: number;
}

export const Navigation: React.FC<NavigationProps> = ({
  activeTab,
  onTabChange,
  isConsumerPaused = false,
  totalPartitions = 6,
}) => {
  const tabs: Array<{ id: TabId; label: string; icon: string; badge?: string; badgeColor?: string }> = [
    {
      id: 'simulation',
      label: 'Visual Simulation Lab',
      icon: '🧪',
      badge: 'Playground',
      badgeColor: 'badge-amber',
    },
    {
      id: 'benchmark',
      label: 'Benchmark & Audit Evidence',
      icon: '📊',
      badge: 'KRaft',
      badgeColor: 'badge-violet',
    },
    {
      id: 'guide',
      label: 'Architecture Guide',
      icon: '📖',
      badge: 'Help',
      badgeColor: 'badge-cyan',
    },
  ];

  return (
    <nav
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        background: '#ffffff',
        padding: '6px 12px',
        borderRadius: 'var(--radius-lg)',
        border: '1px solid var(--border-subtle)',
        boxShadow: 'var(--shadow-card)',
        marginBottom: '20px',
        flexWrap: 'wrap',
        gap: '8px',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        {tabs.map((tab) => {
          const isActive = activeTab === tab.id;
          return (
            <button
              key={tab.id}
              onClick={() => onTabChange(tab.id)}
              style={{
                padding: '8px 18px',
                borderRadius: 'var(--radius-md)',
                border: 'none',
                background: isActive ? 'var(--color-primary)' : 'transparent',
                color: isActive ? '#ffffff' : 'var(--text-body)',
                fontFamily: 'var(--font-sans)',
                fontSize: '0.875rem',
                fontWeight: isActive ? 700 : 500,
                cursor: 'pointer',
                display: 'inline-flex',
                alignItems: 'center',
                gap: '8px',
                transition: 'all 0.15s ease',
                outline: 'none',
                whiteSpace: 'nowrap',
              }}
            >
              <span>{tab.icon}</span>
              <span>{tab.label}</span>
              {tab.badge && (
                <span
                  className={`badge-mono ${isActive ? '' : tab.badgeColor || 'badge-gray'}`}
                  style={{
                    fontSize: '0.675rem',
                    padding: '2px 7px',
                    borderRadius: '4px',
                    background: isActive ? 'rgba(255, 255, 255, 0.25)' : undefined,
                    color: isActive ? '#ffffff' : undefined,
                    border: isActive ? 'none' : undefined,
                    fontWeight: 700,
                  }}
                >
                  {tab.badge}
                </span>
              )}
            </button>
          );
        })}
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        {isConsumerPaused && (
          <span
            style={{
              fontSize: '0.675rem',
              fontWeight: 700,
              padding: '3px 8px',
              borderRadius: '4px',
              background: '#fffbeb',
              color: '#d97706',
              border: '1px solid #fde68a',
              fontFamily: 'var(--font-mono)',
            }}
          >
            CONSUMER PAUSED
          </span>
        )}
        <span style={{ fontSize: '0.725rem', color: '#64748b', fontFamily: 'var(--font-mono)' }}>
          Topic: <b>purchase.events</b> ({totalPartitions} Partitions)
        </span>
      </div>
    </nav>
  );
};

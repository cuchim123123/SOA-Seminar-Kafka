import React from 'react';

export type TabId = 'pipeline' | 'partitions' | 'lag' | 'projections' | 'replay' | 'guide';

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
      id: 'pipeline',
      label: 'Pipeline Flow',
      icon: '⚡',
    },
    {
      id: 'partitions',
      label: 'Kafka Partitions',
      icon: '❖',
      badge: `${totalPartitions}P`,
      badgeColor: 'badge-cyan',
    },
    {
      id: 'lag',
      label: 'Consumer Lag',
      icon: '◷',
      badge: isConsumerPaused ? 'PAUSED' : undefined,
      badgeColor: isConsumerPaused ? 'badge-amber' : undefined,
    },
    {
      id: 'projections',
      label: 'Read Projections',
      icon: '◈',
    },
    {
      id: 'replay',
      label: 'Bounded Replay',
      icon: '↺',
    },
    {
      id: 'guide',
      label: 'User Guide & Manual',
      icon: '📖',
      badge: 'Help',
      badgeColor: 'badge-violet',
    },
  ];

  return (
    <nav
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: '6px',
        background: '#ffffff',
        padding: '6px',
        borderRadius: 'var(--radius-lg)',
        border: '1px solid var(--border-subtle)',
        boxShadow: 'var(--shadow-card)',
        marginBottom: '20px',
        overflowX: 'auto',
        flexWrap: 'wrap',
      }}
    >
      {tabs.map((tab) => {
        const isActive = activeTab === tab.id;
        return (
          <button
            key={tab.id}
            onClick={() => onTabChange(tab.id)}
            style={{
              padding: '8px 14px',
              borderRadius: 'var(--radius-md)',
              border: 'none',
              background: isActive ? 'var(--color-primary)' : 'transparent',
              color: isActive ? '#ffffff' : 'var(--text-body)',
              fontFamily: 'var(--font-sans)',
              fontSize: '0.85rem',
              fontWeight: isActive ? 600 : 500,
              cursor: 'pointer',
              display: 'inline-flex',
              alignItems: 'center',
              gap: '8px',
              transition: 'background-color 0.15s ease, color 0.15s ease',
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
                  padding: '1px 6px',
                  background: isActive ? 'rgba(255, 255, 255, 0.25)' : undefined,
                  color: isActive ? '#ffffff' : undefined,
                  border: isActive ? 'none' : undefined,
                }}
              >
                {tab.badge}
              </span>
            )}
          </button>
        );
      })}
    </nav>
  );
};

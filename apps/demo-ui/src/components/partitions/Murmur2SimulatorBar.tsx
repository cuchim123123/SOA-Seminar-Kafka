import React from 'react';
import type { PartitionComputationResult } from '../../utils/kafka-partitioner';

interface Murmur2SimulatorBarProps {
  simulatorKey: string;
  setSimulatorKey: (key: string) => void;
  hashResult: PartitionComputationResult;
}

export const Murmur2SimulatorBar: React.FC<Murmur2SimulatorBarProps> = ({
  simulatorKey,
  setSimulatorKey,
  hashResult,
}) => {
  return (
    <div
      style={{
        marginTop: '16px',
        padding: '14px 18px',
        borderRadius: 'var(--radius-md)',
        background: '#f8fafc',
        border: '1px solid var(--border-subtle)',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '10px', flexWrap: 'wrap', gap: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '1rem', color: '#0284c7' }}>#</span>
          <span style={{ fontSize: '0.875rem', fontWeight: 700, color: 'var(--text-main)' }}>
            Kafka Default Partitioner Simulator (Murmur2 Hash)
          </span>
        </div>
        <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
          Kafka standard: <code style={{ fontFamily: 'var(--font-mono)' }}>abs(murmur2(key)) % {hashResult.partitionCount}</code>
        </span>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <label style={{ fontSize: '0.775rem', fontWeight: 600, color: 'var(--text-muted)' }}>
            Test Partition Key:
          </label>
          <input
            type="text"
            value={simulatorKey}
            onChange={(e) => setSimulatorKey(e.target.value)}
            placeholder="e.g. U001, cust_hot_spot..."
            style={{
              padding: '6px 10px',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--border-medium)',
              fontSize: '0.8rem',
              fontFamily: 'var(--font-mono)',
              width: '160px',
              background: '#ffffff',
            }}
          />
        </div>

        {/* Quick fixture shortcuts */}
        <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
          {['U001', 'U002', 'U003', 'cust_alpha', 'VIP_HOT', 'cust_whale'].map((k) => (
            <button
              key={k}
              type="button"
              onClick={() => setSimulatorKey(k)}
              style={{
                padding: '4px 8px',
                borderRadius: 'var(--radius-sm)',
                border: simulatorKey === k ? '1px solid #0284c7' : '1px solid var(--border-subtle)',
                background: simulatorKey === k ? '#e0f2fe' : '#ffffff',
                color: simulatorKey === k ? '#0369a1' : 'var(--text-body)',
                fontSize: '0.75rem',
                fontFamily: 'var(--font-mono)',
                cursor: 'pointer',
                fontWeight: simulatorKey === k ? 700 : 400,
              }}
            >
              {k}
            </button>
          ))}
        </div>

        {/* Live Computed Partition Result */}
        <div
          style={{
            marginLeft: 'auto',
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
            background: '#ffffff',
            padding: '5px 12px',
            borderRadius: 'var(--radius-sm)',
            border: '1px solid #bae6fd',
          }}
        >
          <span style={{ fontSize: '0.75rem', color: '#0369a1', fontFamily: 'var(--font-mono)' }}>
            Hash: {hashResult.hexHash}
          </span>
          <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>➔</span>
          <span
            style={{
              fontFamily: 'var(--font-mono)',
              fontWeight: 800,
              fontSize: '0.9rem',
              color: '#0284c7',
            }}
          >
            Partition P#{hashResult.targetPartition}
          </span>
        </div>
      </div>
    </div>
  );
};

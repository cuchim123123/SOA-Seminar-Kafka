import React from 'react';
import type { StagingCustomerRecord } from '../../api/client';

interface ReplayStagingTableProps {
  records: StagingCustomerRecord[];
}

export const ReplayStagingTable: React.FC<ReplayStagingTableProps> = ({ records }) => {
  if (records.length === 0) return null;

  return (
    <div style={{ marginTop: '16px' }}>
      <div style={{ fontSize: '0.85rem', fontWeight: 700, color: 'var(--text-main)', marginBottom: '8px' }}>
        Replay Staging Records (Top {records.length} customers computed):
      </div>
      <div style={{ overflowX: 'auto', border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-sm)' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.785rem', textAlign: 'left', fontFamily: 'var(--font-mono)' }}>
          <thead>
            <tr style={{ background: 'var(--bg-subtle)', borderBottom: '1px solid var(--border-subtle)', color: 'var(--text-muted)' }}>
              <th style={{ padding: '8px 12px' }}>Customer ID</th>
              <th style={{ padding: '8px 12px' }}>Recomputed Points</th>
              <th style={{ padding: '8px 12px' }}>Recomputed Tier</th>
              <th style={{ padding: '8px 12px' }}>Eligible Spend</th>
              <th style={{ padding: '8px 12px' }}>Events Scanned</th>
              <th style={{ padding: '8px 12px' }}>Last Customer Seq</th>
            </tr>
          </thead>
          <tbody>
            {records.map((rec) => (
              <tr key={rec.customerId} style={{ borderBottom: '1px solid var(--border-subtle)' }}>
                <td style={{ padding: '8px 12px', fontWeight: 600, color: 'var(--color-primary)' }}>{rec.customerId}</td>
                <td style={{ padding: '8px 12px', color: 'var(--color-emerald)', fontWeight: 700 }}>{rec.points} pts</td>
                <td style={{ padding: '8px 12px' }}>
                  <span className={`badge-mono ${rec.tier === 'GOLD' ? 'badge-amber' : rec.tier === 'SILVER' ? 'badge-cyan' : 'badge-ghost'}`}>
                    ★ {rec.tier}
                  </span>
                </td>
                <td style={{ padding: '8px 12px' }}>${(rec.eligibleSpendCents / 100).toFixed(2)}</td>
                <td style={{ padding: '8px 12px' }}>{rec.scannedEventCount}</td>
                <td style={{ padding: '8px 12px' }}>#{rec.lastCustomerSequence}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

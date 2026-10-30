import React, { useState, useEffect } from 'react';
import { api } from '../api/client';
import type { LoyaltyAccountDto, SalesSummaryDto } from '../api/client';

interface ProjectionsPanelProps {
  summary: SalesSummaryDto | null;
  lastUpdatedEventTime?: number;
}

export const ProjectionsPanel: React.FC<ProjectionsPanelProps> = ({
  summary,
  lastUpdatedEventTime,
}) => {
  const [selectedCustomerId, setSelectedCustomerId] = useState<string>('U001');
  const [account, setAccount] = useState<LoyaltyAccountDto | null>(null);
  const [loadingAccount, setLoadingAccount] = useState<boolean>(false);
  const [refreshTrigger, setRefreshTrigger] = useState<number>(0);

  useEffect(() => {
    let cancelled = false;
    const fetchAccount = async () => {
      setLoadingAccount(true);
      try {
        const data = await api.getLoyaltyAccount(selectedCustomerId.trim());
        if (!cancelled) setAccount(data);
      } catch {
        if (!cancelled) setAccount(null);
      } finally {
        if (!cancelled) setLoadingAccount(false);
      }
    };

    fetchAccount();
    return () => {
      cancelled = true;
    };
  }, [selectedCustomerId, lastUpdatedEventTime, refreshTrigger]);

  const getTierColor = (tier: string) => {
    switch (tier) {
      case 'GOLD':
        return '#b45309';
      case 'SILVER':
        return '#0369a1';
      default:
        return '#475569';
    }
  };

  return (
    <section className="cyber-card" style={{ padding: '20px 24px', marginBottom: '20px' }}>
      <div style={{ marginBottom: '16px' }}>
        <h2 style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ color: 'var(--color-violet)' }}>◈</span>
          Downstream Read Projections (Isolated Postgres Databases)
        </h2>
        <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem', marginTop: '2px' }}>
          Business projections are updated idempotently by consumer workers after DB commits. Databases are strictly isolated (<code style={{ color: 'var(--color-cyan)', fontFamily: 'var(--font-mono)' }}>loyalty_db</code> and <code style={{ color: 'var(--color-cyan)', fontFamily: 'var(--font-mono)' }}>analytics_db</code>).
        </p>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '16px' }}>
        
        {/* Widget 1: Loyalty Account Projection */}
        <div
          style={{
            padding: '16px',
            borderRadius: 'var(--radius-md)',
            background: '#ffffff',
            border: '1px solid var(--border-subtle)',
            boxShadow: 'var(--shadow-sm)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '8px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span className="badge-mono badge-violet">loyalty_db</span>
              <span style={{ fontWeight: 600, fontSize: '0.9rem', color: 'var(--text-main)' }}>Loyalty Account</span>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap' }}>
              <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>Customer:</span>
              <div style={{ display: 'flex', gap: '3px' }}>
                {['U001', 'U002', 'cust_alpha'].map((c) => (
                  <button
                    key={c}
                    type="button"
                    onClick={() => setSelectedCustomerId(c)}
                    style={{
                      padding: '2px 6px',
                      fontSize: '0.725rem',
                      fontFamily: 'var(--font-mono)',
                      borderRadius: 'var(--radius-sm)',
                      border: selectedCustomerId === c ? '1px solid #7c3aed' : '1px solid var(--border-medium)',
                      background: selectedCustomerId === c ? '#f5f3ff' : '#ffffff',
                      color: selectedCustomerId === c ? '#7c3aed' : 'var(--text-body)',
                      cursor: 'pointer',
                      fontWeight: selectedCustomerId === c ? 700 : 400,
                    }}
                  >
                    {c}
                  </button>
                ))}
              </div>
              <input
                type="text"
                value={selectedCustomerId}
                onChange={(e) => setSelectedCustomerId(e.target.value)}
                placeholder="Customer ID..."
                style={{
                  background: '#ffffff',
                  color: 'var(--text-main)',
                  border: '1px solid var(--border-medium)',
                  borderRadius: 'var(--radius-sm)',
                  padding: '3px 6px',
                  fontSize: '0.75rem',
                  fontFamily: 'var(--font-mono)',
                  width: '95px',
                  outline: 'none',
                }}
              />
              <button
                type="button"
                onClick={() => setRefreshTrigger((prev) => prev + 1)}
                disabled={loadingAccount}
                className="btn-cyber btn-ghost"
                style={{ padding: '3px 8px', fontSize: '0.725rem' }}
                title="Reload live PostgreSQL projection"
              >
                {loadingAccount ? '...' : '↺ Refresh'}
              </button>
            </div>
          </div>

          {loadingAccount && !account ? (
            <div style={{ padding: '24px', textAlign: 'center', color: 'var(--text-dim)', fontSize: '0.8rem', fontFamily: 'var(--font-mono)' }}>
              Loading loyalty projection...
            </div>
          ) : account ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 14px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)' }}>
                <div>
                  <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', textTransform: 'uppercase' }}>Points Balance</div>
                  <div style={{ fontSize: '1.5rem', fontWeight: 800, color: 'var(--color-violet)', fontFamily: 'var(--font-mono)', marginTop: '2px' }}>
                    {account.pointsBalance} <span style={{ fontSize: '0.8rem', fontWeight: 500, color: 'var(--text-muted)' }}>pts</span>
                  </div>
                </div>

                <div style={{ textAlign: 'right' }}>
                  <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', textTransform: 'uppercase' }}>Current Tier</div>
                  <div
                    style={{
                      fontSize: '1rem',
                      fontWeight: 800,
                      color: getTierColor(account.tier),
                      fontFamily: 'var(--font-mono)',
                      marginTop: '4px',
                    }}
                  >
                    ★ {account.tier}
                  </div>
                </div>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
                <div style={{ padding: '8px 10px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)' }}>
                  <div style={{ color: 'var(--text-muted)' }}>Eligible Spend</div>
                  <div style={{ color: 'var(--text-main)', fontWeight: 600, marginTop: '2px' }}>
                    ${(account.totalEligibleSpendCents / 100).toFixed(2)}
                  </div>
                </div>

                <div style={{ padding: '8px 10px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)' }}>
                  <div style={{ color: 'var(--text-muted)' }}>Status</div>
                  <div style={{ color: 'var(--color-cyan)', fontWeight: 600, marginTop: '2px' }}>
                    {account.lastCustomerSequence ? `Seq #${account.lastCustomerSequence}` : 'Active Account'}
                  </div>
                </div>
              </div>

              {account.correctionRequired && (
                <div style={{ padding: '8px 12px', background: 'var(--color-amber-bg)', border: '1px solid var(--color-amber-border)', borderRadius: 'var(--radius-sm)', fontSize: '0.75rem', color: '#b45309', display: 'flex', alignItems: 'center', gap: '6px' }}>
                  <span>⚠</span>
                  <span>Correction flag active: Invalidation received, pending two-pass bounded replay.</span>
                </div>
              )}
            </div>
          ) : (
            <div style={{ padding: '24px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '0.8rem', fontFamily: 'var(--font-mono)' }}>
              No loyalty record yet for {selectedCustomerId}. Emit a purchase to create account.
            </div>
          )}
        </div>

        {/* Widget 2: Platform Analytics Projection */}
        <div
          style={{
            padding: '16px',
            borderRadius: 'var(--radius-md)',
            background: '#ffffff',
            border: '1px solid var(--border-subtle)',
            boxShadow: 'var(--shadow-sm)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span className="badge-mono badge-cyan">analytics_db</span>
              <span style={{ fontWeight: 600, fontSize: '0.9rem', color: 'var(--text-main)' }}>Sales Analytics Summary</span>
            </div>
            <span className="badge-mono badge-emerald">Live Projection</span>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 14px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)' }}>
              <div>
                <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', textTransform: 'uppercase' }}>Total Platform Revenue</div>
                <div style={{ fontSize: '1.5rem', fontWeight: 800, color: 'var(--color-emerald)', fontFamily: 'var(--font-mono)', marginTop: '2px' }}>
                  ${((summary?.totalRevenueCents || 0) / 100).toFixed(2)}
                </div>
              </div>

              <div style={{ textAlign: 'right' }}>
                <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', textTransform: 'uppercase' }}>Completed Purchases</div>
                <div style={{ fontSize: '1.3rem', fontWeight: 800, color: 'var(--color-cyan)', fontFamily: 'var(--font-mono)', marginTop: '2px' }}>
                  {summary?.totalPurchasesCount || 0}
                </div>
              </div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
              <div style={{ padding: '8px 10px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)' }}>
                <div style={{ color: 'var(--text-muted)' }}>Processed Events</div>
                <div style={{ color: 'var(--text-main)', fontWeight: 600, marginTop: '2px' }}>
                  {summary?.processedEventCount || 0} events
                </div>
              </div>

              <div style={{ padding: '8px 10px', background: 'var(--bg-subtle)', borderRadius: 'var(--radius-sm)' }}>
                <div style={{ color: 'var(--text-muted)' }}>Last Updated</div>
                <div style={{ color: 'var(--text-body)', fontWeight: 500, marginTop: '2px', fontSize: '0.7rem' }}>
                  {summary?.lastUpdated ? new Date(summary.lastUpdated).toLocaleTimeString() : 'N/A'}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
};

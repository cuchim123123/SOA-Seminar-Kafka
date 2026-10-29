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

  useEffect(() => {
    let cancelled = false;
    const fetchAccount = async () => {
      setLoadingAccount(true);
      try {
        const data = await api.getLoyaltyAccount(selectedCustomerId);
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
  }, [selectedCustomerId, lastUpdatedEventTime]);

  const getTierColor = (tier: string) => {
    switch (tier) {
      case 'GOLD':
        return 'var(--color-amber)';
      case 'SILVER':
        return 'var(--color-cyan)';
      default:
        return '#94a3b8';
    }
  };

  return (
    <section className="cyber-card" style={{ padding: '24px', marginBottom: '24px' }}>
      <div style={{ marginBottom: '16px' }}>
        <h2 style={{ fontSize: '1.15rem', fontWeight: 700, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ color: 'var(--color-violet)' }}>◈</span>
          DOWNSTREAM READ PROJECTIONS (ISOLATED POSTGRES DATABASES)
        </h2>
        <p style={{ color: 'var(--text-muted)', fontSize: '0.825rem' }}>
          Business projections are updated idempotently by consumer workers after DB commits. Databases are strictly isolated (<code style={{ color: 'var(--color-cyan)', fontFamily: 'var(--font-mono)' }}>loyalty_db</code> and <code style={{ color: 'var(--color-cyan)', fontFamily: 'var(--font-mono)' }}>analytics_db</code>).
        </p>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '20px' }}>
        
        {/* Widget 1: Loyalty Account Projection */}
        <div
          className="cyber-card"
          style={{
            padding: '18px',
            background: 'rgba(10, 14, 24, 0.85)',
            border: '1px solid var(--border-subtle)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px', flexWrap: 'wrap', gap: '8px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span className="badge-mono badge-violet">loyalty_db</span>
              <span style={{ fontWeight: 600, fontSize: '0.9rem' }}>Loyalty Account</span>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{ fontSize: '0.75rem', color: 'var(--text-dim)' }}>Customer:</span>
              <select
                value={selectedCustomerId}
                onChange={(e) => setSelectedCustomerId(e.target.value)}
                style={{
                  background: 'var(--bg-input)',
                  color: 'var(--text-main)',
                  border: '1px solid var(--border-subtle)',
                  borderRadius: 'var(--radius-sm)',
                  padding: '4px 8px',
                  fontSize: '0.75rem',
                  fontFamily: 'var(--font-mono)',
                  outline: 'none',
                }}
              >
                <option value="U001">U001</option>
                <option value="U002">U002</option>
                <option value="U003">U003</option>
              </select>
            </div>
          </div>

          {loadingAccount && !account ? (
            <div style={{ padding: '24px', textAlign: 'center', color: 'var(--text-dim)', fontSize: '0.8rem', fontFamily: 'var(--font-mono)' }}>
              Loading loyalty projection...
            </div>
          ) : account ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 14px', background: 'rgba(0,0,0,0.25)', borderRadius: 'var(--radius-sm)' }}>
                <div>
                  <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', textTransform: 'uppercase' }}>Points Balance</div>
                  <div style={{ fontSize: '1.6rem', fontWeight: 800, color: 'var(--color-violet)', fontFamily: 'var(--font-mono)', marginTop: '2px' }}>
                    {account.pointsBalance} <span style={{ fontSize: '0.8rem', fontWeight: 500, color: 'var(--text-muted)' }}>pts</span>
                  </div>
                </div>

                <div style={{ textAlign: 'right' }}>
                  <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', textTransform: 'uppercase' }}>Current Tier</div>
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

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
                <div style={{ padding: '8px 10px', background: 'rgba(255,255,255,0.03)', borderRadius: 'var(--radius-sm)' }}>
                  <div style={{ color: 'var(--text-dim)' }}>Eligible Spend</div>
                  <div style={{ color: 'var(--text-main)', fontWeight: 600, marginTop: '2px' }}>
                    ${(account.totalEligibleSpendCents / 100).toFixed(2)}
                  </div>
                </div>

                <div style={{ padding: '8px 10px', background: 'rgba(255,255,255,0.03)', borderRadius: 'var(--radius-sm)' }}>
                  <div style={{ color: 'var(--text-dim)' }}>Last Sequence</div>
                  <div style={{ color: 'var(--color-cyan)', fontWeight: 600, marginTop: '2px' }}>
                    Seq #{account.lastCustomerSequence}
                  </div>
                </div>
              </div>

              {account.correctionRequired && (
                <div style={{ padding: '6px 10px', background: 'rgba(245, 158, 11, 0.12)', border: '1px solid rgba(245, 158, 11, 0.3)', borderRadius: 'var(--radius-sm)', fontSize: '0.75rem', color: '#fcd34d', display: 'flex', alignItems: 'center', gap: '6px' }}>
                  <span>⚠</span>
                  <span>Correction flag active: Invalidation received, pending two-pass replay.</span>
                </div>
              )}
            </div>
          ) : (
            <div style={{ padding: '24px', textAlign: 'center', color: 'var(--text-dim)', fontSize: '0.8rem', fontFamily: 'var(--font-mono)' }}>
              No loyalty record yet for {selectedCustomerId}. Emit a purchase to create account.
            </div>
          )}
        </div>

        {/* Widget 2: Platform Analytics Projection */}
        <div
          className="cyber-card"
          style={{
            padding: '18px',
            background: 'rgba(10, 14, 24, 0.85)',
            border: '1px solid var(--border-subtle)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '14px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span className="badge-mono badge-cyan">analytics_db</span>
              <span style={{ fontWeight: 600, fontSize: '0.9rem' }}>Sales Analytics Summary</span>
            </div>
            <span className="badge-mono badge-emerald">Live Projection</span>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 14px', background: 'rgba(0,0,0,0.25)', borderRadius: 'var(--radius-sm)' }}>
              <div>
                <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', textTransform: 'uppercase' }}>Total Platform Revenue</div>
                <div style={{ fontSize: '1.6rem', fontWeight: 800, color: 'var(--color-emerald)', fontFamily: 'var(--font-mono)', marginTop: '2px' }}>
                  ${((summary?.totalRevenueCents || 0) / 100).toFixed(2)}
                </div>
              </div>

              <div style={{ textAlign: 'right' }}>
                <div style={{ fontSize: '0.7rem', color: 'var(--text-dim)', textTransform: 'uppercase' }}>Completed Purchases</div>
                <div style={{ fontSize: '1.3rem', fontWeight: 800, color: 'var(--color-cyan)', fontFamily: 'var(--font-mono)', marginTop: '2px' }}>
                  {summary?.totalPurchasesCount || 0}
                </div>
              </div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
              <div style={{ padding: '8px 10px', background: 'rgba(255,255,255,0.03)', borderRadius: 'var(--radius-sm)' }}>
                <div style={{ color: 'var(--text-dim)' }}>Processed Events</div>
                <div style={{ color: 'var(--text-main)', fontWeight: 600, marginTop: '2px' }}>
                  {summary?.processedEventCount || 0} events
                </div>
              </div>

              <div style={{ padding: '8px 10px', background: 'rgba(255,255,255,0.03)', borderRadius: 'var(--radius-sm)' }}>
                <div style={{ color: 'var(--text-dim)' }}>Last Updated</div>
                <div style={{ color: 'var(--text-muted)', fontWeight: 500, marginTop: '2px', fontSize: '0.7rem' }}>
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

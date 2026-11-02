import { useState } from 'react';
import { Recovery } from './Recovery';
import { ConsumerControls } from './ConsumerControls';
import { EventEvidence } from './EventEvidence';
import { Purchases } from './Purchases';
import { api } from '../api/client';
import { businessApi } from '../api/business';
import { useLiveResource } from './hooks/useLiveResource';
import { routeHash } from './state/routes';
import type { WorkspaceRoute } from './state/routes';

export function RealApplication({ route }: { route: WorkspaceRoute }) {
  const [customer, setCustomer] = useState(route.customerId);
  const purchases = useLiveResource('producer', route.customerId, (signal) =>
    businessApi.listPurchases({ customerId: route.customerId || undefined, limit: 25 }, signal),
  );
  const account = useLiveResource('loyalty', route.customerId, () =>
    route.customerId ? api.getLoyaltyAccount(route.customerId) : Promise.resolve(null),
  );
  const analytics = useLiveResource('analytics', 'summary', () => api.getAnalyticsSummary());
  return (
    <section className="real-workspace">
      <header>
        <span className="badge-mono badge-emerald">Live backend</span>
        <h1>Real Application</h1>
        <p>
          Source purchases and independent business projections. Updates arrive through service
          event streams.
        </p>
      </header>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          window.location.hash = routeHash({
            ...route,
            customerId: customer.trim(),
            purchaseId: '',
          });
        }}
      >
        <label>
          Customer ID{' '}
          <input
            value={customer}
            onChange={(event) => setCustomer(event.target.value)}
            placeholder="Enter a customer ID"
          />
        </label>
        <button type="submit">Select customer</button>
      </form>
      {route.page === 'purchases' ? (
        <Purchases route={route} />
      ) : route.page === 'recovery' ? (
        <Recovery route={route} />
      ) : route.page === 'consumers' ? (
        <ConsumerControls />
      ) : route.page === 'events' ? (
        <EventEvidence key={route.purchaseId} route={route} />
      ) : (
        <>
          <div className="real-columns">
            {route.page !== 'analytics' && (
              <article>
                <h2>Loyalty account</h2>
                <p>Push: {account.connection}</p>
                {account.error && <p role="alert">Unavailable: {account.error}</p>}
                {!route.customerId ? (
                  <p>Select a customer to inspect their account.</p>
                ) : account.loading && !account.data ? (
                  <p>Loading account…</p>
                ) : account.data ? (
                  <>
                    <strong>
                      {account.data.pointsBalance} points · {account.data.tier}
                    </strong>
                    <p>Eligible spend: {account.data.totalEligibleSpendCents} cents</p>
                    <p>Applied sequence: {account.data.lastCustomerSequence ?? 'Unknown'}</p>
                    <p>
                      {account.data.correctionRequired
                        ? 'Correction required'
                        : 'No correction flag'}
                    </p>
                  </>
                ) : (
                  !account.error && <p>Account not created yet.</p>
                )}
                <p>
                  {account.stale ? 'State may be stale. ' : ''}Observed:{' '}
                  {account.observedAt ?? 'Not yet observed'}
                </p>
                {route.page === 'loyalty' && (
                  <>
                    <p>Bronze: below 10,000 cents; Silver: from 10,000; Gold: from 30,000.</p>
                    {account.data && (
                      <progress
                        aria-label="Eligible spend toward Gold"
                        max={30000}
                        value={Math.min(account.data.totalEligibleSpendCents, 30000)}
                      />
                    )}
                    <p>
                      Points = floor(purchase cents / 1,000) × the tier multiplier before that
                      purchase (Bronze1, Silver2, Gold3). A new tier applies to subsequent
                      purchases.
                    </p>
                  </>
                )}
                <button onClick={account.refresh}>Refresh account</button>
              </article>
            )}
            {route.page !== 'loyalty' && (
              <article>
                <h2>Global sales analytics</h2>
                <p>Push: {analytics.connection}</p>
                {analytics.error && <p role="alert">Unavailable: {analytics.error}</p>}
                {analytics.data && (
                  <>
                    <strong>{analytics.data.totalPurchasesCount} completed purchases</strong>
                    <p>Gross revenue: {analytics.data.totalRevenueCents} cents</p>
                    <p>Unique customers: {analytics.data.uniqueCustomerCount ?? 'Unknown'}</p>
                  </>
                )}
                <p>Invalidations and Loyalty replay do not reduce these gross totals.</p>
                <p>
                  {analytics.stale ? 'State may be stale. ' : ''}Observed:{' '}
                  {analytics.observedAt ?? 'Not yet observed'}
                </p>
                <p>Last processed: {analytics.data?.lastUpdated ?? 'Not observed'}</p>
                <a href={routeHash({ ...route, page: 'consumers', real: true })}>
                  Open consumer controls
                </a>
                <button onClick={analytics.refresh}>Refresh analytics</button>
              </article>
            )}
          </div>
          {route.page !== 'analytics' && (
            <article>
              <h2>Recent source purchases {route.customerId && `for ${route.customerId}`}</h2>
              <p>
                Push: {purchases.connection} · {purchases.stale ? 'May be stale' : 'Connected'} ·
                Latest 25 records
              </p>
              {purchases.error && <p role="alert">Unavailable: {purchases.error}</p>}
              {purchases.loading && !purchases.data && <p>Loading source history…</p>}
              <div className="real-table">
                <table>
                  <thead>
                    <tr>
                      <th>Purchase</th>
                      <th>Customer</th>
                      <th>Amount (cents)</th>
                      <th>Source status</th>
                      <th>Completion sequence</th>
                    </tr>
                  </thead>
                  <tbody>
                    {purchases.data?.items.map((item) => (
                      <tr key={item.purchaseId}>
                        <td>
                          <a
                            href={routeHash({
                              ...route,
                              page: 'events',
                              real: true,
                              purchaseId: item.purchaseId,
                            })}
                          >
                            {item.purchaseId}
                          </a>
                        </td>
                        <td>{item.customerId}</td>
                        <td>{item.amountCents}</td>
                        <td>{item.status}</td>
                        <td>{item.completionSequence}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {purchases.data?.items.length === 0 && <p>No matching persisted purchases.</p>}
              <p>Source status does not assert downstream processing.</p>
              <button onClick={purchases.refresh}>Refresh history</button>
            </article>
          )}
        </>
      )}
    </section>
  );
}

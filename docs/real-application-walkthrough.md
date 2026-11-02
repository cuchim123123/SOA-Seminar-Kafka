# Real Application presenter walkthrough

Use the existing local seminar startup (`pnpm start`) and open the Demo UI. Keep Simulation Playground and Real Application distinct: real pages read and mutate actual lab services.

## Configuration and deployment

The root `.env` supplies public `VITE_KAFBAT_URL`, `VITE_GRAFANA_URL` and `VITE_HTTP_TIMEOUT_MS`. Vite loads these at startup/build; restart the dev server or rebuild after editing. URLs must use HTTP(S) without embedded credentials. Timeout bounds are1,000–600,000ms, default120,000ms. Zod validates settings. No secrets belong in `VITE_` variables.

`UI_PRODUCER_ORIGIN`, `UI_LOYALTY_ORIGIN`, `UI_ANALYTICS_ORIGIN` and `UI_REPLAY_ORIGIN` configure the Vite dev proxy upstreams. Defaults match `.env.example`. Kafbat and Grafana links open existing tools; they do not start those tools. Grafana uses the monitoring stack's default port3005.

The production build creates static assets, but it does **not** bundle Vite's proxy. A static deployment needs an explicitly configured reverse proxy for the existing relative API prefixes, health routes and CDC status. SSE responses need streaming without buffering and suitable idle timeouts. No hosted deployment is configured by this feature.

## Purchase and projection demonstration

1. Choose a fresh synthetic customer ID in the real workspace. Purchases accepts an amount in currency units with at most two decimal places;100.00 becomes10,000 cents exactly.
2. Submit100.00 and retain the displayed idempotency key. Source acceptance is a committed source transaction. Wait for independent Loyalty and Analytics receipts; they prove each handler's database transaction, not its later Kafka offset commit.
3. Open Loyalty Accounts: the first purchase earns10 points and reaches Silver. Submit another10.00 under a **new** intent; it earns2 points because the previous tier applies. Analytics shows global gross totals, not customer-filtered revenue.
4. Repeat an identical single-purchase request with the same key/body: returned purchase/event identity stays unchanged. Change its amount under that key: expect HTTP409. Returning to the page regenerates page-local intent state, so retain the key before leaving after an uncertain outcome.
5. Use a small same-customer burst (for example3×10.00). Inspect actual allocated customer sequences and open each event's receipt evidence. Ordering is partition-scoped; customer sequence is not a global Kafka offset.

## Independent groups

Open Consumer Controls. Note current pause and delay settings before changing them. Pause Analytics, submit a small purchase and inspect independent Loyalty/Analytics evidence. A handler already in flight may finish after pause. Resume Analytics and observe catch-up; restore original delay and pause settings before finishing. Process-local controls are not broker administration or separately deployed replicas. No real poison injection/DLQ redrive feature is exposed.

## Correction and replay

1. Select a known purchase in Purchases or Event Evidence. Confirm its invalidation explicitly. Repeating this action must report the existing invalidation event rather than a new event.
2. Open Replay & Correction with that purchase selected. Start is gated on the actual Loyalty invalidation receipt, correction flag and applied sequence. Invalidating does not immediately subtract points or decrement Analytics.
3. Start bounded replay. Keep the generated run ID; POST awaits the scan. If the request times out/disconnects, recover that same ID and read persisted state. A transient404 may precede startup persistence. Do not automatically start a duplicate run.
4. Inspect selected-customer preview, then the separate **global** review. Every staged customer is compared; the selection never limits promotion. Missing comparisons, unsafe read precision or disconnected feeds block confirmation. Newer live sequence is expected to skip; equal sequence remains eligible even if values are unchanged.
5. Open global confirmation to refresh all comparisons. Review actual expected changes and acknowledge the all-customer scope. Any new review invalidates acknowledgement. Execute only when intentionally promoting this entire run. Preview is observational; backend rechecks eligibility with coordinated locks. Read actual applied count afterward; the response does not identify exact applied/skipped customers. Analytics stays gross and unchanged.

Do not execute the retained acceptance run `11c39b6c-b5e2-4393-97f5-a3af185ea6a2` merely to test UI behavior: it includes95 existing staged customers. It was reviewed without global promotion.

## Outages and verification limits

Disconnected push streams and failed reads retain explicitly stale snapshots. A true account404 means no account; network/service errors mean unavailable. Manual refresh is available. There is no timer-based business-state polling. Client timeout/cancellation does not cancel a server-side mutation or replay, and no HTTP write is automatically retried.

See [implementation progress](real-application-implementation-progress.md) for executed checks and remaining acceptance work. Mobile/keyboard and partial-service-outage browser checks remain pending; code/build/unit success does not certify those interactions. Non-fatal bundle-size and React Flow directive warnings remain.

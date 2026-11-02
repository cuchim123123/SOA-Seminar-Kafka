# Real Application workspace — implementation plan

Date: 2026-11-02. Status: **approved for incremental implementation**, including server push. The user subsequently approved coordinated database locking and concurrency tests for cutover and live Loyalty processing. Implementation checkpoints, validation and remaining work are tracked in [the progress document](docs/real-application-implementation-progress.md). Historical approval boundaries below describe the original proposal; this explicit approval supersedes statements that implementation has not started. Global scope and business eligibility rules remain unchanged.

## 1. Scope and evidence

Add a persistent left sidebar and a real business workspace to the existing Demo UI. Preserve Simulation Playground, the current Purchase benchmark screen, Architecture Guide, and the separate official benchmark CLI. Do not add broker administration, raw-message browsing, infrastructure charts, customer management, authentication, or a new backend service.

This assessment is based on current source, not claims in earlier plans. Runtime end-to-end validation belongs to implementation; no business transactions or infrastructure changes were performed for this analysis. The working tree contains unrelated in-progress simulation and benchmark changes: preserve them and re-read changed files before implementation.

### Verified current system

| Area | Evidence and actual behavior | Implication |
|---|---|---|
| Frontend shell | `apps/demo-ui/src/App.tsx`, `components/Navigation.tsx`: React 18/Vite, hash tabs `simulation`, `benchmark`, `guide`; no router dependency | Extend hash navigation rather than adding a routing framework |
| Design | `src/index.css`: light slate surfaces, blue primary, semantic accent tokens, Inter/monospace, existing buttons/tables/badges | Reuse tokens; do not copy old dark gradients into new pages |
| Simulation | `src/simulation/ui/SimulationLab.tsx`, store/engine, React Flow | Isolated simulated state must never supply real-workspace status |
| Dormant business UI | `ReplayStudio`, `ProjectionsPanel`, `PipelineVisualizer`, `WorkloadSimulator`, pipeline subcomponents exist but are not rendered by App's current routes | Reuse useful presentation and API calls, not entire obsolete workflows unquestioningly |
| HTTP integration | `src/api/client.ts`, `vite.config.ts`: browser-relative proxy prefixes for producer, loyalty, analytics, replay, CDC | No new gateway needed for local seminar deployment |
| Purchase writes | Producer purchases controller and create handlers: transaction persists purchase, sequence, outbox and optional HTTP idempotency key; HTTP 202 after handler completes | Accepted means source transaction committed, not downstream processing complete |
| Purchase reads | `GET /api/v1/purchases/:purchaseId`, Postgres purchase read DAO | Source state and original completion event available; no list/search endpoint |
| Batches | Batch handler: up to 500, one transaction, generated amounts, fixed customer if specified; returns purchase IDs/event IDs/sequences | Useful small same-customer bursts; no batch idempotency contract |
| Loyalty | Account entity/policy and apply handlers | Points use the tier BEFORE each purchase; eligible spend then changes the tier for later purchases |
| Loyalty rules | Bronze below 10,000 cents; Silver from 10,000; Gold from 30,000. Points = floor(amountCents / 1,000) × tier multiplier 1/2/3 | Progression visualization must follow these rules, not apply the new tier retrospectively |
| Ordering | Loyalty rejects sequence gaps; old sequence with a new event ID marks correction required; transaction dedup handles known event IDs | Ordering and deduplication are distinct demonstrations |
| Analytics | Analytics consumer only applies PurchaseCompleted; other valid event types are ignored and offsets committed | Revenue/count are gross completed-purchase aggregates, not invalidation-adjusted net sales |
| Invalidation | Producer invalidation endpoint and Loyalty invalidation handler/entity | Invalidating marks correction required and advances sequence; it does not immediately subtract points |
| Replay | Start handler captures boundaries, scans retained history, recomputes staging with two-pass correction | POST currently awaits completion; not an asynchronous job-acceptance API |
| Cutover | Cutover handler promotes staged records across customers; skips records whose live sequence advanced; rejects all-skipped runs | Selected customer is a comparison filter, not cutover scope |
| CDC | Read DAO explicitly returns ENQUEUED/UNKNOWN and publishedAt=null; Connect status checks connector/tasks | Outbox PENDING is not proof of failed delivery; RUNNING is not per-event delivery evidence |
| Consumer controls | Both workers expose pause/resume, partition controls, delay and status; Loyalty additionally exposes in-process replicas/scale/kill/history | Use application pause/delay for seminars; replicas are not separate deployed worker processes |
| Error handling | `packages/observability/src/kafka/kafka-message-processor.ts` logs/classifies errors and rethrows; adapters commit N+1 only after processing | No implemented real DLQ publish/redrive flow or per-event retry-history API found |
| Observability | Existing metrics and structured logs; no SSE/WebSocket implementation found | Grafana remains aggregate monitoring; UI requires refresh or explicitly approved push work |

### Existing frontend behavior that must not be reused unchanged

- `getLoyaltyAccount` turns every error into null: distinguish a true 404 from service/network failure.
- `request` discards status and structured error details: preserve both for conflicts, validation and retention errors.
- `App` refreshes infrastructure telemetry every three seconds even on simulation, preserves old values without freshness labels, and supplies a fallback partition count of six. Scope refresh to consumers of each resource; unknown must remain unknown.
- The Analytics client assigns purchaseCount to processedEventCount. Do not label that as total delivered events or invalidation processing.
- `waitForLoyaltyProcessed` requires correctionRequired=true: it is an invalidation barrier, not a general purchase-completion helper.
- The recent Loyalty receipts endpoint returns only 30 rows and a global count. Absence from that sample does not prove an event was not processed.
- Legacy replay UI uses fixed sleeps and claims that simulated retention failure actually pruned Kafka history; neither is acceptable evidence.
- Legacy cutover success text implies complete reconciliation even when some customers can be skipped. Show actual affected scope and refetched state.
- Purchase GET returns the original completion sequence/event after invalidation. Keep completion and invalidation event identities separate.

## 2. Proposed information architecture

Persistent desktop sidebar, approximately 240px wide, collapsible to an icon rail; mobile drawer with Escape/overlay dismissal, focus trapping and focus restoration. Main content uses the remaining width with a restrained maximum readable width. Simulation retains a large usable canvas.

```text
Simulation Playground
Real Application
  Business
    Overview
    Purchases
    Loyalty Accounts
    Sales Analytics
  Processing
    Event Evidence
  Recovery
    Replay & Correction
    Consumer Controls
Purchase Benchmark             [existing screen]
Architecture Guide            [existing screen]
External tools
  Kafbat
  Grafana
```

Do not add separate Idempotency, Ordering, Invalidation or DLQ pages. Idempotency belongs beside purchase submission/evidence; ordering belongs in a customer's event sequence; invalidation belongs in purchase detail and links to recovery. Failure limitations appear in Consumer Controls. Existing official broker benchmarks remain CLI-only.

Retain old hashes as valid routes. Add `#real/overview`, `#real/purchases`, `#real/loyalty`, `#real/analytics`, `#real/events`, `#real/recovery`, `#real/consumers`. A small typed route parser builds links and rejects unknown paths. Keep customerId, purchaseId and runId in encoded route query parameters; preserve applicable context between real pages. Browser Back/Forward and refresh must work. An unknown route provides a sensible fallback without losing the app.

Default entry remains Simulation Playground to preserve behavior. Real pages carry a small “Live backend” label; simulation retains its “Simulation” label. API outage never switches a real page to mock data.

## 3. Feature specifications and layouts

### Overview

Purpose: orient the presenter and demonstrate independent downstream projections. Layout: compact customer context bar, one purchase-to-two-projections evidence diagram, current Loyalty state beside global Analytics state, recent source purchases below. Distinguish global totals from selected-customer values. Use simple HTML/SVG with accessible text, not another editable React Flow canvas.

Interactions: choose/type customer, create purchase shortcut, open latest purchase, navigate to correction when required. Show each resource's observed-at time and partial outage independently. No synthetic throughput or inferred per-event Kafka timestamp.

Dependencies: purchase history addition if approved, existing account/summary/status APIs, proposed receipt reads for per-event evidence. Without history approval, label recent items “Created in this browser session.”

### Purchases

Purpose: actual writes, source history and HTTP idempotency. Layout: compact creation form above a filterable purchase table; selected row opens detail alongside it on wide screens and below on narrow screens.

Fields: customer ID, monetary amount with exact cents conversion, optional advanced idempotency key. Generate and retain a key per user intent; retry the same single purchase with the same key after an uncertain response. Never regenerate a key automatically on retry. Show accepted purchase ID immediately, then obtain canonical GET details.

Small burst form: fixed selected customer, count, amount or explicit range. Map fixed amount to equal min/max. Respect actual batch validation (1–500; amounts 100–10,000,000 cents), start with a small presenter-friendly count. Batch requests have no idempotency support: timeout means outcome unknown, not safe to auto-retry.

Detail: amount, source status, source creation time, completion event ID/sequence, separate invalidation evidence, read-model observation, “Invalidate” with reason and an explicit confirmation. Repeated invalidation must display isAlreadyInvalidated rather than claim a new event.

HTTP idempotency exercise: resubmit identical customer/key/body and show returned same purchase/event; resubmit changed amount under same key and show HTTP409 without changing the original record. HTTP202 alone is not a deduplication flag; compare returned IDs.

Backend: existing create, batch, GET-by-ID, invalidate; proposed history query. No order catalog, inventory, checkout or refunds.

### Loyalty Accounts

Purpose: explain the actual tier-dependent projection. Layout: prominent points balance and tier, a spend progression bar with exact thresholds, sequence/correction state, then selected customer's source purchases and observed evidence.

Show “Account not created yet” on confirmed404; service error is separate. New-tier threshold applies to subsequent purchases. A projected points preview, if included, must be labeled an estimate based on the displayed account snapshot; never replace backend totals with local calculations.

Optional before/after snapshot compares two actual successful reads with timestamps. It is a browser observation, not a persisted audit ledger. If correctionRequired is true, keep actual totals visible and offer recovery navigation. Do not invent per-purchase points ledger entries: none is exposed.

### Sales Analytics

Purpose: show an independent consumer/read model. Layout: one concise totals strip (gross revenue, completed-purchase count, unique customers, last processed time), then a before/after comparison and Analytics pause/resume shortcut linking to controls.

State explicitly that invalidation and Loyalty replay do not adjust this aggregate. Do not display customer-filtered revenue: existing API is global. No fabricated time series from a single aggregate snapshot. If a short browser-observation chart is later useful, label its sample window and gaps explicitly; it is optional, not MVP.

Backend: existing summary and Analytics controls; proposed receipt endpoint for a selected completion event. Missing invalidation receipts mean “Not applied by Analytics by design,” not waiting forever.

### Event Evidence

Purpose: show observed application processing, ordering and idempotency without raw Kafka browsing. Layout: selected purchase/customer header; source transaction evidence branching into Loyalty and Analytics receipts; compact per-customer sequence table below. Tabs/sections for delivery evidence and HTTP idempotency, not separate pages.

State vocabulary: source committed; event enqueued; receipt observed; not yet observed; unavailable; unsupported for this event type. A receipt proves the handler's database transaction committed, not that its subsequent Kafka offset commit succeeded. Broker delivery is logically implied by consumer processing, but its exact time/partition/offset remains unobserved here.

Never animate completion based on elapsed time, global lag=0, connector health or source outbox flags. “Still waiting” after a deadline is not a definite processing failure. Preserve independent timestamps; no fabricated cross-service atomic snapshot.

Ordering demonstration: create a small same-customer burst, display allocated sequences and observed Loyalty sequence. Sequence advancement is a projection checkpoint; distinguish it from exact event receipts, especially after replay cutover. Do not sum partition offsets or claim global ordering.

Consumer duplicate demonstration is optional and initially disabled: existing test-duplicate accepts arbitrary caller-supplied IDs/payload and can create a new business event. To enable safely, resolve an existing source event server-side and republish its exact envelope. Even then a stable account and one receipt do not prove the second delivery was attempted; per-event duplicate evidence would require extra instrumentation. Keep aggregate duplicate counters/log evidence in Grafana rather than claim exact success from an unchanged balance.

### Replay & Correction

Purpose: invalidate a real purchase, compare recomputed staging, explicitly promote corrected Loyalty state. Layout: actual workflow steps with current run status; side-by-side live/staged comparison; all staged customers table; final scope review.

Flow: choose existing purchase → invalidate via detail → observe its Loyalty receipt and correctionRequired/sequence barrier → start bounded replay → fetch run/staging/comparison → review scope → optionally cut over → refetch every relevant state. No fixed sleep substitutes for the barrier. Do not capture replay boundaries before the invalidation has reached the consumer.

Generate and retain a run ID before POST. The current POST awaits the scan; while outstanding display “Replay request in progress,” not fake percentage progress. GET-by-ID can recover a known persisted run after an uncertain response. A transient404 can mean startup has not persisted it yet; do not automatically start a second replay. The server does not persist scannedCount continuously, so do not promise a live progress bar. Browser abort does not cancel server replay.

Show failed retention/invalid-history errors and retain the run ID for investigation. Do not include synthetic retention failure in the normal workflow. Real poison injection is excluded because no real redrive path exists.

Real cutover is included in the agreed plan. Preserve the existing handler's semantics. Its read-then-upsert sequence check is not visibly an atomic compare-and-write against concurrent live worker writes; this is a concurrency concern to validate, not permission to change it. A UI pause is not sufficient concurrency protection. If investigation proves a fix necessary, present the exact race, reproduction and proposed locking/conditional-write behavior to the user before implementing that change. Do not silently add new cutover guards, preview tokens, per-customer promotion or a different replay mechanism.

The confirmation must state that promotion covers eligible staged customers across the run, not just the selection. Analytics remains unchanged. Repeated cutover currently returns appliedCustomersCount0; render as already applied, not newly applied zero. The existing result only reports an applied count, not exact applied/skipped IDs: never invent per-customer outcomes from that count or from later reads. An additive result-detail extension is a proposal for discussion, not an assumed existing capability.

#### Mandatory global review and confirmation

Keep two visibly distinct areas: **Selected-customer correction preview** (a comparison filter) and **Global cutover for this replay run** (the actual mutation). Changing customer selection must never narrow the cutover request or the review table.

Build the review from existing GET staging plus the existing comparison endpoint for EVERY staged customer, with bounded concurrent reads. Do not truncate review to the selected customer or the old UI's first15 rows. Pagination in the UI is fine, but all customers must be fetched and accounted for before confirmation is enabled. If any comparison is unavailable, show incomplete review and disable confirmation until retried. A new bulk read endpoint is optional only if actual scale warrants it.

For each customer show live → staged points, tier, eligible spend, sequence and correctionRequired → false; distinguish a newly created account, eligible update (including numerically unchanged values), and expected skip. Mirror existing eligibility exactly: absent live account or live sequence <= staged sequence is eligible; live sequence > staged sequence is skipped. Do not filter out unchanged rows: the handler still writes them and clears correctionRequired. Show staged customer count, expected eligible count and expected skipped count, plus the run ID and review timestamps.

Confirmation text: “Apply this replay run globally to all eligible staged customers. The selected customer does not limit this action.” Require an explicit acknowledgement and a separate **Confirm global cutover** action. Display expected changes directly in the review, not only behind an external link. Refresh the full review on opening confirmation; if pushed updates reveal changed relevant state, invalidate the acknowledgement and require review again. No automatic POST after opening the dialog.

The preview is observational, not a frozen transaction: live writes can occur between review and execution. State this directly; the current backend rechecks eligibility during execution. Exact preview-to-execution guarantees would require a semantics change and must be discussed first. Preserve current behavior for incomplete run, empty staging, all-skipped run, partial application and repeated cutover. After POST show the actual returned applied count and re-read run/comparisons/accounts; describe these as post-action observations, not an exact per-customer execution audit.

### Consumer Controls

Purpose: demonstrate eventual consistency and recovery using supported application controls. Layout: Loyalty and Analytics side by side, each with fresh status, pause/resume and processing-delay controls; current affected customer context below. Preserve an evident “Resume” action after navigation.

Only show confirmed control state after response/refetch. Pause does not undo an in-flight database transaction, so a final update after pause is possible. Warn when status is stale; do not imply that an HTTP health response proves the consumer is processing. Control state is process-local and may reset on restart.

Place existing partition-specific and Loyalty replica controls behind an advanced disclosure only if needed for the seminar; no generic group admin, offset seek, topic scaling or broker controls. Replica UI must say “consumer instances in this worker process.”

Failure section explains actual behavior: malformed records/handler errors withhold commit and emit logs/metrics. No DLQ count, retry countdown, skip or redrive button without a separately approved backend feature. Link Kafbat/Grafana for investigation.

## 4. API integration contract

All paths below are backend paths; prepend existing browser service proxies (`/api/producer`, `/api/loyalty`, `/api/analytics`, `/api/replay`). Encode path/query identifiers.

| Operation | Existing backend mapping |
|---|---|
| Create / burst / read / invalidate | POST `/api/v1/purchases`; POST `/api/v1/purchases/batch`; GET `/api/v1/purchases/:purchaseId`; POST `/api/v1/purchases/:purchaseId/invalidate` |
| Account | GET `/api/v1/loyalty/:customerId` |
| Recent receipts | GET `/api/v1/loyalty/processed-events` (limited sample only) |
| Analytics | GET `/api/v1/analytics/summary` |
| Worker control | GET `/api/v1/{loyalty|analytics}/consumer/status`; POST same prefix `/pause`, `/resume`, `/delay` |
| Advanced application controls | Existing consumer `/partitions/:partition/pause|resume`; Loyalty `/scale`, `/kill-replica`, `/rebalance-history` |
| Replay | POST `/api/v1/replay/runs`; GET `/runs/:runId`, `/runs/:runId/staging`, `/runs/:runId/comparison/:customerId`; POST `/runs/:runId/cutover` |
| CDC | Existing browser `/api/cdc/status` proxy (service health only) |

### Proposed minimal backend additions — not existing APIs

1. Producer GET `/api/v1/purchases?customerId=&cursor=&limit=`: stable cursor pagination by creation time plus ID, bounded page size, source-owned purchase summaries. No joins into worker tables. Include original event identity and invalidation identity by reading producer-owned data; specify separate completion and invalidation sequence fields. Add indexes only after inspecting existing schema/query plans.
2. Loyalty and Analytics GET `/api/v1/{context}/event-receipts?eventIds=...`: bounded validated ID list, query each context's own processed-event records with explicit consumerName filter; return eventId and processedAt for found records. Missing means not observed, not failed. Avoid exposing the entire shared processed-events table through a worker. These are reads, not a new event audit store.
3. Optional read-only policy metadata if tier thresholds need to stay server-authoritative without duplicating constants in the browser. Alternatively keep explanatory policy text covered by a contract test; business calculation still stays server-side.
4. Cutover: reuse existing staging/comparison/cutover endpoints for global review. Investigate concurrency safety without changing semantics; discuss any required fix or additive execution-result detail before implementation. No changes to Analytics semantics.
5. Optional durable replay history listing is deferred; MVP retains known run IDs in route/session state and allows explicit run-ID lookup. Do not call that a complete server history.

Follow controllers → query/command handlers → small ports → existing persistence adapters. Do not expand existing direct-Prisma controller patterns. Preserve compatibility of current endpoints and clients. No new general orchestration service or cross-context mutable model.

### Frontend state and synchronization

Use a small RealApplication context/reducer for selected IDs and bounded recent-action references. Keep server resource state in reusable hooks keyed by service/ID. No new global state library required. Simulation store remains independent. Session/local storage can remember preferences/IDs only; never treat cached account balances as current server truth.

User selected server-pushed updates. Use SSE for one-way change notifications and existing HTTP APIs for authoritative snapshots and commands. No periodic browser data polling and no server timer that polls the database disguised as SSE. Initial loads, notification-triggered reads, explicit refresh and reconnect reconciliation remain necessary. One shared subscription owner per service avoids a connection for every component. Expose connection status separately from resource freshness.

#### Proposed push implementation — subject to plan approval

Add an SSE endpoint to each existing service that owns the relevant reads; no new gateway/service, Kafka topic or frontend Kafka consumer. A small shared transport utility is reasonable, but resource notification contracts remain owned by their contexts. Prefer resource-invalidated messages carrying identifiers over publishing full business records or creating a second state store. On notification, coalesce affected reads and fetch the authoritative snapshots/receipts; this is event-triggered reconciliation, not interval polling.

For committed database changes, propose PostgreSQL LISTEN/NOTIFY emitted inside the existing persistence transactions. It addresses the actual cross-process case: replay writes Loyalty state, and future worker replicas can commit outside the process serving an SSE connection. An in-process emitter alone misses those changes. Notifications contain bounded resource identifiers, not raw payloads or credentials. Each service has one dedicated listener connection, separate from transaction-pooled Prisma operations, and fans out only its own resource notifications to connected browsers. No database tables are read by another context merely to deliver a notification. Final channel ownership, driver dependency and notification write locations must be confirmed against existing adapters before coding; this proposal is not an instruction to add database triggers or alter domain policy.

PostgreSQL delivers transactional NOTIFY after commit; startup must establish LISTEN before reading initial state. This supports source/projection evidence without emitting successes from rolled-back transactions. LISTEN/NOTIFY is a transient hint, not durable event history. See [PostgreSQL NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) and [LISTEN startup ordering](https://www.postgresql.org/docs/current/sql-listen.html). SSE is supported by the existing NestJS framework; use the installed version's API, not unverified new-version helpers from [current NestJS documentation](https://docs.nestjs.com/techniques/server-sent-events).

Notification coverage: producer purchase/outbox/invalidation commits; Loyalty account and processed-receipt commits; Analytics summary and processed-receipt commits; replay run transitions/staging commits and cutover account changes. In-memory application controls can emit locally after successful control operations, because their state is process-local. No duplicate-delivery-success message unless it has actual matching instrumentation. Replay progress only reports persisted run transitions; this does not introduce continuous scan progress or change replay execution.

Connection protocol: establish the service listener, attach the browser stream, send a ready/resync-required message, then load visible snapshots. Buffer/coalesce invalidations arriving during that load and reconcile again afterward. On listener reconnect, service restart or browser reconnect, always request fresh authoritative state; do not pretend Last-Event-ID offers durable replay without a durable notification log. Tag callbacks with a subscription generation to discard responses from old customer selections. One in-flight read per resource, with a dirty flag, avoids losing changes during an active fetch.

Use heartbeat comments for transport liveness only; they do not prove CDC or consumer health. If the database listener fails, mark push degraded and force stream resynchronization on recovery. Do not silently continue displaying a connected/healthy badge. Bound client queues; slow clients receive resync-required or disconnect rather than unbounded memory growth. Coalesce batch changes, bound subscription filters/IDs and connections, and close listeners/streams cleanly. Validate configurable heartbeat/reconnect limits at bootstrap. Local HTTP connections across the four services must be tested for browser connection limits; avoid extra tab/component connections and multiplex within each service.

Verify Vite proxy streaming without buffering and document reverse-proxy buffering/timeouts for any later deployment. A disconnected stream leaves last-known data explicitly stale; manual refresh remains available. Do not fall back silently to timer polling. Notification failure behavior must be explicit: transactional NOTIFY participates in the transaction and can affect its failure path, so test rollback/error handling and queue saturation. If that coupling is unacceptable, discuss a different delivery design rather than silently introducing a new durable outbox.

Use AbortController, request timeouts and a request generation key to prevent a slow response for customer A replacing customer B. GET retry may use capped backoff; no blanket retry for writes. Keep stale data visibly marked on read failure. Do not poll Kafka topic metadata across the entire app to determine business completion.

Service errors have status, message and structured detail. Distinguish404,409, validation errors, timeout and network outage. Disable only actions whose dependencies are unavailable. A frontend timeout on a write means outcome unknown; provide read/reconcile actions. Never invent rollback after an HTTP disconnect.

Deployment remains the existing Vite local seminar workflow initially. Its dev proxy is not bundled into static production output; document that limitation and require an explicit reverse-proxy/deployment decision before promising a static hosted app. Parameterize service origins and Kafbat/Grafana links through validated configuration and `.env.example`; never expose backend credentials via VITE variables.

## 5. Reuse and implementation structure

Suggested cohesive modules under `apps/demo-ui/src/real/`: `pages/`, `components/`, `state/`, `hooks/`; shared shell under `components/layout/`. Extend the existing API client with typed resource/error support and missing GET wrappers. Do not build generic CRUD/page factories.

Reusable small components: customer context bar, source purchase table/detail, state-with-freshness indicator, business evidence branches, before/after comparison, accessible confirmation dialog, page-level unavailable state. Reuse `ReplayComparisonCard`/`ReplayStagingTable` presentation after checking field semantics and theme. Preserve existing callers while replacing misleading text. Existing PipelineStageFlow is explanatory, not trustworthy event telemetry.

Accessibility: semantic nav and headings, aria-current, text alongside status colors, visible keyboard focus, announced action outcome, proper form labels, responsive tables, reduced motion. Main content pages should be visually distinct through purposeful layouts, not repeated cards with different headings. No continuous animation implying unobserved processing.

## 6. Ordered implementation and handoff checklist

No boxes below authorize implementation. Update each checkpoint with changed files, checks and remaining limitations when approved.

- [x] Inspect shell, API client, controllers, handlers, consumer processing, policy, replay and reusable UI.
- [x] Document actual capability gaps and proposed navigation.
- [x] Resolve the three user decisions in section7.
- [x] Obtain explicit implementation approval, including the proposed SSE notification source.
- [x] **Phase1 — contracts and safety:** add status-aware HTTP errors/GET-by-ID; confirm schema ownership; implement approved bounded history/receipt reads with controller/handler tests; analyze cutover concurrency and discuss any necessary semantic change before coding it. Acceptance: per-event evidence never relies on a latest30 list or global lag.
- [x] **Phase1b — server push:** approve notification-source design, implement transaction-aware invalidations and service SSE streams, then snapshot/reconnect reconciliation. Test rollback silence, cross-process updates, lost connection/restart recovery, subscribe/snapshot races, burst coalescing, slow clients and disposal. No timer-based business-state polling.
- [x] **Phase2 — shell:** persistent responsive sidebar, hash parser/context, legacy links, accessible drawer, existing simulation/benchmark/guide reachable. Verify canvas resize, browser history, direct links and simulation state/playback behavior after navigating away/back. Do not keep hidden simulation timers running accidentally.
- [x] **Phase3 — business vertical slice:** create purchase → read persisted source → observe actual Loyalty and Analytics receipts/state. Add history, tier progression, Analytics scope labeling and stale/error states. Acceptance: real fresh customer produces correct source and projection state without fabricated lifecycle stages.
- [x] **Phase4 — teaching evidence:** same-customer burst, HTTP duplicate/conflict exercise, event evidence, before/after observations, independent worker pause/delay. Exact consumer duplicate instrumentation is deferred unless separately approved.
- [x] **Phase5 — recovery:** invalidation barrier, replay request lifecycle/recovery by ID, selected-customer comparison, separate complete global review and explicit cutover confirmation using existing semantics. Acceptance: all staged customers reviewed, unknown comparisons block confirmation, relevant pushed changes require renewed review, actual result counts preserved, Analytics not falsely decremented. Discuss rather than assume richer per-customer outcome reporting.
- [x] **Phase6 — presenter polish and verification:** responsive/accessibility review, offline/partial-outage handling, walkthrough documentation, links and config. Update README/backlog status honestly and record known limits.

### Validation requirements

Unit/contract tests: routing/context serialization; exact cents parsing; HTTP error distinctions; cancellation/stale response protection; invalidation event identity; bounded query validation; explicit consumer filter; pagination; unavailable receipt versus absent receipt; no mutation retries; source acceptance versus handler evidence; replay state and partial promotion; schema/policy explanatory consistency.

Browser tests: preserve all current routes; keyboard/mobile sidebar; select customer across pages; navigate during an in-flight request; account404 versus503; offline view preserves visibly stale data; refresh/deep link restores selected context. Use existing test infrastructure where possible; document any required DOM/browser test dependency before adding it.

Real seminar acceptance run (isolated synthetic customer IDs, no reset of application data):

1. Fresh customer, purchase10,000 cents: expect10 points and Silver afterward; next1,000-cent purchase earns2 points. Verify actual server responses.
2. Repeat first request with same key/body: same purchase/event and no extra contribution; conflicting amount returns409.
3. Pause Analytics, create purchase, observe Loyalty advance while Analytics has not observed that completion; resume and observe catch-up. Restore original controls after testing.
4. Submit small same-customer burst; verify allocated sequence and eventual projection checkpoint without claiming global Kafka order.
5. Invalidate a known purchase; wait for observed correctionRequired and invalidation receipt; replay; compare staging; if approved safely promote, verify actual corrected Loyalty and unchanged gross Analytics semantics.
6. Fail a service/read; show unavailable instead of zeros, fabricated success or “no account.” Exercise concurrent live progress versus cutover in an automated integration test before the live demonstration.

Run repository format/lint/typecheck/unit gates and frontend build after implementation. Run infrastructure verification only when the intended application Kafka stack is active, documenting any generated verification topic. This planning pass does not claim a fresh runtime certification.

## 7. Recorded decisions and remaining approval boundaries

1. **Approved planning scope:** minimal durable purchase-history and context-owned receipt read APIs. Remove the session-only-history fallback from the intended MVP; session references remain useful UI conveniences only.
2. **Approved planning scope:** server-pushed updates despite added backend work. SSE plus transactional PostgreSQL notifications is the proposed concrete design for review; it is not yet implemented or separately approved. No periodic browser/database refresh loop.
3. **Approved planning scope:** include real global cutover, reuse existing semantics, show every staged customer's expected change/eligibility, distinguish selected-customer preview, and require explicit confirmation.
4. **Not authorized:** changing cutover eligibility, transaction behavior, replay mechanics, adding preview/execution locking guarantees or narrowing cutover to a customer. Discuss any such proposal first. Additional execution-result fields are also proposed, not assumed.

The three product choices are resolved. Implementation still requires explicit approval. No implementation, migration, infrastructure change or runtime business operation was performed while updating this plan.

Implementation and local acceptance completed incrementally; consolidated evidence and operational limits are in C13 of the progress document. Historical proposal/approval boundaries above are superseded by recorded user approvals; no further semantic changes are implied.

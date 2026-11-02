# Real Application implementation progress

Updated: 2026-11-02. **Approved local seminar UI implemented and acceptance exercised.** Source plan: [REAL-APPLICATION-UI-PLAN.md](../REAL-APPLICATION-UI-PLAN.md).

## Overall status

- Approved local UI phases1–6 implemented: source/history/receipts/push, coordinated cutover locking, sidebar/routes, purchases/idempotency/burst/evidence, projection pages/observations, independent controls, replay/global review/confirmation, configuration and presenter guide.
- Browser acceptance exercised actual writes/push, duplicate/conflict responses, mobile keyboard/focus/375px overflow, desktop canvas resize/legacy routes, true partial outage/reconnect, deliberately delayed mutation navigation, Analytics independence/catch-up and isolated partial global promotion.
- All disposable cutover fixture rows cleaned by exact owned IDs. The95-customer retained replay was not promoted. Synthetic source/outbox history remains intentionally retained.
- Local scope delivered. Known limits: one active Demo UI tab recommended on Vite HTTP/1; static production proxy/HTTP2 deployment is not configured; no durable event timeline or exact per-customer cutover result API; non-fatal bundle warnings remain. No claim of production load or notification-queue saturation certification.
- Unrelated benchmark edits remain unstaged. No push requested/performed.

## Decisions and boundaries

- User approved the existing plan and requested scoped commits, incremental validation and updates to this document after each milestone.
- Minimal history/receipt reads; server push via the approved SSE/transactional PostgreSQL notification design, not timer polling.
- Real global cutover must review all staged customers and require explicit confirmation. Selected-customer comparison does not constrain cutover.
- Preserve source/consumer business rules, CDC, existing simulation and benchmark functionality. No infrastructure administration UI or fake lifecycle states.
- User explicitly approved coordinated database locking and concurrency tests after the read-then-upsert race was explained. Preserve existing eligibility, all-customer scope and response semantics. No customer-scoped cutover or new replay mechanism.
- The working tree started with many unrelated simulation, benchmark and README/backlog changes. Do not stage or revert them. Stage explicit task files only; stage only task-specific documentation hunks for mixed files. No push requested.

## Completed checkpoints

### C0 — initial handoff

- Read approved plan, task attachment, AGENTS.md and actual working tree.
- Identified existing read controllers/adapters and Prisma schema; no real-workspace implementation already present.
- No runtime tests claimed at this documentation-only checkpoint.

## Current work and exact next steps

The local feature is delivered. Read `docs/real-application-walkthrough.md` to present it. Do not restart implementation from historical incomplete checkpoints below. Future work should be explicitly scoped (for example deployment, automated DOM browser regression tests or multi-tab streaming), rather than changing replay semantics.

Keep the retained95-customer replay unpromoted unless intentionally reviewed for a real seminar promotion. Tests may create only owned disposable replay fixtures and clean exact IDs. Current application listeners are running; Loyalty was restored after outage testing (PID29688 at observation). Use normal `pnpm stop` when shutting down the lab.

Resume protocol: read plan and this document, inspect `git status`/recent commits, reconcile code with these statements, continue first incomplete milestone. Do not mark integration complete based only on mocks.

### C1 — real read evidence foundation

- Producer: `GET /api/v1/purchases?customerId=&cursor=&limit=`; optional customer filter, default25/max100, stable descending creation-time/ID order, stored-row cursor, invalid/mismatched cursor400. Separate completion/invalidation IDs and sequences; integer values in this new DTO are strings. Existing GET-by-ID remains compatible.
- Loyalty/Analytics: `GET /api/v1/{context}/event-receipts?eventIds=...`, 1–100 validated IDs. Context-specific persistence identity filter (`loyalty-v1`/`analytics-v1`) matches current handlers. Missing receipt is absence of evidence, not failure. No cross-context queries or new tables.
- Controller → handler → port → adapter, registered in existing modules. Shared receipt wire-input parser in contracts; no shared mutable domain model.
- UI: `api/http.ts` retains HTTP status/details and never retries mutations; account404 alone returns null. `api/business.ts` supplies canonical purchase/history/receipt reads with cancellation and encoded IDs. No UI page integration yet.
- Tests: format check, lint, workspace typecheck passed; unit suite **55 files /236 tests passed**. Three new read-only HTTP/PostgreSQL integration tests passed, using ephemeral local HTTP listeners and existing database rows; no consumer loops or database mutations started.
- `pnpm kafka:verify` passed on active app infrastructure, including creation/deletion of its own `p002.verification.test` topic and Kafbat health.
- `pnpm --filter @lab/demo-ui build` failed exclusively in pre-existing simulation edits: missing broker `icon` fields across blueprints; missing `SimKafkaBrokerData` import and too-narrow inferred data type in `useSimulationStore.ts`. These files were not modified by C1. Do not report full frontend build success.
- Live integration first attempt lacked ConfigModule in two isolated test harnesses; corrected harness imports, rerun all3 passed. No application wiring change needed.
- No migrations, business mutation, live cutover or schema changes. Full feature remains incomplete.

### C2 — approved coordinated cutover locking

- User explicitly approved coordinated locking and concurrency tests. Live Loyalty unit-of-work now takes a transaction-scoped advisory lock before reading each account; replay cutover takes the same lock before checking eligibility/upserting. Locks cover missing accounts too. Both require the existing shared Loyalty database.
- Cutover takes a per-run transaction lock and reads run status inside that transaction, serializing duplicate requests. Staged customers are ordered by customerId to prevent lock-order inversion across overlapping runs.
- Scope, eligibility (`live sequence > staged sequence` skips), all-skipped rejection, points/tier calculation, response shape and global promotion are unchanged. A lock hash collision only adds serialization; external SQL writers that bypass this protocol remain outside its guarantees.
- Modified `postgres-loyalty-unit-of-work.adapter.ts`, `cutover-replay-run.handler.ts` and corresponding unit tests; added `cutover-locking.integration.spec.ts`.
- Four real PostgreSQL tests pass: live-first skips obsolete staging; cutover-first preserves later live update for existing and absent accounts; duplicate cutovers apply once. Random test-owned accounts/runs/staging were removed by exact owned IDs; no application history was reset and no Kafka messages published.
- Existing five cutover unit tests pass; full unit suite remains236 tests/55 files. Format/lint passed. A subsequent typecheck caught missing response casts in the new C1 integration test; corrected those test types and reran workspace typecheck successfully.
- Browser build still blocked by the unrelated pre-existing simulation errors documented above. Full Real Application feature is incomplete.

### C3 — producer push checkpoint complete

- Shared `packages/observability/src/live/postgres-change-feed.ts` accepts an existing pg connection factory; no new package dependency. One dedicated LISTEN session per service, coalesced invalidations, bounded clients, disconnect on backpressure, heartbeat-only timer, reconnect and resnapshot semantics. No database polling or durable SSE event-ID claims.
- Producer `GET /api/v1/purchase-changes` exposes SSE; its unit-of-work issues transactional NOTIFY before commit. This is a transient invalidation, not CDC publication or processing success. Channel/timing/client limits are Zod-validated bootstrap settings documented in `.env.example`.
- Four transport unit tests and one live PostgreSQL→SSE→HTTP integration test pass. The live test creates exactly one random `real-ui-push-*` purchase per execution, retaining its source/outbox evidence rather than retracting CDC history. It also verifies cross-session notification, rollback silence and fresh initial invalidation on SSE reconnection. No browser UI is integrated yet.
- Workspace typecheck and observability build pass. Full format/lint/typecheck passed; unit suite56 files/240 tests passed. Tests do not yet cover a real database-server disconnect, browser EventSource through Vite, worker/replay changes or consumer-control updates.
- Exact next task: finish/record gates, commit C3; then reuse this feed for Loyalty/Analytics and replay (including cross-process cutover → Loyalty invalidation), implement subscription/reconciliation hooks, and verify those paths before shell/page expansion.

### C4 — projection/replay push and browser primitives

- Reused feed for `/api/v1/loyalty-changes`, `/api/v1/analytics-changes`, `/api/v1/replay-changes`. Each uses its own context database/channel. Environment defaults documented; no dependency added.
- Loyalty/Analytics units-of-work emit transactional invalidation; replay run/staging writes emit with their own commit. Cutover emits to both replay and Loyalty channels in its existing transaction. Application control POST success emits a local hint; GET/error paths do not, avoiding feedback loops.
- Browser `real/state/change-subscriptions.ts` shares one EventSource per active service; `resource-observer.ts` coalesces dirty reads, cancels obsolete reads and retains explicitly failed snapshots. `useLiveResource` wires these without timer polling and prevents old-customer snapshots from rendering after selection change. No pages mounted yet; no browser/Vite validation claimed.
- Focused validation: both worker unit-of-work→HTTP SSE integration checks passed; cross-process replay run/staging/cutover notification test passed; four cutover concurrency tests still pass; three subscription/reconciliation tests and control-interceptor test pass. Worker notification tests use empty transactions, not live Kafka processing. Replay test affects only random owned fixtures and cleans those exact IDs.
- Full gates: format/lint/typecheck passed;58 files/244 unit tests passed. A new replay integration fixture initially used number instead of bigint; corrected and reran both typecheck and that live test successfully. No outstanding error introduced by this checkpoint is known.
- Next: sidebar/pages and actual browser integration. Existing simulation build errors remain; user was asked whether to repair and separately commit the pre-existing simulation edits or leave them for their owner. Do not fold those unrelated changes into a Real Application commit without that decision.

## Commit checkpoints

- `cc86217`: approved plan and initial progress file.
- `92c7b61`: durable purchase history, receipts, HTTP evidence and read-only integration checks.
- `c19a2c1`: user-approved coordinated cutover/Loyalty locking and concurrency tests.
- `beef746`: producer PostgreSQL→SSE flow and live HTTP validation.
- C4 commit: projection/replay push and browser reconciliation primitives (find by its descriptive commit subject in `git log`).

No commits pushed. Unrelated simulation/benchmark edits remain unstaged. Existing running services were not restarted, so running app processes may still execute previous builds. Build/restart the selected services before browser acceptance testing; preserve databases and Kafka history.

### C5 — separately approved simulation repair

- User authorized repair and separate commit of the pre-existing Simulation Playground work. Includes its broker nodes, poison-message controls and associated simulation changes; benchmark edits remain excluded.
- Added required broker icons to blueprint data; imported the missing broker type and explicitly typed palette initial data. Fixed palette callback dependencies so broker-ID allocation uses the current node list.
- Frontend production build now passes. Format/lint/workspace typecheck and244 unit tests passed. Build emits non-fatal bundle-size and React Flow module-directive warnings. No browser behavior certification claimed yet.
- This resolves the build blocker for Phase2. Simulation changes belong in their own commit, not the Real Application shell commit.

### C6 — sidebar and read-only overview checkpoint

- Persistent sidebar preserves legacy hash entry points and customer/purchase/run selection context. Includes desktop collapse and mobile drawer/focus handling; explicit accessible link names. Only implemented real route is `#real/overview`.
- Overview reads actual purchase history, selected-customer Loyalty state and global Analytics via existing APIs and shared push hooks. Loading, missing account, service errors, disconnected/stale snapshots and manual refresh are explicit. Source status never asserts downstream completion; Analytics totals are labelled gross.
- Existing periodic legacy telemetry runs only on the existing benchmark page. Removed unsupported partition-count/single-node claims from shared footer.
- Production build passed (existing non-fatal bundle-size and React Flow directive warnings). Browser confirmed existing Analytics totals and honest unavailable history/disconnected streams against old running services. No service restarted or business mutation performed during this check.
- Added route tests for legacy/fallback behavior and encoded context round-trips. Final quality gates recorded below after execution.
- Incomplete: fresh-backend browser push verification, responsive acceptance, purchase actions/detail receipts, dedicated business/controls/recovery pages. This is a resumable checkpoint, not completion of the approved plan.

- C6 final gates: format/lint/typecheck passed; 59 test files / 246 unit tests passed; kafka:verify passed, including owned test-topic lifecycle and Kafbat health. Frontend production build passed before the final label/footer-only adjustments. Full application UI remains incomplete.

### C7 — fresh service verification and purchase entry/history

- Compiled all four backend services and restarted only verified application processes. Docker/data preserved. Full `pnpm build` encountered Windows Prisma DLL locks held by running workers; schemas unchanged, direct backend TypeScript compilation passed. Frontend production build passed.
- Actual browser through Vite showed all three mounted push feeds live and persisted history available. Selected isolated customer initially had no account/history. Submitted one synthetic 10,000-cent purchase through HTTP: `pur_01a19b7c-a150-7453-9f3a-fba947cc5cc0`. Real HTTP reads verified Silver /10 points and both independent completion receipts. This source history is intentionally retained. Browser automation then became unavailable (`browser.user is unavailable`); post-mutation browser refresh and mobile acceptance remain unverified.
- Added `#real/purchases`: exact decimal cents parsing, retained editable idempotency key, explicit new-intent action, no automatic retry, HTTP status/uncertain outcome reporting, cursor-paged source history and canonical detail. Reuses existing APIs and push hooks. Key is page-local; UI tells users to retain it before navigation after an uncertain outcome. No new backend semantics.
- Format/lint/typecheck passed;60 files/247 unit tests passed; Kafka verification passed. Production frontend build initially caught inferred UUID state type; fixed with explicit string state and build passed. Existing bundle-size/directive warnings remain.
- Remaining: purchase invalidation confirmation, separate original/invalidation evidence, receipt UI, burst exercise, dedicated projection pages, teaching controls and recovery/global review. Browser acceptance of new purchase form is pending. Do not mark Phase3 complete.
- Exact next task: inspect `real/Purchases.tsx`, finish purchase evidence/receipts and invalidation with confirmation using canonical history event identities; then verify UI mutations and SSE updates in an available browser. C6 responsive acceptance also remains pending. Running services are fresh builds; do not repeat restarts unless code requires them.

### C8 — purchase evidence and confirmed invalidation

- Added `real/PurchaseEvidence.tsx`, mounted from Purchases using persisted history identities. Separate original completion/invalidation event IDs and sequence strings; independent Loyalty/Analytics receipt reads via shared pushed subscriptions. Errors, absent receipts and stale connections remain distinct. Analytics invalidation is explicitly unsupported by design; receipts prove database handling, not offset commit.
- Invalidation form requires purchase-specific acknowledgement and separate submit, disables duplicate clicks, preserves actual repeated-invalidation result and reports unknown network outcomes without automatic retries. Reason choices match existing server normalization (DEMO_INVALIDATION / FRAUD_CONFIRMED). No backend change.
- Live HTTP test invalidated only C7's synthetic purchase and repeated it: first returned false, second true for isAlreadyInvalidated with identical event ID `01a19b81-bca6-7688-922e-b7bc6e06d6e9`, sequence2. History still preserves completion event/sequence1 separately. No cutover performed. Source history retained.
- Gates: format/lint/typecheck and60 files/247 unit tests passed; frontend production build and kafka:verify passed. Existing bundle warnings remain. Browser interaction acceptance remains pending because prior automation was unavailable; HTTP tests do not certify UI confirmation/freshness behavior.
- Limitation: evidence/control panel requires the selected purchase on the displayed history page. Canonical source detail supports deep links, but arbitrary deep-link evidence lookup remains unfinished. Do not infer original event identity from legacy detail's latest event field.
- Next: resolve selected-purchase history evidence independently of pagination with a minimal read API extension; verify confirmation and pushed receipt updates in browser. Then continue approved dedicated Loyalty/Analytics pages, teaching controls and recovery/global review. Full feature remains incomplete.

### C9 — expanded business and control workspace

- Added exact optional `purchaseId` filter to bounded source-history query/port/adapter, with regression test. Purchases and new Event Evidence page resolve event identities independently of history pagination. Existing routes/filters remain compatible.
- Added Loyalty Accounts, global Sales Analytics, Event Evidence and Consumer Controls routes/sidebar entries. Loyalty explains existing thresholds and shows Gold progress; Analytics shows actual last-processed time and global gross semantics. Controls use actual status plus pause/resume/delay POSTs, preserve independent groups, and label unsupported poison/DLQ flows. Delay UI intentionally limits presenter input to0–30,000ms.
- Fixed missing read-side Loyalty fields: PostgreSQL DAO exposes existing lastCustomerSequence as a decimal string and correctionRequired; handler/result/DTO and in-memory adapter carry them. Existing financial totals unchanged. UI distinguishes unknown correction status; legacy wait barrier compares sequence via BigInt. No domain/persistence mutation semantics changed.
- Browser automation recovered after reset. Verified purchase deep-link evidence and all three existing receipts; actual UI submission created `pur_01a19b86-fc59-7d31-b983-45e9ddc1ae37` (1,000 cents, sequence3) for owned synthetic customer. Push updated both receipts without manual refresh. Same-key retry retained identity; changed amount returned visible HTTP409. UI Analytics pause showed paused partitions; resume restored no paused partitions; Loyalty remained active. No cutover performed.
- Compiled/restarted only producer and Loyalty to serve additive read changes. No data reset. Gates format/lint/typecheck/frontend build/kafka verification passed; unit suite60 files/248 tests passed after updating DTO and DAO fixtures for additive fields (including sequence beyond Number safe integer).
- Remaining: same-customer burst; before/after observation; recovery/global review; responsive and offline/browser acceptance; configured external links. Full plan remains incomplete. Dedicated projection pages are implemented but their full visual/accessibility acceptance is pending.

### C10 — same-customer burst teaching exercise

- Added `real/PurchaseBurst.tsx` to Purchases. Fixed selected customer and exact fixed amount;1–500 records,100–10,000,000 cents. Uses existing batch API. Results show actual allocated sequences/event IDs and evidence links. Batch outcome ambiguity and lack of idempotency are explicit; no automatic retry.
- Browser submitted exactly3 records for owned synthetic customer, returned sequences4/5/6, and source history updated. Source fixtures retained. Frontend build, format/lint/typecheck and248 unit tests passed; active Kafka verification passed in C9 immediately before this frontend-only slice.
- Remaining: recovery/global review, before/after observation, responsive/accessibility and partial-outage acceptance, configured links. Next phase is replay lifecycle/barrier and global all-customer review, retaining approved locking and unchanged cutover semantics.

### C11 — replay lifecycle and complete global confirmation

- Added Recovery route with observed invalidation receipt + actual account correction/sequence barrier before launching replay. Run ID generated/retained before POST, preserved in URL, recover-by-ID GET, explicit uncertain outcome/startup404 explanation, persisted run state without fabricated progress. Browser abort/navigation does not claim server cancellation.
- Complete staging review fetches every comparison with at most4 concurrent reads; no selected-customer scope restriction/truncation. Exact existing eligibility (absent/equal/older live eligible, newer skipped), unchanged eligible writes included. Missing/failed comparisons and unsafe numeric read precision block confirmation. Separate selected-customer preview and all-customer tables show points/tier/spend/sequence/correction changes.
- Opening global confirmation refreshes the whole review. Explicit acknowledgement plus separate POST button required; loading/errors/disconnected feeds invalidate readiness, pushed Loyalty/replay reads revoke acknowledgement. Preview is observational; backend rechecks eligibility. Result displays actual applied count only, then rereads state; no invented per-customer execution outcomes.
- Browser launched bounded replay `11c39b6c-b5e2-4393-97f5-a3af185ea6a2`: COMPLETED,297 scanned,95 staged,90 eligible/5 skips at observation. Verified opening confirmation disables controls during refresh, explicit acknowledgement enables final button, refreshing revokes acknowledgement and disables it again. Did NOT execute global cutover because run includes existing lab customers; acknowledgement was revoked before leaving.
- Frontend build, format/lint/typecheck and250 unit tests passed. Pure tests cover equality/newer/missing/unsafe sequence eligibility, full37-customer bounded reads and failure rejection. Seven focused tests passed including four live PostgreSQL cutover concurrency cases and cross-process replay notification. No backend replay semantics changed; owned integration fixtures cleaned by tests.
- Remaining Phase6 items are listed at top. Existing bundle-size and React Flow directive warnings remain non-fatal. Full plan is not yet certified complete.

### C12 — public configuration, bounded HTTP requests and presenter guide

- Added Zod-validated public UI settings for existing Kafbat/Grafana links and HTTP timeout; dedicated UI Zod dependency recorded in package/lockfile. Root `.env` now loaded by Vite through envDir. Defaults and restart/build behavior documented in `.env.example`. Proxy service origins are configurable/validated in Vite; no credentials exposed to browser config.
- HTTP requests combine caller cancellation with a bounded default120s deadline, including response-body reading, and clean listeners/timers afterward. No automatic retries. Timed-out writes/replay remain explicitly uncertain; server may continue. Added deadline/no-retry, cancellation, conflict-details and config validation tests. Existing signal test updated to assert transport signal presence; cancellation propagation separately verified.
- Sidebar links open existing tools, mobile closed nav is hidden from keyboard/accessibility traversal, and mobile route navigation closes drawer/focuses main. Dropdown/checkbox styling matches workspace. Browser acceptance of these changes remains pending: IAB creation timed out attaching and inventory returned no tabs; no native/browser fallback or service outage attempted.
- Added `docs/real-application-walkthrough.md`: real purchase/tier/idempotency/burst/independent-group/correction walkthrough, global scope warning, state vocabulary, uncertain outcomes and local Vite versus static deployment limits.
- Gates: frontend production build passed; format/lint/typecheck passed;63 files/253 unit tests passed after correcting the legacy signal-identity assertion; Kafka verification passed. Existing non-fatal bundle-size/React Flow warnings persist. No business mutation or cutover during this checkpoint.
- Exact remaining acceptance: browser mobile drawer/focus/legacy routes/canvas resize; partial-service outage/reconnect preserving stale data; pushed review acknowledgement invalidation and isolated cutover error/uncertain-response UI. Optional before/after observations remain unimplemented. Read the presenter walkthrough and C11 safety context before runtime testing. Full approved feature is implemented substantially but not yet certified complete.

### C13 — consolidated final acceptance and fixes

- Fixed actual Vite SSE interruption bug: upstream abort/error/incomplete close now destroys downstream stream so native EventSource reconnects. Durable regression test starts isolated real Vite/upstream HTTP servers and verifies interrupted-stream teardown. True Loyalty process outage showed disconnected +502, retained18-point stale snapshot, unaffected source history, then automatic live resnapshot after restart without manual refresh. Service restored.
- Fixed375px horizontal overflow from grid/table intrinsic sizing; tables stay in scroll containers, long text wraps. Browser verified width fit, drawer opening focus, Shift+Tab wrap, Escape restore, route selection focus to main, desktop sidebar/canvas resize, Guide/Benchmark compatibility, and simulation stops/unmounts when leaving (return starts a fresh stopped simulation, preserving existing behavior).
- Scoped service subscriptions to visible pages. Added action-lifetime/navigation guards and propagated read cancellation. Actual source-row lock held only owned synthetic customer while submitting a100-cent purchase; navigated away with request visibly pending; after successful commit (sequence7), browser stayed on Guide. No hidden redirect. Source history intentionally retained.
- Session-persistent per-customer purchase amount/key with schema validation and honest storage-failure fallback; browser verified navigation+refresh retained exact key/amount. Added before/current read observation tables and explanatory overview flow/shortcuts. Header's existing Kafbat link now uses validated config. Event Evidence includes bounded paginated customer events ordered with BigInt and separate invalidation identities. Added contract check between visible policy explanation and pure backend thresholds/multipliers.
- Isolated manual UI cutover fixture:3 random owned customers,2 eligible and1 newer-sequence skip. Full review showed3/2/1; actual pushed Loyalty NOTIFY revoked acknowledgement; changed run status caused server rejection and refreshed FAILED view; restored fixture and executed UI confirmation, server returned applied2/CUTOVER_COMPLETED. DB verified both eligible customers promoted and skip untouched. All3 accounts/run/staging removed by exact owned IDs; no source records/Kafka messages for this fixture. Existing95-customer replay untouched.
- Independent-group test: UI paused Analytics; new owned synthetic customer purchase showed Loyalty receipt and Analytics not-yet-observed; after resume, Analytics receipt appeared through push. Restored Analytics not paused with delay0. Owned source history retained.
- Validation: frontend production build, format/lint/typecheck and67 files/257 unit tests passed; six focused real history/cutover/push integration tests passed; Kafka verification passed with owned verification-topic cleanup. Existing bundle-size/React Flow warnings remain. Local single-tab acceptance exercised; ambiguous network outcomes remain conservatively labelled and require reconciliation, never automatic write retry.

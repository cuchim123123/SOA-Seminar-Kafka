# Real Application implementation progress

Updated: 2026-11-02. **Implementation authorized; incomplete.** Source plan: [REAL-APPLICATION-UI-PLAN.md](../REAL-APPLICATION-UI-PLAN.md).

## Overall status

- Current phase: Phase1b backend push and browser primitives complete; next is Phase2 shell integration.
- Completed: bounded purchase history, context-filtered receipts, status-aware HTTP client and canonical read wrappers; unit and live read-only HTTP integration checks.
- In progress: preparing shell integration. All service streams, transactional projection/replay notifications and browser subscription/reconciliation primitives are implemented and tested. Hooks are not mounted in a page yet. Simulation build repair/commit scope question is pending.
- Remaining: Phase1 APIs/client; Phase1b transactional notifications/SSE and one verified end-to-end flow; Phase2 sidebar; Phase3 business pages; Phase4 teaching controls; Phase5 global recovery review; Phase6 browser/runtime validation.
- Blockers: none currently. User approved repairing/committing the pre-existing simulation changes separately; frontend build repaired (C5). Global review/confirmation UI is still required before exposing promotion.

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

1. Simulation repair/commit was approved and completed (C5). Implement Phase2 persistent sidebar/hash routes, preserving Simulation/Benchmark/Guide and mounting the real-workspace resource hooks. Do not repeat completed backend work.
2. Mount the shared subscription pool via `useLiveResource`; verify real EventSource through Vite and dependency outages in an actual browser. Backend/direct HTTP tests do not certify this yet. Implement business pages sequentially afterward.
3. Cutover locking was approved and implemented; do not rework it. Retain global review/explicit confirmation requirements when building recovery UI.
4. Before frontend shell integration, reconcile the pre-existing simulation build failures without reverting the user's ongoing changes. `pnpm typecheck` does not run the same referenced app configuration as `pnpm --filter @lab/demo-ui build`; run both.

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

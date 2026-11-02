# Real Application implementation progress

Updated: 2026-11-02. **Implementation authorized; incomplete.** Source plan: [REAL-APPLICATION-UI-PLAN.md](../REAL-APPLICATION-UI-PLAN.md).

## Overall status

- Current phase: Phase1 read foundation complete; next is Phase1b server push.
- Completed: bounded purchase history, context-filtered receipts, status-aware HTTP client and canonical read wrappers; unit and live read-only HTTP integration checks.
- In progress: preparing Phase1b server push. User-approved coordinated cutover/Loyalty locking implemented and validated (C2).
- Remaining: Phase1 APIs/client; Phase1b transactional notifications/SSE and one verified end-to-end flow; Phase2 sidebar; Phase3 business pages; Phase4 teaching controls; Phase5 global recovery review; Phase6 browser/runtime validation.
- Blockers: existing unrelated simulation edits fail the frontend build (see C1). Cutover concurrency tests now pass; global review/confirmation UI is still required before exposing promotion.

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

1. Begin Phase1b with one producer-owned transactional notification → SSE → authoritative history-read flow. Existing `pg`/types dependencies are already present in the services. Inspect source unit-of-work and configuration modules before changes; do not introduce polling or a new service.
2. Test commit visibility, rollback silence, listener reconnect/resync and bounded stream behavior before adding worker/replay notifications.
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

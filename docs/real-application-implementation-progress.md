# Real Application implementation progress

Updated: 2026-11-02. **Implementation authorized; incomplete.** Source plan: [REAL-APPLICATION-UI-PLAN.md](../REAL-APPLICATION-UI-PLAN.md).

## Overall status

- Current phase: Phase1, read contracts and HTTP evidence foundation.
- Completed: repository/plan reconciliation and initial persistent checkpoint.
- In progress: inspecting existing read adapters, DTOs and tests before implementing bounded history and receipts.
- Remaining: Phase1 APIs/client; Phase1b transactional notifications/SSE and one verified end-to-end flow; Phase2 sidebar; Phase3 business pages; Phase4 teaching controls; Phase5 global recovery review; Phase6 browser/runtime validation.
- Blockers: no implementation blocker yet. Cutover concurrency requires investigation before enabling promotion; do not change its semantics without discussing findings with the user.

## Decisions and boundaries

- User approved the existing plan and requested scoped commits, incremental validation and updates to this document after each milestone.
- Minimal history/receipt reads; server push via the approved SSE/transactional PostgreSQL notification design, not timer polling.
- Real global cutover must review all staged customers and require explicit confirmation. Selected-customer comparison does not constrain cutover.
- Preserve source/consumer business rules, CDC, existing simulation and benchmark functionality. No infrastructure administration UI or fake lifecycle states.
- Current cutover read-then-upsert concurrency concern remains unverified. No new locking, customer-scoped cutover or replay mechanism without discussion.
- The working tree started with many unrelated simulation, benchmark and README/backlog changes. Do not stage or revert them. Stage explicit task files only; stage only task-specific documentation hunks for mixed files. No push requested.

## Completed checkpoints

### C0 — initial handoff

- Read approved plan, task attachment, AGENTS.md and actual working tree.
- Identified existing read controllers/adapters and Prisma schema; no real-workspace implementation already present.
- No runtime tests claimed at this documentation-only checkpoint.

## Current work and exact next steps

1. Implement status-preserving HTTP errors and canonical purchase GET in `apps/demo-ui/src/api/`; distinguish account404 from outages. Add focused tests.
2. Implement bounded source-owned purchase history with stable cursor and separate invalidation identity, and bounded context-filtered receipt APIs. Follow controller → handler → port → adapter. Inspect existing Prisma generated-client setup before changing types.
3. Run focused tests, format/lint/typecheck/unit gates, update this document with exact results and commit the coherent milestone.
4. Proceed to Phase1b only after foundation checks pass. Validate transactional rollback silence and a real notification-to-browser/read flow before expanding push integration.

Resume protocol: read plan and this document, inspect `git status`/recent commits, reconcile code with these statements, continue first incomplete milestone. Do not mark integration complete based only on mocks.

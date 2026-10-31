# CDC-only migration handoff — 2026-10-30

## Request and confirmed decisions

The user requested repository-wide analysis, then completion of the partial Debezium implementation. They explicitly decided:

1. **Remove polling completely.** Debezium must be the sole transactional-outbox publisher. Do not reintroduce a selectable polling mode.
2. **Use Docker PostgreSQL for the whole lab.** Leave existing native Windows PostgreSQL data untouched.
3. Work stopped at the user's request for this handoff, not because implementation was complete.

Read root `AGENTS.md`, `.agents/rules/engineering-standards.md`, and the architecture/reliability/contract/backlog/ADR docs under `kafka-streaming-agent-spec/`. The project is a Kafka mechanics lab; purchases and loyalty are small synthetic fixtures. Preserve transactional customer sequence allocation, aggregateId/customerId key authority, DB-before-offset commit, stable event IDs, and consumer idempotency. Ask about genuinely unresolved architecture; do not ask again about the two confirmed decisions above.

## Working tree precautions

Repository: `E:\SOA-Seminar-Kafka`; Windows PowerShell; Node 22; pnpm 10.

The working tree was already heavily dirty: staged command/query directory renames, changed backend/UI files, untracked topology components and scripts. **Do not reset, clean, or overwrite unrelated work.** No commits were made. Inspect the full current diff before continuing. Some files were edited by Python transformations: review resulting code rather than assuming every replacement was complete.

Recursive removal of `apps/outbox-publisher` was rejected by automatic approval review. The safer alternative succeeded: delete only its 20 tracked source/config/test files through apply_patch. Ignored `dist` and `node_modules` may remain. Those stale compiled files must not be launched by scripts. Do not broadly delete user files or dependency trees.

## Most important runtime discovery

`localhost:5432` originally connected the applications to **native Windows PostgreSQL 18.1**, while Debezium's `postgres:5432` connected to **Docker PostgreSQL 16**, which initially had empty databases. Container health and connector RUNNING did not prove source delivery.

With user authorization:

- Changed local, untracked `.env`: POSTGRES_PORT=5433; DATABASE_URL, PURCHASES_DATABASE_URL, LOYALTY_DATABASE_URL, ANALYTICS_DATABASE_URL now use `127.0.0.1:5433`, preserving credentials and database names. Removed OUTBOX_RELAY_IN_PROCESS there.
- Changed Compose default published PostgreSQL port to 5433 and `.env.example` accordingly.
- Ran `docker compose up -d postgres`, recreating only the PostgreSQL container while preserving its named volume.
- Applied all existing producer, loyalty (including replay staging), and analytics migrations to Docker PostgreSQL through the new setup script.
- Native Windows PostgreSQL was not stopped, modified, or migrated into Docker. Old native data was not copied.
- Existing Kafka topic has **12 partitions** (observed in verifier group assignment), despite six being the fresh-topic default. Do not reset or shrink it automatically.

Last read-only status check while writing this handoff: `purchases-outbox-connector` and source task 0 are both RUNNING. **CDC delivery has not yet been proven after the latest fix.**

## Changes already made

### Polling removal

- Deleted tracked `apps/outbox-publisher` package/source/tests/configs.
- Deleted producer `OutboxRelayService`, its unit test, and the producer's unused KafkaEventPublisherAdapter.
- Removed relay and publisher DI registrations from PurchasesModule.
- Removed post-commit relay hooks from both PostgresUnitOfWorkAdapter and InMemoryUnitOfWorkAdapter.
- Removed OUTBOX_RELAY_IN_PROCESS from producer config and logging.
- Removed root start:publisher and publisher demo commands.
- Adapted the producer outbox-disconnected integration test to inspect source durability without the deleted relay. Review its remaining comments and coverage.
- `scripts/verify-outbox-publisher.mjs` now explicitly errors with the replacement CDC command rather than running polling.
- Removed publisher launch blocks from invalidation/replay/correction scripts. Removed polling status waits; verify replacement waits actually establish delivery before capturing replay boundaries.
- Removed publisher post-ACK crash scenario from verify-fault-injection.mjs, retaining consumer crash scenario. This is a retired mechanism, not evidence of CDC recovery coverage.
- Startup launches four backend services plus UI, runs cdc:setup and then pnpm build before launching. Review readiness, failure propagation, and remaining stale labels.

### CDC provisioning

New `scripts/setup-cdc.mjs`, root `pnpm cdc:setup`:

- Loads .env and validates settings with Zod (dependencies resolved from producer package).
- Runs committed migrations for producer/loyalty/analytics.
- Creates configured purchase topic only if absent; leaves existing partition count unchanged.
- Generates connector configuration from `docker/debezium/outbox-connector.json`; credentials/database come from PURCHASES_DATABASE_URL, internal host/port from CDC_DATABASE_HOST/PORT.
- Uses stable connector, replication slot, publication names; does not reset offsets or drop slots.
- PUTs connector config and waits for connector AND nonempty source task list to report RUNNING.
- Initial snapshots explicitly order outbox rows by aggregate_id/customer_sequence. Existing offsets continue streaming; changing snapshot configuration does not retroactively snapshot an existing connector.

Template no longer includes hardcoded credentials or database connection values. It is a template, not directly registerable without setup filling it.

Two live failures were diagnosed and corrected:

1. `publication.autocreate.mode=filtered` failed against the existing FOR ALL TABLES publication. Restored all_tables, preserving existing publication/slot. Capture still filters to public.outbox_events. Do not drop publication/slot casually.
2. EventRouter failed on `created_at`: `Field 'created_at' is not of type INT64` because schema uses timestamptz. Removed `transforms.outbox.table.field.event.timestamp`. Kafka timestamp now defaults to Debezium event time; business occurredAt remains in the full payload envelope. Document this.

Setup originally failed immediately on stale FAILED task status during reconfiguration; it now waits through transitions until the bounded deadline. It may still need restart handling for an unchanged configuration with an already-failed task. Audit timeout handling and clear errors.

### CDC delivery proof (unfinished)

New `scripts/verify-cdc.mjs`, root `pnpm cdc:verify`:

- Unique verifier consumer group and customer IDs; connects consumer before writing.
- Rolls back one outbox insert, then transactionally writes three ordered purchases and their outbox envelopes.
- Waits for records, validates full contract/IDs, same partition, order, and exclusion of rolled-back event.
- Does not directly publish to Kafka. Retains synthetic source rows and Kafka records as evidence.
- Review test strength: rollback exclusion currently has only the observation window until three committed records arrive; no separate post-success quiet window. Recovery/connector outage is not yet tested.

Its first execution failed/timed out while the timestamp SMT problem was active. After removing timestamp mapping, setup was rerun and connector is now RUNNING. **Rerun cdc:verify and inspect actual results.**

### API/UI semantics

- Purchase read DAO reports ENQUEUED when outbox exists and publishedAt=null. CDC does not write publication acknowledgment back to source rows. Added ENQUEUED to backend response/read types; legacy status values remain for compatibility.
- UI drainPublisher calls renamed checkCdc; API client reads connector/task status through Vite proxy and rejects non-running tasks.
- UI drain callbacks renamed and several labels replaced with read-only Check CDC. No drain mutation is performed.
- Vite proxy loads root env for Connect URL and connector name. `/health/publisher` still exists as a compatibility alias to connector status; the health helper currently checks HTTP success and may incorrectly regard a FAILED task as healthy. Fix this.
- Search for remaining polling/publisher/draining terminology, fields, controls, and misleading success messages. UI updates are incomplete; do not claim everything was relabelled.

### Consumer/replay correctness

- processKafkaRecord now throws on empty or malformed envelopes instead of returning success and letting callers commit offsets. Updated its tests.
- Replay scanner now uses deserializeEnvelope and rejects empty/malformed records instead of silently skipping them.
- ReplayInputEvent has optional eventId (legacy fixtures lack it). Handler forwards it. Replay corrector deduplicates by eventId, falling back to aggregateId:sequence for legacy input, and rejects conflicting reuse by fingerprint.
- Added replay-cdc-duplicates.spec.ts: one repeated $120 event must stay 12 points, and conflicting event identity must fail.
- Previously reproduced the bug directly: one event yielded 12 points; identical duplicate yielded 36. New test covers that case.
- Review optional eventId/fallback policy and consistency with versioned contracts. Live Kafka replay uses validated envelopes, so eventId is available there.

## Verification already performed

Before changes: format, lint, typecheck, 157 unit tests/43 files, Kafka verification, and explicit frontend tsc -b passed.

During this work:

- Unit tests after polling removal and poison handling changes: **150 tests / 40 files passed**. This was before the new two-test replay regression file and some final edits.
- Root lint and typecheck passed after initial migration edits (again, before all final edits).
- Explicit frontend `pnpm --filter @lab/demo-ui exec tsc -b --pretty false` passed after initial UI changes.
- Targeted prettier ran successfully, but later edits still require formatting.
- `pnpm install --lockfile-only --ignore-scripts` succeeded after removing publisher package; review lockfile diff for unrelated resolution churn.
- Docker migrations succeeded for all three databases on port 5433.
- Latest connector/task status is RUNNING; no successful final CDC verifier run yet.
- Full build, final unit/format/lint/typecheck, CDC restart/recovery, HTTP-to-projection smoke, replay under CDC, and final Kafka verification remain outstanding.

Potential previous terminal sessions (may already be finished): setup 32481; verifier 23190; older setup 77967. Poll tool sessions only if still available. The turn was interrupted; do not assume commands completed successfully.

## Remaining work in priority order

1. Inspect current diffs and source for syntax/semantic mistakes. Confirm no application publication path can still poll or manually drain the outbox. Search source, scripts, package scripts, UI, and startup paths; ignored old dist can still be accidentally launched.
2. Rerun cdc:setup and cdc:verify. Fix failures using connector task trace; never claim RUNNING proves record delivery. Verify full envelope JSON, aggregate key, stable IDs, three same-customer sequence values and rollback exclusion.
3. Add CDC outage/recovery proof with bounded waits and retained offsets: stop/pause connector, commit source event, resume/restart, confirm eventual delivery; test replay dedup for repeats. Do not reset existing data/slots to make tests green.
4. Strengthen startup: actual dependency readiness, migration errors, connector/task failure propagation, no stale dist launches. Verify fresh start and repeat setup are idempotent. Confirm all local app config defaults use Docker port 5433, not fallback native port 5432. Other scripts/env validators still contain 5432 defaults.
5. Complete UI and API status semantics. A status check is not a flush barrier. Replay creation must not assume checkCdc drained outstanding WAL/events. Existing replay controls still swallow status errors in some flows.
6. Review append-only outbox port/adapters: legacy findPending/markPublished/markFailed/status columns remain although no publisher uses them. Remove obsolete executable behavior with cohesive test changes; schema migrations may retain legacy columns safely. Do not break HTTP idempotency lookup by deleting history.
7. Complete script migration. Invalidation now verifies actual Kafka receipts rather than published markers; replay/correction scripts need real consumer progress prerequisites. Fault suite currently covers consumer crash only; document replacement CDC recovery proof.
8. Update docs/ADR/backlog/README. **Not done yet.** Add accepted CDC-only ADR, Docker DB port decision, timestamp semantics, snapshot/recovery/retention rules, commands, honest progress. Remove CDC out-of-scope contradiction and update polling-era current-state claims. Root README stops at Phase 3 while backlog overstates later phases; distinguish historical milestones from current evidence.
9. Run all required gates: pnpm format:check, pnpm lint, pnpm typecheck, pnpm test:unit, pnpm kafka:verify; frontend tsc -b and full build; CDC live tests. Run relevant integration tests only after inspecting their cleanup/data isolation. Report exact passes/failures.

## Broader issues discovered in initial audit (avoid unrelated scope expansion)

- Worker scaling creates multiple Kafka consumers inside one Node process, despite backlog claiming independent containers. Kill control gracefully stops/disconnects a consumer, not a process crash.
- BenchmarkStudio measures browser HTTP request timing, random workloads, synthetic batch-average latency, not isolated deterministic transport benchmark.
- Topology has invented fallback partitions/zero lag/healthy states on missing telemetry.
- Replay cutover directly uses Prisma/live loyalty tables and lacks an enforced write freeze. It can race live updates; replay boundary capture also needs a proven source/consumer barrier.
- Live partition expansion contradicts fixed-key-mapping ordering instructions.
- ADR says pg; implementation uses Prisma. Scope docs explicitly excluded Debezium until this newly authorized work.

Keep the current task focused on finishing CDC-only safely; surface unrelated findings rather than silently declaring all earlier milestones complete.

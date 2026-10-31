# Application reliability implementation and handoff plan

Date: 2026-11-01

Status: proposed plan only; none of the fixes below have been implemented by writing this document.

## 1. Purpose and scope

Make the actual application demonstrably reliable from UI request through PostgreSQL, Debezium, Kafka, consumer projections, replay, and correction cutover. Exclude the simulated labs, topology editor, and broker topology redesign. A UI tool that writes to the real business topic is in scope because it can break the application.

The user requested a detailed plan and is deferring implementation because of credit limits. The next agent should read this document before repeating the repository-wide audit. Revalidate the specific code being changed because the working tree contains ongoing work.

Confirmed requirements carried forward:

- Debezium is the sole transactional-outbox publisher. Do not restore the removed polling publisher or a polling fallback.
- All application databases use Docker PostgreSQL. Native Windows PostgreSQL and its existing data must remain untouched. The documented host mapping is port 5433; container-to-container PostgreSQL uses its internal port.
- Kafbat replaces Provectus. Do not revert that migration.
- Keep the synthetic purchase/loyalty domain minimal. Do not add retail machinery to solve infrastructure reliability.
- Preserve aggregateId/customerId equality, factory-created envelopes, customer partition keys, transactional customer sequencing, stable event IDs, database deduplication, and database-before-offset commit.

## 2. Baseline and evidence limits

At the latest audit, format, lint, typecheck, and all 180 unit tests in 46 files passed. Docker had no running containers. The reported failure paths were identified by code review; a full running CDC pipeline and database concurrency races were not reproduced in that audit.

The repository is heavily dirty, including staged renames, modified backend and UI code, deleted polling publisher files, and untracked scripts. Do not reset, clean, restore, broadly stage, or commit unrelated changes. The earlier request to commit the Kafbat change does not authorize publishing this new reliability work.

`CDC-HANDOFF.md` remains useful historical context, but some of its outstanding work has since changed. Current source and current verification results take precedence over historical claims in that file.

Read before implementing:

- `AGENTS.md` and applicable instructions under `.agents/`.
- Architecture, event contracts, reliability, replay, and ADR documents under `kafka-streaming-agent-spec/docs/`.
- `README.md`, `kafka-streaming-agent-spec/docs/11-agent-backlog.md`, and `CDC-HANDOFF.md`.
- This plan, followed by only the files relevant to the next ticket.

## 3. Actual architecture and ownership

```text
React UI
  -> Producer HTTP API
     -> transaction in purchases_db
        -> customer sequence + purchase + outbox event + optional HTTP idempotency
     -> Debezium reads PostgreSQL WAL / initial outbox snapshot
        -> purchase topic, keyed by customer aggregate
           -> loyalty group -> loyalty_db -> offset N+1
           -> analytics group -> analytics_db -> offset N+1

Replay API
  -> captures per-partition Kafka boundaries
  -> scans historical envelopes
  -> two-pass correction
  -> replay staging
  -> explicit cutover into live loyalty accounts
```

The producer owns source purchases, sequence allocation, and outbox records. Loyalty owns live loyalty projection state and its processed-event ledger. Analytics owns its summary and processed-event ledger. Replay currently writes loyalty tables directly; that is an ownership boundary that must be resolved deliberately, not normalized by adding more cross-context database access.

HTTP purchase acceptance proves the source transaction committed. It does not prove CDC delivered the event or that either projection processed it. Connector RUNNING and consumer group lag zero are not substitutes for event-specific delivery evidence.

## 4. Decisions to settle before dependent implementation

These recommendations are not recorded user approvals. Under `AGENTS.md`, consult the user before implementing unresolved architectural choices. Bundle the decisions once; do not repeatedly ask about already confirmed Docker PostgreSQL or CDC-only requirements. Independent defect fixes can proceed without inventing these decisions.

| Decision | Recommended direction | Trade-off / alternative |
| --- | --- | --- |
| Replay ownership | Treat replay as a loyalty-owned rebuild capability, or let loyalty expose an application operation for cutover | A separate replay context should not directly own loyalty mutable tables; moving all replay computation into loyalty is a larger change |
| Cutover strategy | For the small demo, use a coordinated maintenance window with a proven source/CDC/consumer boundary | Online correction needs substantially more concurrency and ledger coordination; atomic compare-and-set alone does not solve all replay/live overlap |
| Malformed business events | Fail visibly and preserve offsets by default; provide an explicit recovery procedure | Automatic quarantine/skip requires a durable audit trail and explicit acceptance that history is incomplete |
| Analytics meaning | Keep gross completed-purchase totals and label them clearly if that matches the intended demo | Net sales require invalidation processing, per-purchase contribution tracking, and independent replay/rebuild semantics |
| Batch contract | Prefer one bounded transaction for the existing small maximum if performance permits | Keeping chunk commits requires an explicit partial-success/idempotent resume contract |
| Reset behavior | Make the existing topic-only command unmistakably limited; separately design an opt-in full application reset | Full reset is destructive and must coordinate databases, Kafka, connector offsets/slot, and source snapshots |

## 5. Execution order and stopping points

1. Establish a safe baseline and a real end-to-end smoke test.
2. Fix configuration loading and consumer initialization.
3. Isolate raw benchmark traffic and make failures/readiness truthful.
4. Repair replay scan correctness and completion evidence.
5. Implement the approved replay ownership and cutover protocol.
6. Fix concurrent HTTP idempotency and the approved batch behavior.
7. Clarify analytics semantics and reset behavior.
8. Run the integrated regression matrix and update project documentation.

If resources run out after step 3, the normal purchase demo may be usable after live verification, but replay cutover must remain explicitly unavailable until steps 4 and 5 pass. Do not label the whole application complete because a purchase smoke test passes.

## 6. Ticket A — Baseline and reproducible evidence

### Work

1. Capture branch, HEAD, and `git status --short`; inspect the diff for files in scope. Record pre-existing changes without copying secrets.
2. Inventory package start/build scripts, application bootstrap, Prisma migrations, connector setup, and actual UI routes.
3. Confirm endpoint/database destinations using redacted configuration and database identity queries. Do not print credentials or connection strings.
4. When runtime work is authorized, start the intended existing Docker environment without deleting volumes, topics, groups, slots, or connector offsets.
5. Apply only expected committed migrations; review pending schema changes before applying them.
6. Extend the existing CDC verification scripts where possible rather than creating a competing test framework.

### Acceptance

A unique test purchase can be traced through source purchase/outbox, Kafka event ID/key/partition/offset, loyalty processed-event/account state, and analytics processed-event/summary. Capture eventual success with a bounded timeout and diagnostic output. Never use a fixed sleep as proof of delivery.

A second verification pass with the same event does not award points or increment sales twice. Test-generated customers and event IDs must be isolated from existing demo fixtures.

## 7. Ticket B — Consistent bootstrap and environment validation

### Evidence and files

- Producer and replay bootstrap load `.env`; loyalty and analytics bootstrap currently do not.
- Worker start commands run `node dist/main.js`.
- Loyalty and analytics schemas read `PORT`, while `.env.example` documents service-specific port variables.
- Both worker schemas use `z.coerce.boolean()` for `KAFKA_FROM_BEGINNING`; the string `false` evaluates to true.

Inspect `apps/*/src/main.ts`, each `src/config/` directory, application package scripts, `scripts/start-lab.mjs`, `.env.example`, and `apps/demo-ui/vite.config.ts`.

### Work

1. Establish one documented bootstrap loading convention that works for root commands and package-local commands, before configuration providers are constructed.
2. Keep raw environment access in bootstrap/configuration code. Inject validated settings into services and controllers.
3. Ensure a shared producer `PORT` cannot override all worker ports. Document precedence for service-specific settings and any compatibility fallback.
4. Validate booleans explicitly, accepting documented true/false strings and rejecting ambiguous values.
5. Keep database settings context-specific. A purchases database fallback must never silently direct a worker to purchases_db.
6. Derive UI development proxies and launcher URLs from documented configuration. Remove hardcoded business-topic fallbacks in replay and real operations paths in favor of injected configuration.
7. Validate every new timing, topic, or maintenance setting introduced by later tickets.

### Tests and acceptance

- Start all services using only the documented `.env`; each binds its intended distinct port and reaches its intended database.
- Change a broker address and worker port in an isolated environment; all relevant processes and proxies honor them.
- Missing required settings fail with useful redacted errors.
- Boolean parsing covers unset, true, false, and invalid text.
- Native PostgreSQL remains untouched.

## 8. Ticket C — Safe first start and consumer restart

### Evidence and files

Both real consumers default to `fromBeginning: false`; CDC setup runs before application consumers start. See the loyalty and analytics Kafka consumer adapters and config schemas.

### Work

1. Define durable business projection consumers to read retained history when their group has no committed offset. Preserve committed offsets on ordinary restarts.
2. Apply the policy consistently to primary and additional real consumer instances; inspect all subscribe calls rather than fixing one.
3. Do not automatically rewind an established group or reset an existing projection database.
4. Detect and clearly report incompatible projection/group state: for example, empty projection with advanced group offsets, or a sequence gap caused by missing retained history.
5. Keep sequence-gap checks strict. Do not make the demo appear healthy by accepting arbitrary sequence jumps.

### Tests and acceptance

- Publish purchases before starting fresh isolated groups; both projections process all retained purchases.
- Restart existing consumers; state is unchanged except for genuinely new events.
- Re-delivery after database commit but before offset commit does not duplicate effects.
- A missing-history scenario produces an actionable degraded/error state, not silent success or an endless unexplained spinner.

## 9. Ticket D — Isolate raw traffic and define poison-event recovery

### Evidence and files

`apps/demo-ui/src/components/BenchmarkStudio.tsx` constructs direct-broker messages missing required envelope fields. The raw operations endpoint defaults to the business purchase topic. `packages/observability/src/kafka/kafka-message-processor.ts` correctly rejects malformed events without acknowledging them, but recovery is incomplete.

### Work

1. Route arbitrary direct-broker benchmark traffic to a separately configured benchmark topic, with no business consumers subscribed.
2. Keep normal application workload generation on the purchase API/outbox path. Do not fix the benchmark by fabricating customer sequences independently of the producer transaction.
3. Require explicit destination/intent for any operation that deliberately injects poison into the business stream. Ordinary UI actions must not do this accidentally.
4. Preserve failure details: group, topic, partition, offset as a decimal string, error category, and safe event metadata. Avoid exposing full sensitive payloads.
5. Implement the approved recovery policy. An intentional skip must never be disguised as a successfully processed purchase. If quarantine is chosen, make its durability and offset transition idempotent and document replay implications.
6. Distinguish transient dependency failure from deterministic invalid data. Use bounded, configurable retry/backoff where appropriate; surface persistent failures.

### Tests and acceptance

- Direct benchmark activity cannot change loyalty/sales or block their groups.
- A poison record in an isolated business test stream leaves its offset uncommitted and reports a visible error.
- Restarting does not silently discard the record.
- Approved recovery is auditable and does not allow replay to claim complete business history after a skipped business event.

## 10. Ticket E — Truthful startup, readiness, and shutdown

### Evidence and files

`scripts/start-lab.mjs` uses `includes('healthy')`, which also matches unhealthy, and prints success after a timer. Producer readiness always returns ok. Consumer status is derived partly from pause state rather than lifecycle state.

### Work

1. Parse container health exactly; distinguish missing, starting, healthy, unhealthy, and exited states.
2. Handle child process exit as well as spawn errors. Startup must fail if a required application exits or cannot bind its port.
3. Wait for application-specific readiness with a deadline before announcing success.
4. Keep liveness separate from readiness. Report dependencies and consumer lifecycle without claiming that process existence proves data delivery.
5. Track consumer startup, assignment, running, paused, failed/retrying, and stopped states from actual lifecycle events. Do not equate intentionally paused with crashed.
6. Expose CDC status separately from producer source-write availability. The producer may validly accept durable outbox writes while CDC is unavailable; the UI must show that delivery is delayed.
7. Implement graceful shutdown first, with a bounded forced fallback. Verify Windows child-process handling and preserve database-before-offset guarantees.

### Tests and acceptance

Occupied application port, dead database, failed consumer startup, unhealthy container, and failed connector task cannot produce an unconditional success banner. Normal intentional pause is distinguishable from failure. Shutdown and restart do not duplicate projection effects.

## 11. Ticket F — Replay must prove complete bounded input

### Evidence and files

Inspect `apps/replay-worker/src/replay/infrastructure/adapters/kafkajs-bounded-replay-scanner.adapter.ts`, the start-bounded-replay handler, replay run/staging persistence, and `replay-customer-corrector.ts`.

Current problems: malformed records are skipped, the idle timer starts before assignment, captured start offsets are not used as an explicit scan lower bound, and sequence continuity is not validated.

### Work

1. Capture and persist topic identity where supported, partition set, required start offsets, and exclusive end offsets. Keep offsets as strings/BigInt throughout.
2. Separate joining/assignment timeout, scan inactivity timeout, and total run deadline. Make them configurable and start each at the correct lifecycle point.
3. Honor the exact requested interval per partition. Handle empty partitions and existing replay group offsets deliberately; use unique run groups or an explicitly supported resume protocol.
4. Do not require every numeric Kafka offset to contain an application record. Completion must account for valid log gaps and consumer positions while still proving the captured interval was traversed.
5. Fail the run on invalid business envelopes by default. Persist diagnostic coordinates and prevent cutover; do not convert decode failure to ignored null input.
6. Deduplicate stable event identities and reject conflicting duplicates. Validate expected customer sequence continuity after deduplication, including invalidation events. Explicitly define what baseline is required if replay starts after sequence 1.
7. Treat topic recreation, partition-set changes during a run, unavailable required history, and inconsistent boundaries as explicit failures. An earliest offset of zero alone does not prove historical completeness after topic recreation.
8. Bound memory/event count for this small demo or process through a deliberate staging strategy. Do not silently collect unbounded history in RAM.
9. Persist meaningful run state and failure reason. Ensure connections/timers close on all terminal paths and interrupted runs are identified on restart. Do not add an outbox polling publisher to accomplish this.

### Tests and acceptance

- Valid history, duplicate CDC delivery, empty partitions, slow group assignment, and records arriving beyond the captured boundary behave correctly.
- Malformed history, missing customer sequence, conflicting duplicate, expired required history, and recreated topic cannot yield a cutover-eligible run.
- New post-boundary events never leak into the replay result.
- Kafka offset holes do not cause false failure solely because a numeric offset was not an application record.
- A failed or interrupted run cannot leave a misleading completed result.

## 12. Ticket G — Safe replay ownership and cutover

### Evidence and files

The cutover handler directly reads/writes loyalty tables. Its read/check/upsert sequence is vulnerable to live updates. It can report CUTOVER_COMPLETED when all customers were skipped. It does not coordinate the processed-event ledger.

### Recommended protocol, subject to the decision in section 4

1. Give loyalty ownership of applying corrected state. Put the application operation behind a small port; keep Prisma details in infrastructure.
2. Acquire a durable, scoped maintenance/correction lock. Reject conflicting correction runs. UI-only button disabling is insufficient.
3. Prevent new source mutations in the selected scope, including batches and invalidations, and drain transactions already in flight. Define whether the scope is global or per customer and display it honestly.
4. Establish that all accepted source events in scope reached Kafka. Use event-specific evidence or a deliberately designed CDC barrier; group lag zero alone cannot prove there are no undelivered outbox events.
5. Establish that live loyalty has committed processing through the agreed boundary. Then stop/drain relevant processing without confusing consumer pause with completion of in-flight handlers.
6. Run or validate replay against that same source-complete boundary. A historical run created before the maintenance window cannot automatically be reused.
7. In a transaction, revalidate run eligibility and current state, apply corrections, and record cutover outcome. Use database concurrency protection as a final guard, not merely an earlier read comparison.
8. Keep the processed-event ledger consistent with the chosen boundary. Prefer ensuring live processing has already recorded boundary events rather than inventing processed records without their identity/evidence.
9. Commit the corrected state before resuming processing and source writes. Define recovery for process death at every stage; a lost HTTP response must not cause a second application of cutover.
10. Return applied/skipped/conflicted counts and reasons. Zero applications must not be presented as successful reconciliation unless it is an explicitly recognized idempotent repeat.

### Alternative: online cutover

If uninterrupted writes are required, design an atomic version/fencing protocol covering both live writes and cutover, plus reconciliation for replay-ahead-of-consumer state. A simple conditional update fixes only one overwrite race. Do not advertise online correction until deterministic interleaving tests cover both directions.

### Tests and acceptance

- Force a live update between cutover read and write; newer state is never lost.
- Attempt cutover with CDC backlog, consumer backlog, another active correction, and advanced live state; each is handled explicitly.
- Repeat the same cutover request and recover after transaction commit/lost response without changing results twice.
- Inject failure before apply, during apply, and before resumption; document safe recovery and lock cleanup.
- After correction, the next normal purchase uses the corrected tier and sequence without a new correctionRequired flag caused by replay overlap.
- UI accurately shows whether correction is global or customer-scoped and whether anything was actually applied.

## 13. Ticket H — Concurrent HTTP idempotency and batch outcomes

### Idempotency work

Inspect the create-purchase handler, PostgreSQL unit of work, sequence generator, and HTTP idempotency repository.

The existing lookup precedes sequence locking. Serialize the idempotency decision appropriately or implement a narrowly scoped conflict/retry protocol. A retry must start a new database transaction after a PostgreSQL uniqueness failure; do not query again inside an aborted transaction. Preserve the stored request fingerprint and return the original purchase/event result for matching requests. Do not retry arbitrary database errors indefinitely.

### Batch work

The handler commits chunks of 100. Implement the approved all-or-nothing or explicit partial-progress contract. Preserve stable request/item identity across retries if partial commits remain. Avoid recording response success for work whose transaction later rolled back.

### Tests and acceptance

- Concurrent matching idempotency keys produce one purchase/outbox event and equivalent successful responses.
- Concurrent same-key/different-payload requests produce a clear conflict with no extra purchase.
- Different keys preserve customer sequence ordering.
- Force failure in the second chunk of a multi-chunk request: behavior matches the documented contract, and retry cannot silently duplicate earlier accepted purchases.

## 14. Ticket I — Analytics meaning and safe reset boundaries

### Analytics

The analytics consumer processes PurchaseCompleted and acknowledges invalidations without reversing totals. If gross totals are intended, label API/UI/docs accordingly and test that meaning. If net totals are required, design idempotent per-purchase reversal and analytics-owned rebuild semantics; do not reuse loyalty tables or merely subtract arbitrary payload amounts.

### Reset

The current topic reset does not reset application state or CDC checkpoints. First make that limitation explicit and prevent it being presented as a clean application restart.

A separately approved full reset needs a concrete inventory and sequence: stop writes and drain/stop consumers, stop connector activity, identify exact Docker database/topic/group/slot/connector state, reset only authorized demo state, reprovision schema and connector consistently, and run the fresh-start smoke test. Do not delete Connect shared internal topics casually. Never reset native PostgreSQL. Present the exact destructive targets for authorization before executing.

Tests should use disposable isolated resources; demonstrating reset must not destroy existing user data.

## 15. Integrated verification matrix

| Scenario | Required evidence |
| --- | --- |
| Fresh start with preexisting source outbox rows | CDC snapshot reaches Kafka; new groups build complete projections |
| Ordinary purchase | One source transaction, correct key/envelope, one effect in each projection |
| Concurrent same-customer requests | Monotonic committed sequence and correct downstream order |
| Duplicate HTTP retry | Same accepted result, one source event |
| Consumer crash after DB commit | Redelivery deduplicated; offset eventually advances to N+1 |
| CDC outage and recovery | Purchases remain durable; recovery delivers backlog without extra effects |
| Raw benchmark | Isolated topic; business consumer state unaffected |
| Poison business event | Visible failure, no silent acknowledgment, explicit recovery |
| Invalidation and replay | Complete bounded input, correct recalculated tier/points |
| Concurrent correction/live activity | No lost update or ledger inconsistency |
| Interrupted replay/cutover | Recoverable persisted status; no false completed run |
| Empty/retained/truncated/recreated history | Correct classification; incomplete history never cut over |
| Custom ports/database/brokers/topic | All processes agree on validated configuration |
| Service startup failure | Accurate failure output; no false success banner |

Use unique fixture identities, deterministic concurrency barriers, bounded waits, and inspectable assertions. Do not rely on sleeps or log messages alone. Record actual event IDs and per-partition offsets in redacted evidence; never sum offsets into a global progress number.

## 16. Quality gates, documentation, and delivery

For each implemented milestone, run the required repository checks:

```text
pnpm format:check
pnpm lint
pnpm typecheck
pnpm test:unit
pnpm kafka:verify   (when infrastructure is active)
```

Add targeted real integration verification for the changed reliability behavior. Unit tests alone do not prove CDC transport, PostgreSQL race behavior, or safe cutover. Check the UI's actual build/typecheck configuration if UI code changes. Repeat broad checks when changes justify them, not merely to consume time.

Update README and the actual backlog at `kafka-streaming-agent-spec/docs/11-agent-backlog.md` with implemented behavior and evidence. Add/update an ADR for approved replay ownership/cutover and recovery decisions. Distinguish planned, implemented, statically tested, and runtime verified. Writing this plan does not advance implementation milestones.

Suggested implementation-sized commits, only when authorized:

1. Consistent environment bootstrap and durable consumer initialization.
2. Benchmark isolation and truthful runtime status.
3. Strict bounded replay validation and failure lifecycle.
4. Approved loyalty-owned correction/cutover protocol.
5. Concurrent request idempotency and batch semantics.
6. Analytics/reset documentation or approved implementation, plus end-to-end regression evidence.

Stage exact reviewed paths/hunks. Existing staged changes require special care; never use a broad commit that includes them accidentally.

## 17. Final completion criteria

The work is complete only when the normal purchase path and correction path both pass real end-to-end verification; fresh groups cannot silently miss retained business history; malformed/raw traffic cannot accidentally poison business processing; replay cannot silently omit required history; cutover cannot lose live updates or misrepresent skipped work; configuration and readiness reflect actual running dependencies; and retry/reset/analytics semantics are explicit.

If work stops early, leave a short handoff identifying completed ticket IDs, exact files changed, commands/results, remaining architectural decisions, live environment state, and the next single task. Do not repeat the full audit or call unfinished runtime verification complete.

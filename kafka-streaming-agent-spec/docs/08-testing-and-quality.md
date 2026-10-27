# 08 — Test Strategy, Quality Gates and Failure Matrix

## Guiding principle

**Kafka guarantees require tests with a real broker.** Unit tests cover pure domain and application orchestration; integration/e2e tests verify Kafka group/offset/retry and Postgres atomicity. Mocks alone cannot establish correctness of partition assignment, rebalance, durability or timing.

## Testing layers

### Unit tests (fast, deterministic)

- Domain `tierForEligibleSpend`, `pointsForPurchase`, `LoyaltyAccount.apply()`.
- Boundary values: 0, 9999, 10000, 29999, 30000 cents; invalid amount, overflow beyond safe integer.
- Tier is selected before current purchase; totals 82 and 40 for the seeded fixture.
- Command handlers call appropriate ports, but do not directly import Kafka/SQL.
- Validation of event envelope schemas and customerID/key equality.
- Simple query handlers do not mutate repositories.
- Idempotency-key replay returns same result; same key with different request body fails.

### Integration tests (real dependencies)

- Use disposable Kafka topics/groups and PostgreSQL schemas/DB instances. Prefer Testcontainers if compatible with local Docker; Compose-based test environment is acceptable.
- Unique topic or group names per test run prevent stale offsets from affecting expectations.
- Producer publishes with key; inspect record's actual partition and offset.
- Independent groups each process records; pause one group while the other progresses.
- Manual offset `N+1` after DB commit and no commit on rollback.
- Outbox publisher sends, marks published, retries; crash-after-ack can produce duplicate with same `eventId`.
- Consumer event insert uses `RETURNING` and no further update on a conflict.
- Concurrency test: multiple HTTP requests for one customer receive unique increasing `customerSequence` and publish in that same order.
- At least one test that starts/stops a **process** rather than only invoking an in-memory callback.

### E2E / scenario tests

| Case | Input/fault | Assertion |
|---|---|---|
| Single publish | One U001 purchase | Kafka record and worker output share eventId. |
| Same-key ordering | U001 seq 1/2/3 | Same partition, ascending broker offsets and 82 points. |
| Group isolation | Pause Analytics, send 100 events | Loyalty progresses, Analytics lag rises then drains. |
| Duplicate logical event | Send same eventId twice | One state mutation; two deliveries observed. |
| Post-commit consumer crash | Crash before offset commit | Redelivery and skip; no duplicate DB effect. |
| DB rollback | Force transient DB error | No offset advancement; successful retry eventually. |
| Outbox ack/marker gap | Crash after ack | Stable eventId republished; consumer safe. |
| Replay | P001 invalidated | Replayed staging=40, live group offsets untouched. |
| Partition scale | 1→3→6 workers | Real assignments, no customer double-ownership within group. |
| Worker crash | Kill member | Rebalance and progress recovery. |
| Broker crash | Stop a partition leader in RF3 | New eligible leader, ISR metadata and resumed progress. |
| Retention missing history | Expire required records | Replay fails explicitly instead of miscomputing. |

## Failure injection policy

- Fault points must be **named, deterministic and environment-gated** (e.g. `AFTER_LOYALTY_DB_COMMIT_BEFORE_OFFSET_COMMIT`, `AFTER_KAFKA_ACK_BEFORE_OUTBOX_MARK`).
- If process-exit is used, ensure a supervisor/restart command and isolated test dataset; do not throw errors in production paths merely for demo.
- No `sleep(1000)` as a synchronization assertion. Await real broker group state, offset conditions, database rows and status transitions with explicit timeouts.
- Unit tests do not depend on wall clock; inject time provider and deterministic IDs.

## Quality pipeline (target commands)

```bash
npm run format:check
npm run lint
npm run typecheck
npm run test:unit
npm run test:integration
npm run test:e2e
```

The agent must add these scripts as functionality is created. Each pull request must pass relevant active scripts before continuing.

## Code quality rules

- TypeScript strict mode, no `any`, `ts-ignore` requires justification.
- Modules grouped by bounded context and layer, not generic shared service dumping ground.
- No raw DB/Kafka operations in domain or presentation layer.
- Use explicit `Result`/error types where they improve clarity; no indiscriminate exception swallowing.
- Clear naming: `customerSequence`, `committedNextOffset`, `logEndOffset`, `correlationId`.
- Prefer parameterized SQL; count/lock operations must be concurrency-tested.
- No unjustified retries, blind offset commits, or busy-spin consumer polling.
- Strong input validation at HTTP and Kafka boundaries.
- Prove correctness for both success and failure cases.

## Performance test methodology

- Record hardware, CPU/memory allocation and topology before every trial.
- Warm up, run fixed records/customer cardinality, record backlog and stable measurement interval.
- Compare 1/3/6 consumers with identical workload and topic partitioning.
- Separate producer throughput from DB commit throughput and end-to-end processing latency.
- Identify load skew/hot partitions, extra DB round-trips, Kafka broker CPU, consumer rebalance pauses.
- Never quote unexplained peak throughput as a general Kafka limit or capability.

## Definition of done for a phase

- Acceptance scenario passes reproducibly.
- Test evidence available (logs/CLI commands or machine-readable test outputs).
- Architecture does not violate dependency rules.
- Documentation/runbook updated.
- Next phase will build on actual proven behavior, not untested assumptions.

## Benchmark and adapter-specific quality gates

- Unit-test deterministic event corpus and exact payload size; benchmark results must be byte-for-byte comparable independent of broker adapter.
- Unit-test metrics aggregation using a known raw distribution (especially **merged p99**) and positive/negative event reconciliation.
- Integration-test each adapter's publish confirmation, manual ACK behavior, bounded cancellation, failure/recovery, native replay support (`UNSUPPORTED` for Quorum already-ACKed messages), and clean shutdown.
- Ensure a new broker adapter does **not** modify workload generation, metric formulas, Purchase/Loyalty domain, or any unrelated broker adapter.
- Confirm transport-only benchmark hot path excludes NestJS API and PostgreSQL; label any full-system DB benchmark separately.
- Verify 3+ real trials, configuration manifests, stale-topic/queue isolation, percentile data and honest status on failed/partial runs before producing comparison charts.
- Run CPU/resource sanity checks, no unlimited in-flight Promise generation, no logging every event in timed profile unless intentional.

See `14-benchmark-methodology-and-metrics.md`, `15-kafka-redis-rabbitmq-comparison.md`, `16-comparison-runbook-and-data-contracts.md`.

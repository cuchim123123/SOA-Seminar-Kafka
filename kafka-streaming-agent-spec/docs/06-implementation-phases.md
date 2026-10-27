# 06 — Incremental Implementation Plan and Phase Gates

**Never implement all phases in a single coding pass.** Each phase is an independently demoable vertical slice. Write code and tests only for current needs and keep future design as documentation.

## Phase 0 — Repository foundation

- TypeScript workspace/monorepo; strict tsconfig; formatting/linting; test runner.
- Bootstrap minimal NestJS API package, tiny contract package, KafkaJS client adapter, Docker Compose with Kafka KRaft + Kafka UI.
- `.env.example`, config validation, README run steps, graceful shutdown skeleton.
- No PostgreSQL, web dashboard, event sourcing framework or broad shared base classes yet.

**Gate:** clean `install`, lint/typecheck/test commands; broker health and Kafka UI accessible.

## Phase 1 — Real event flow (MVP)

**Implement:** `POST /api/v1/demo/purchases`, one broker, `purchase.events` one partition, one consumer group, one KafkaJS producer, small typed event envelope.

- API synthesizes `purchaseId`, `eventId`, `customerSequence` (monotonic in-process for a single seeded customer in this phase).
- API awaits Kafka producer ack; returns 202.
- Worker prints event ID, broker-reported key, partition, offset, and simple earned-points output.
- Use Kafka UI or CLI to verify record is in Kafka. No persistence/reliability claim.

**Gate:** deterministic test sends one event and asserts the real consumer sees same ID and Kafka offset. README includes end-to-end curl and broker inspection.

## Phase 2 — Partitioning and ordering

**Implement:** controlled demo reset, recreate topic with **six partitions**, stable Kafka partitioner/key, multi-customer generator and per-customer sequence checks.

- Show three events for `U001` on the same partition and increasing offsets.
- Generate multiple customers and show distribution; note that different keys can collide into one partition.
- No consumer parallelism claim yet; show actual partition metadata.
- Decide and document partitioner before generating datasets.

**Gate:** ordering test with P001/P002/P003 and observed result 82, plus CLI/UI verification of partition/offset. Topic reset tested and clearly destructive.

## Phase 3 — Consumer groups and offsets

**Implement:** independent Analytics consumer group, manual commit, pause/resume commands and offset/lag reader.

- Persist progress in Kafka group commits; if state still in-memory, label results **non-durable**.
- Show Analytics paused while Loyalty advances and lag accumulates.
- Resume Analytics and verify it catches up from its committed offsets.
- Use actual group topic-partition offsets; avoid summing them into a fake global sequence.

**Gate:** automated two-group test; manual scripted pause/resume with visible before/after committed offsets.

## Phase 4 — Durable state, outbox and idempotency

**Implement:** Postgres migrations, repository ports/adapters, CQRS command/query handlers, atomic purchase+outbox commit, separate publisher process and persistent consumer idempotency.

- Migrate API to DB+outbox semantics without breaking response shape.
- Serialize per-customer sequence assignment with row locks and preserve publication order.
- Implement processing transaction: unique processed event insert → if inserted update Loyalty/Analytics → commit DB → commit offset.
- Add deterministic crash switch **after** DB commit but **before** offset commit.
- Write integration tests against real Postgres and Kafka.

**Gate:** repeated event never double-mutates; injected crash triggers Kafka redelivery; publisher crash/retry can duplicate Kafka event but not business side effects.

## Phase 5 — Replay and retention

**Implement:** invalidation API/event, bounded replay worker, replay-run metadata and isolated staging, replay progress metrics.

- Seed U001 P001/P002/P003 to produce 82 points.
- Publish invalidation P001; freeze input, drain publisher and consumers, capture per-partition boundaries.
- Read retained Kafka log in two passes to identify invalidations, then recompute staging: 40 points.
- Compare and optionally atomically cut over projection while writes are paused.
- Demonstrate catch-up vs replay as different features.

**Gate:** replay group has a **new group ID**, and untouched Loyalty/Analytics committed offsets; clear failure when retention cannot cover required history.

## Phase 6 — Consumer horizontal scaling / rebalancing

**Implement:** separately launched Loyalty worker containers, same group ID, worker ownership telemetry and load generator.

- Scale 1→3→6 (and optionally 7 to see idle consumer) with six partitions.
- Capture actual assignments and rebalances.
- Kill a worker and validate reassignment and no repeated DB effects.
- Measure throughput, lag/latency across repeatable trials; document CPU/DB bottlenecks.

**Gate:** real Kafka group membership changes, no fake simulated assignments, lag eventually drains and state matches a single-worker baseline.

## Phase 7 — Replicated KRaft cluster

**Implement:** separate clean multi-node Compose profile with **3 combined broker/controller nodes**, 3 voter quorum, six-partition topic with RF=3, `min.insync.replicas=2` and producer `acks=all`.

- Ensure Kafka internal topic replication settings fit 3 brokers.
- Observe leaders, replicas and ISR; kill a broker that leads a partition.
- Verify metadata change, retry/recovery and broker replica catch-up.
- Simulate insufficient ISR and record producer errors; distinguish KRaft quorum loss from minISR failure.

**Gate:** actual metadata proves RF=3, election observed, successful writes with one broker down when quorum/ISR permit, failure with too few replicas.

## Phase 8 — Actual observability + Kafka benchmark baseline

**Implement:** real Kafka producer/consumer metrics, resource sampling, benchmark-ready deterministic workload generator, a small **benchmark-only publisher/subscriber port implemented for Kafka**, transport-only runner, immutable run manifest/result export, and initial dashboard views. Keep the benchmark runner separate from Purchase/Loyalty CQRS handlers.

- Record attempted vs broker-confirmed publish rate, received delivery attempts vs unique acknowledged processing, p50/p95/p99 latency, per-group lag, retry/errors and CPU/memory/disk/network samples.
- Run the Kafka-only baseline repeatedly with deterministic payloads, bounded concurrency, pinned image/client versions and saved raw results.
- Use metadata/metrics APIs for topic/partition/ISR offsets and client instrumentation for timing. Make all unavailable signals explicit.
- Build the UI incrementally; CLI/JSON/CSV proof must exist before chart polish.

**Gate:** a fresh machine can run at least 3 Kafka trials of one profile, export real versioned results/histograms and reproduce the visualizations. `14-benchmark-methodology-and-metrics.md` is normative.

## Phase 9 — Extensible Redis Streams and RabbitMQ comparison

**Implement:** independent `Streaming Benchmark` bounded context, transport-port contracts and adapters. Start with Kafka adapter, then Redis Streams, then RabbitMQ **Quorum Queue** adapter. RabbitMQ native Streams/Super Streams is a distinct optional adapter.

- Share exactly one deterministic workload corpus, run configuration, metrics formulas and reporting schema.
- Do not change Purchase/Loyalty bounded contexts or duplicate generator/reporting logic when adding an adapter.
- Document native ack/delivery, replay and retention capabilities, persistence/durability profiles, topology, prefetch/batching and recovery semantics.
- First compare **transport-only** Kafka/Redis Streams/RabbitMQ Quorum under a clearly labeled single-node baseline. Optional replicated or fan-out profiles are separate experiments.
- Run ≥3 trials per mode/config, compare median and spread of confirmed throughput, unique processed throughput, p50/p95/p99 latency, errors, backlog and resource usage.

**Gate:** same seed/payload bytes and result schema across broker adapters; only adapter and composition root change to add a broker; results include fair-mode labels and explicit `N/A` for unsupported native features. See docs 13–16.

## Phase 10 — Final dashboard and seminar package

**Implement:** comparisons dashboard, saved-result visualizations, fault control guards and reproducible combined seminar scripts.

- Show broker-native topology and Kafka internal metrics separately from cross-broker transport comparison.
- Comparisons must reject mismatched benchmark modes/profiles or label them explicitly; no single universal winner graphic.
- Source all graphs from run result files or live instrumentation, not generated decoration.
- Validate clean local installs and demo scripts. Archive manifest, test outcomes, trial samples and limitations.

**Gate:** end-to-end Kafka scenes plus Kafka/Redis/RabbitMQ benchmark demonstrations run reproducibly, with actual measured charts and known host-resource limitations. Documentation in `17-audit-and-definition-of-done.md` passes review.

## Suggested incremental command contract

These commands are **targets for the coding agent to implement**, not currently existing scripts:

```bash
npm run lint
npm run typecheck
npm run test
npm run test:integration
npm run demo:seed
npm run demo:reset      # MUST warn: destructive and local-only
npm run demo:lag
npm run demo:replay
npm run demo:load
npm run benchmark:run    # added in Phase 8; Kafka only initially
npm run benchmark:report # added in Phase 8; file-backed reports
```

Compose target profiles: `infra/compose/core.yml` for one broker and `infra/compose/replicated.yml` for a **fresh, separate cluster**. Avoid trying to migrate a single-node KRaft controller quorum to three voters in place during a seminar.

## Stop conditions

If the requested phase's gate fails: **fix the current phase** before adding features. If Docker resources are inadequate, reduce demo load or defer advanced broker-profile execution; do not silently replace Kafka with mock queues.

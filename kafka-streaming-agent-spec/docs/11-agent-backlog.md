# 11 — Agent-Ready Backlog (One Ticket at a Time)

This checklist is intended for an AI coding agent. **Default next task: P0-01**, unless the user explicitly selects another ticket. Every ticket has strict boundaries, dependencies, and proof. Ask no unnecessary broad product questions; use the decisions already documented.

## Global ticket workflow

For each ticket: identify touched paths → implement the smallest vertical slice → test → update relevant docs → report commands and evidence. Do not work on future tickets in the same PR. Ensure each ticket can be demonstrated independently.

## Phase 0 — Foundation

- [x] **P0-01 Workspace setup** — Initialize TypeScript monorepo, strict tsconfig, pnpm/npm workspace, lint/format/test scripts, minimal NestJS app, env validation. **Proof:** install, lint, typecheck and a smoke unit test pass. **No:** Postgres, replay, dashboard.
- [x] **P0-02 Local Kafka environment** — One KRaft broker RF1, Kafka UI, persistent Docker volume, health/readiness, reproducible start/stop docs. **Proof:** CLI can create/describe topic and connect on host/container network. **No:** 3-broker cluster.
- [x] **P0-03 Contracts foundation** — `IntegrationEventEnvelopeV1` Zod validation, deterministic ID/correlation fixtures, typed serializer/deserializer. **Proof:** accept valid envelope, reject mismatch/invalid cents/unsupported version.

## Phase 1 — Pipeline

- [x] **P1-01 HTTP producer** — `POST /api/v1/demo/purchases` generates purchase event, sends via KafkaJS, awaits ack, returns stable response. **Proof:** a real Kafka record exists with `key=customerId` and reported metadata.
- [x] **P1-02 Loyalty consumer** — NestJS/Node independent worker subscribes to one topic/group and logs broker-supplied `{eventId,key,topic,partition,offset}`; pure loyalty function calculates fixture points. **Proof:** API request yields correlated received event.
- [x] **P1-03 Phase 1 smoke script** — Reproducible startup, test event, Kafka UI/CLI verification and shutdown. **Proof:** fresh clone can follow documented commands.

## Phase 2 — Partitioning and ordering

- [x] **P2-01 Six-partition reset/migration** — Explicit destructive local reset and recreate `purchase.events` with six partitions, preserve stable key selection. **Proof:** metadata reports six; script refuses unconfirmed destructive reset.
- [x] **P2-02 Ordered workload fixture** — Multi-customer load generator and stable per-customer sequences (in-memory/single instance clearly labelled Phase 2 limitation). **Proof:** U001 events all same partition in increasing offsets; calculated output 82.
- [x] **P2-03 Partition inspector** — CLI/API/compact dashboard section lists actual partitions, offsets and record-key distribution. **Proof:** live Kafka observations, no modulo guesses.

## Phase 3 — Consumer groups and offsets

- [x] **P3-01 Independent Analytics group** — Separate worker/group processes same events, independent state output. **Proof:** both groups see identical event IDs.
- [x] **P3-02 Manual commit + lag reader** — Explicit offset commit and Admin-derived next-committed/log-end lag per partition. **Proof:** commit N+1, real lag readings handle `UNCOMMITTED`.
- [x] **P3-03 Pause/resume scenario** — CLI or allowlisted control to pause Analytics and later catch up. **Proof:** Analytics lag rises while Loyalty advances; resumes to 0.

## Phase 4 — Reliable DB/Kafka integration

- [x] **P4-01 Postgres migrations + repository ports** — Schema, unit-of-work adapter, SQL constraints, customer row locks. **Proof:** concurrent customer requests assign unique sequential event numbers.
- [x] **P4-02 CQRS purchase flow + outbox** — Replace direct source event publication with atomic purchase/outbox command handler and reader query; retain 202 response shape. **Proof:** DB source commit survives Kafka disconnection, outbox pending visible.
- [x] **P4-03 Publisher worker** — Single ordered outbox publisher, retries/backoff, stable IDs and ack status. **Proof:** Kafka restored → pending events published, ordering preserved.
- [x] **P4-04 Durable Loyalty processing** — Processed-event unique insertion in same DB transaction as loyalty update; manual offset after commit. **Proof:** duplicate event does not update points twice.
- [x] **P4-05 Durable Analytics processing** — Independent processed-event namespace and revenue projection; no cross-context write leak. **Proof:** Analytics commits independently.
- [x] **P4-06 Fault injection integration suite** — Crash after DB commit before offset; publisher crash after ack before outbox status update. **Proof:** redelivery, skip, one business state mutation.

## Phase 5 — Replay

- [x] **P5-01 Purchase invalidation event** — Transactional status transition and next customer sequence, outbox event. **Proof:** replay fixture has seq1/2/3 completed + seq4 invalidated.
- [x] **P5-02 Replay group + fixed boundary** — On-demand unique group, capture log-end high-watermarks, scan retained records, isolated staging. **Proof:** original groups' offsets unchanged, replay tracks its own offsets.
- [x] **P5-03 Two-pass correction calculation** — Collect invalid IDs; compute historical points in customer sequence order, 82→40. **Proof:** unit+integration fixture, no outward effects.
- [x] **P5-04 Retention failure reporting** — Detect insufficient retained history, fail clearly. **Proof:** controlled short-retention fixture produces explicit failure.

## Phase 6 — Scaling and rebalancing

- [ ] **P6-01 Independent worker containers** — Same `loyalty-v1` group, unique client/member IDs, Compose scaling without fixed `container_name`/host-port collisions. **Proof:** 1→3→6 assignments from Kafka.
- [ ] **P6-02 Load and performance** — Adjustable rate/customer cardinality, per-worker throughput, group lag/latency. **Proof:** repeatable tests report actual measured values.
- [ ] **P6-03 Kill/restart worker demo** — Terminate one consumer process, observe rebalance and recover. **Proof:** no loss/double business effects after resume.

## Phase 7 — Broker replication

- [ ] **P7-01 Separate three-node KRaft profile** — 3 brokers/controllers, persistent volumes, 3-voter quorum, configured RF3 internal topics. **Proof:** live brokers and controller quorum healthy.
- [ ] **P7-02 Replicated topic + producer acks** — Six partitions, RF3/minISR2, `acks=all`; observe topic leader/replica/ISR. **Proof:** metadata reflects config and writes succeed healthy.
- [ ] **P7-03 Broker leader failover** — Kill broker leading a partition while workload active; observe election and recovery. **Proof:** eligible new leader, resumed writes and ISR catch-up.
- [ ] **P7-04 Insufficient ISR experiment** — Bounded controlled fault with accurate quorum-vs-ISR diagnosis. **Proof:** trace errors, no false guarantee claims.

## Phase 8 — Kafka instrumentation and benchmark baseline

- [ ] **P8-01 Kafka topology/dashboard** — Real partition/group/broker metadata and event traces. **Proof:** data reconciles with Kafka UI/CLI.
- [ ] **P8-02 Reliability and replay panels** — Show duplicate attempts, committed changes, replay progress and before/after state. **Proof:** no fake telemetry.
- [ ] **P8-03 Operator control safeguards** — Local-only allows-list, explicit approvals for destructive operations, no Docker socket browser access. **Proof:** unauthorized/disabled control attempts fail.
- [ ] **P8-04 Kafka instrumentation + histogram correctness** — Instrument attempted/confirmed publish, deliveries/unique processing, p50/p95/p99, lag and CPU/memory/disk. **Proof:** measured time series and known-distribution percentile tests.
- [ ] **P8-05 Deterministic Kafka transport benchmark** — Implement fixed corpus, bounded in-flight generator, a small benchmark-only transport boundary with one Kafka adapter, file-backed manifest/results/histograms. **Proof:** identical seed gives identical corpus hash, three real Kafka trial results and event reconciliation.
- [ ] **P8-06 Kafka baseline dashboard/report** — Build plots from saved Kafka trial files; differentiate transport-only from DB-inclusive system throughput. **Proof:** offline report regeneration with no hardcoded values.

## Phase 9 — Redis Streams and RabbitMQ comparison adapters

- [ ] **P9-01 Benchmark ports and runner isolation** — Review and stabilize the existing Phase 8 benchmark-only ports, manifest and metric pipeline in their isolated context; retain the working Kafka adapter. **Proof:** Kafka benchmark unchanged and old Kafka app still passes tests.
- [ ] **P9-02 Redis Streams adapter** — `XADD`, consumer-group `XREADGROUP`, `XACK`, pending recovery, explicit persistence/retention. **Proof:** same payload hash and result schema as Kafka; client integration tests pass.
- [ ] **P9-03 RabbitMQ Quorum Queue adapter** — Durable queues/bindings, publisher confirms, manual ACK, prefetch and native metrics. **Proof:** same payload hash and result schema; recovered unacked message test passes.
- [ ] **P9-04 Fair experiment profiles** — Single-node transport-only profile C1 and optional fan-out/replicated profiles; document durability constraints. **Proof:** >=3 successful trials per comparison cell, manifest complete, no false equivalence.
- [ ] **P9-05 Comparison chart/report** — Latency percentiles, confirmed/unique throughput, native backlog and resource metrics, explicit capability table. **Proof:** charts generated solely from files and incompatible runs rejected or clearly separated.
- [ ] **P9-06 RabbitMQ native Streams (optional)** — Separate Streams/Super Streams adapter, retained history, offset read and topology. **Proof:** independent stream-mode tests, no Quorum semantics reused.

## Phase 10 — Final integrated seminar

- [ ] **P10-01 Unified monitoring & comparison UI** — Kafka-specific cluster/ISR/offset panels plus separate broker-neutral transport measurements and evidence links. **Proof:** no sample values presented as actual data.
- [ ] **P10-02 Full demo runbook and fresh-clone QA** — Automated pipeline, rebalance, duplicate, replay, leader failover, baseline and cross-broker tests. **Proof:** exact commands, raw data, version/host settings and known limitations included.

## Agent task prompt template

```text
Read AGENTS.md and the architecture/phase docs.
Implement ONLY ticket <TICKET-ID> from docs/11-agent-backlog.md.
Preserve DDD/CQRS boundaries and all Kafka correctness invariants.
Add tests and update docs, execute checks, report evidence and blockers.
Do not implement later phases, fake metrics, or change the chosen stack.
```

## Default initial request to an agent

```text
Implement P0-01 only. Create the strict TypeScript workspace and minimal NestJS shell,
with formatter/linter, test runner, validated environment setup, and smoke test.
Do not add PostgreSQL, React, Docker/Kafka, microservices, or repository abstractions
until their tickets are selected. Report commands actually run.
```

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
- [x] **P4-03 Transactional outbox streaming via Debezium CDC** — PostgreSQL WAL logical replication (`pgoutput`) via Debezium Connect and Outbox Event Router to `purchase.events`. Append-only outbox table, zero database polling (ADR-017). **Proof:** `pnpm cdc:verify` proves 3 ordered purchases published and rolled-back transaction excluded; `pnpm cdc:recovery` proves zero message loss across connector pause/resume.
- [x] **P4-04 Durable Loyalty processing** — Processed-event unique insertion in same DB transaction as loyalty update; manual offset after commit. **Proof:** duplicate event does not update points twice.
- [x] **P4-05 Durable Analytics processing** — Independent processed-event namespace and revenue projection; no cross-context write leak. **Proof:** Analytics commits independently.
- [x] **P4-06 Fault injection integration suite** — Crash after DB commit before offset commit; CDC connector pause/resume under committed source writes. **Proof:** redelivered event skipped without double points; WAL-buffered events delivered with stable IDs upon resumption.

## Phase 5 — Replay

- [x] **P5-01 Purchase invalidation event** — Transactional status transition and next customer sequence, outbox event. **Proof:** replay fixture has seq1/2/3 completed + seq4 invalidated.
- [x] **P5-02 Replay group + fixed boundary** — On-demand unique group, capture log-end high-watermarks, scan retained records, isolated staging. **Proof:** original groups' offsets unchanged, replay tracks its own offsets.
- [x] **P5-03 Two-pass correction calculation** — Collect invalid IDs; compute historical points in customer sequence order, 82→40. **Proof:** unit+integration fixture, no outward effects.
- [x] **P5-04 Retention failure reporting** — Detect insufficient retained history, fail clearly. **Proof:** controlled short-retention fixture produces explicit failure.

## Phase 6 — Scaling and rebalancing

- [x] **P6-01 Independent worker containers** — Same `loyalty-v1` group, unique client/member IDs, dynamic fleet scaling from 1 to 3 instances. **Proof:** real dynamic partition assignments from Kafka broker (`RoundRobinAssigner`).
- [x] **P6-02 Load and performance** — Adjustable rate/customer cardinality, per-worker throughput, group lag/latency. **Proof:** repeatable tests report actual measured values in Workload and Benchmark studios.
- [x] **P6-03 Kill/restart worker demo** — Terminate one consumer process, observe rebalance and recover. **Proof:** zero loss/double business effects after resume (`Kill Worker Instance` demo).

## Phase 7 — Broker replication

- [x] **P7-01 Separate three-node KRaft profile** — 3 combined brokers/controllers (Node IDs 1, 2, 3), persistent volumes, 3-voter controller quorum, configured RF3 internal topics. **Proof:** `docker-compose.cluster.yml` profile and `pnpm cluster:verify` (`scripts/verify-kraft-cluster.mjs`).
- [x] **P7-02 Replicated topic + producer acks** — Six partitions, RF3/minISR2, `acks=all`; verified topic leader, replica set [1, 2, 3], and ISR=[1, 2, 3]. **Proof:** `pnpm demo:replicated:p7` (`scripts/verify-replicated-topic.mjs`).
- [x] **P7-03 Broker leader failover** — Stop container leading partition 0, observe surviving 2/3 controller quorum elect new leader, prove writes succeed with degraded ISR size 2, restart container, and observe follower catch-up back to ISR size 3. **Proof:** `pnpm demo:failover:p7` (`scripts/verify-leader-failover.mjs`).
- [x] **P7-04 Insufficient ISR experiment** — Enforce min.insync.replicas threshold rejection (`NOT_ENOUGH_REPLICAS`) while cleanly preserving 3/3 KRaft controller quorum to distinguish quorum failure from minISR failure. **Proof:** `pnpm demo:minisr:p7` (`scripts/verify-insufficient-isr.mjs`).

## Phase 8 — Kafka instrumentation and benchmark baseline

- [x] **P8-01 Kafka topology/dashboard** — Real partition/group/broker metadata and event traces. **Proof:** data reconciles with Kafka UI/CLI.
- [x] **P8-02 Reliability and replay panels** — Show duplicate attempts, committed changes, replay progress and before/after state. **Proof:** no fake telemetry.
- [ ] **P8-03 Operator control safeguards** — Local-only allows-list, explicit approvals for destructive operations, no Docker socket browser access. **Proof:** unauthorized/disabled control attempts fail.
- [x] **P8-04 Kafka instrumentation + histogram correctness** — Instrument attempted/confirmed publish, deliveries/unique processing, p50/p95/p99, lag. **Proof:** measured time series and known-distribution percentile tests in Benchmark Studio.
- [x] **P8-05 Deterministic Kafka transport benchmark** — High-throughput load runner, bounded in-flight requests, Transactional Outbox vs Direct Broker Ingress. **Proof:** repeatable trial runs with real Kafka measurements.
- [x] **P8-06 Kafka baseline dashboard/report** — Comprehensive audit export (.md and .json) aggregating cluster positions, rebalance events, replay diffs, and latency histograms. **Proof:** offline report regeneration with no hardcoded values.

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

## Kafbat UI migration — 2026-11-01

- [x] Replace the administration UI in both Compose profiles with pinned Kafbat UI v1.5.0; update environment example, launcher messages, frontend labels, and operator documentation. Runtime cluster validation requires an active Kafka profile.

## Milestone: Observability Infrastructure (Prometheus & Grafana) — 2026-11-01

- [x] **OBS-01 Freeze telemetry contract and inventory**: ADR-018 written and approved. Metric dictionary, explicit measurement boundaries, and finite cardinality budget established.
- [x] **OBS-02 Application metrics MVP**: Process-local registry in `@lab/observability`, `MetricsModule`, `HttpMetricsInterceptor`, `/metrics` endpoint across `producer-api`, `loyalty-worker`, `analytics-worker`, and `replay-worker`. Transaction commit and consumer delivery/ack boundaries instrumented without polluting domain models.
- [x] **OBS-03 Provision Prometheus and Grafana**: `docker-compose.monitoring.yml`, template config renderer (`scripts/render-monitoring-config.mjs`), stable provisioned datasource (`prometheus`), provisioned canonical dashboards (`apps-pipeline.json`, `kafka-system-diagnostics.json`), and monitoring verification CLI (`scripts/verify-monitoring.mjs`).

## Milestone: Standalone Ingress Benchmark (Kafka vs Redis Streams vs RabbitMQ Streams) — 2026-11-01

- [x] **ING-01 Capability Proof & Pinned Adapters**: KafkaJS (`acks=-1`), `ioredis` (AOF verified, offline queue disabled), `rabbitmq-stream-js-client` (native Stream protocol 5552, wire `PublishConfirm` callback mapping).
- [x] **ING-02 Isolated Runner & Workload Generator**: `@lab/ingress-benchmark` Node.js CLI package with bounded concurrency scheduler, deterministic byte-reproducible Mulberry32 payload generator with SHA-256 hash, exact decimal MB/s arithmetic, and monotonic latency histograms.
- [x] **ING-03 Isolated Kafka Baseline**: Docker Compose single-node KRaft broker on port 9094, partition 0, acks=-1. Zero business topic pollution.
- [x] **ING-04 Redis Streams Publisher**: Single-node Redis 7 on port 6380, AOF enabled (`appendfsync no`, `noeviction`), per-record server acknowledged `XADD`.
- [x] **ING-05 RabbitMQ Native Streams Publisher**: RabbitMQ 3.13 Stream on port 5552, server PublishConfirm correlation mapping, clean error and disconnect handling.
- [x] **ING-06 Shared Monitoring Integration**: Prometheus target (`job_name: ingress-benchmark` on port 3006) and Grafana dashboard (`ingress-benchmark.json`).
- [x] **ING-07 Matrix Execution & Reasonableness Analysis**: 3 repetitions per cell across concurrencies 1, 16, 64 with 1 KiB payload. Complete Little's Law, RTT, and mechanical bottleneck analysis.



## Milestone: Real Application workspace — local scope delivered

- [x] REAL-01: bounded source history and exact purchase lookup; context-owned event receipts and HTTP error evidence.
- [x] REAL-02: transactional server push, shared visible-page subscriptions, reconnect/resnapshot and interrupted Vite SSE regression.
- [x] REAL-03: responsive sidebar, business/event pages, idempotency/burst exercises, observations and independent worker controls.
- [x] REAL-04: approved coordinated cutover locking, complete global review/confirmation, isolated partial-promotion and failure acceptance; browser keyboard/layout/outage/navigation checks.

See `docs/real-application-walkthrough.md` and consolidated C13 in `docs/real-application-implementation-progress.md`. Production deployment/cross-tab streaming are outside the delivered local scope; existing non-fatal bundle warnings remain.

Simulation Playground enhancement: S1 correctness, S2 validated paused scaling and S3 virtual-clock rendering are implemented. Engine/view data are separated; panels publish at 10 Hz and particles follow Pause/Step/speed. Presenter layout and session persistence remain in progress. See SIMULATION-PLAYGROUND-ENHANCEMENT-PLAN.md.

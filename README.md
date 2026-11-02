# Apache Kafka Distributed Event Streaming Lab

An engineering seminar lab demonstrating Apache Kafka streaming mechanics, reliability patterns (Transactional Outbox, at-least-once, consumer idempotency), retention & replay, consumer group scaling, KRaft broker failover, and fair broker benchmarking (Kafka vs Redis Streams vs RabbitMQ Quorum Queues).

---

## 1. Prerequisites

- **Node.js**: Active LTS `v22.x` (verified with `v22.21.0`)
- **pnpm**: `>= 9` (configured for workspace resolution)
- **Docker & Docker Compose**: v2+ with Docker Desktop or Engine running

---

## 2. Quick Start

### Workspace Setup

```bash
# Install workspace dependencies
pnpm install

# Setup local environment configuration
cp .env.example .env

# Verify code formatting, linter, and typechecking
pnpm format:check
pnpm lint
pnpm typecheck

# Run unit tests
pnpm test:unit
```

### One-Command Interactive Lab Launch

```bash
# Start Docker (Kafka, Postgres, Kafbat UI), all 5 microservices, and Interactive UI in ONE command:
pnpm start
# (or: pnpm lab:start / pnpm lab:up)

# Stop all microservices, clear port listeners, and stop Docker containers cleanly:
pnpm stop
# (or: pnpm lab:stop / pnpm lab:down)
```

### Local Kafka Environment (KRaft RF=1)

```bash
# Start Apache Kafka 4.3.1 and Kafbat UI in background
pnpm kafka:up

# Check container status and health
pnpm kafka:status

# Run automated broker and topic lifecycle verification
pnpm kafka:verify

# View broker logs
pnpm kafka:logs

# Stop Kafka environment
pnpm kafka:down
```

### Phase 1: Real Event Pipeline Demo

```bash
# Run automated Phase 1 end-to-end smoke test
pnpm demo:smoke

# Or start services manually:
# Terminal 1: Producer API (Port 3000)
pnpm start:producer

# Terminal 2: Loyalty Worker Consumer (Port 3001, Group loyalty-v1)
pnpm start:loyalty

# Terminal 3: Send a purchase event via curl
curl -X POST http://localhost:3000/api/v1/demo/purchases \
  -H "Content-Type: application/json" \
  -d '{"customerId":"U001","amountCents":12000}'

# Inspect message on Kafka broker
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic purchase.events \
  --from-beginning \
  --max-messages 1 \
  --formatter-property print.key=true \
  --formatter-property key.separator=" | "

# Inspect consumer group offset and lag
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group loyalty-v1
```

### Phase 2: Partitioning & Ordering Demo

```bash
# Reset topic to 6 partitions (requires confirmation guardrail)
pnpm demo:reset --confirm

# Run automated multi-customer ordered workload and 82-point verification
pnpm demo:workload

# Inspect live broker partition metadata, group lag, and key distribution (CLI)
pnpm demo:partitions

# Query live Kafka broker topic telemetry via REST API
curl -s http://localhost:3000/api/v1/ops/kafka/topics/purchase.events
```

### Phase 3: Consumer Groups & Offsets Demo

```bash
# Start Analytics Worker manually (Port 3002, Group analytics-v1)
pnpm start:analytics

# Run automated independent consumer group demonstration and verification
pnpm demo:groups

# Run automated manual offset commit (N + 1) and lag reader verification
pnpm demo:lag

# Run automated consumer pause, lag accumulation, and catch-up demonstration
pnpm demo:pause-resume
```

### Phase 4: Reliable DB/Kafka Integration & Outbox Demo

```bash
# Apply Prisma database migrations to PostgreSQL
pnpm --filter @lab/producer-api prisma:migrate
pnpm --filter @lab/loyalty-worker prisma:migrate
pnpm --filter @lab/analytics-worker prisma:migrate

# Run automated concurrent customer sequence locking verification (P4-01)
pnpm demo:concurrency

# Run automated CQRS purchase command, Read DAO query, and transactional outbox verification (P4-02)
pnpm demo:outbox

# Provision CDC Outbox Connector & Apply Migrations (P4-03 / ADR-017)
pnpm cdc:setup

# Run automated Debezium CDC transactional outbox & rollback exclusion verification (P4-03 / ADR-017)
pnpm cdc:verify

# Run automated Debezium CDC pause/resume & WAL buffering recovery verification (P4-03 / ADR-017)
pnpm cdc:recovery

# Run automated durable loyalty processing & consumer idempotency verification (P4-04)
pnpm demo:loyalty-dedup

# Run automated durable analytics processing & consumer idempotency verification (P4-05)
pnpm demo:analytics-dedup

# Run automated fault injection integration suite: consumer crash & recovery (P4-06)
pnpm demo:faults
```

### Phase 5: Replay & Stream Correction Demo

```bash
# Run automated purchase invalidation event flow verification (P5-01)
pnpm demo:invalidation

# Run automated bounded replay & fixed boundary verification (P5-02)
pnpm demo:replay

# Run automated two-pass stream correction (82 -> 40 points) verification (P5-03)
pnpm demo:correction
```

### Phase 7: Multi-Broker KRaft Cluster & Replication Demo

```bash
# Start 3-node KRaft cluster profile (Node IDs 1, 2, 3 with combined broker/controller quorum)
pnpm cluster:up

# Verify cluster topology and 3-node KRaft controller quorum (P7-01)
pnpm cluster:verify

# Verify replicated topic (RF=3, minISR=2, acks=all) across 6 partitions (P7-02)
pnpm demo:replicated:p7

# Run automated broker leader failover & ISR catch-up experiment (P7-03)
pnpm demo:failover:p7

# Run automated insufficient ISR rejection (NOT_ENOUGH_REPLICAS) experiment (P7-04)
pnpm demo:minisr:p7

# Stop 3-node KRaft cluster
pnpm cluster:down
```

- **Kafka Cluster Brokers (Host listeners)**:
  - Broker 1: `localhost:9092` (Controller `9093`)
  - Broker 2: `localhost:9094` (Controller `9095`)
  - Broker 3: `localhost:9096` (Controller `9097`)
- **KRaft Controller Quorum**: `1@kafka-1:9093,2@kafka-2:9095,3@kafka-3:9097` (3 voters)
- **Kafka Broker (Single-node profile)**: `localhost:9092`
- **Kafka Broker (Internal network)**: `kafka:29092`
- **Kafbat UI**: `http://localhost:8080`
- **Debezium Connect REST API**: `http://localhost:8083`
- **PostgreSQL Multi-DB Cluster**: `localhost:5433` (`postgres:16-alpine`, mapped internally to 5432)
  - `purchases_db`: `purchases`, `purchase_customers`, `outbox_events`, `http_idempotency_keys`
  - `loyalty_db`: `loyalty_accounts`, `processed_events`, `replay_runs`, `replay_staging`
  - `analytics_db`: `analytics_summary`, `processed_events`

---

## 2.1 Ingress Benchmark & Monitoring Quick Start

### 1. Isolated Broker Ingress Benchmark Infrastructure
Start the dedicated, isolated broker containers (Kafka, Redis, RabbitMQ) configured with explicit volume storage and identical resource budgets (`2.0 CPUs`, `2048 MB RAM`):

```bash
# 1. Start isolated benchmark broker containers (Kafka, Redis, RabbitMQ)
pnpm benchmark:up

# 2. Stop benchmark broker containers when done
pnpm benchmark:down
```

### 2. Independent Official Benchmark Tools (GitHub / Docker Hub)
For an editable centralized configuration, use `config/official-benchmark.json`:

```bash
pnpm benchmark:preview all
pnpm benchmark:run kafka
pnpm benchmark:run redis
# Set BENCH_RABBITMQ_PASSWORD in .env before RabbitMQ or all:
pnpm benchmark:run all
```

See [the launcher guide](docs/official-benchmarks.md) for settings, results, disk checks and automatic owned-data cleanup. Defaults are small smoke workloads. The launcher preserves raw official output under `benchmark/official/`; it is separate from the web Purchase Benchmark Studio. The manual commands below remain available.

All benchmark scenarios are executed using established, high-performance official benchmark tools running in isolated containers under identical CPU/memory constraints:

```bash
# Kafka Official Producer Perf Test (Single stream / 1 partition):
docker run --rm --network ingress-benchmark_default --cpus="2.0" --memory="2048m" \
  apache/kafka:4.3.1 /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic perf-p1 --num-records 3500000 --record-size 1024 --throughput -1 \
  --command-property acks=all batch.size=262144 buffer.memory=67108864 linger.ms=0 \
  --warmup-records 200000 --bootstrap-server kafka-bench:29092

# RabbitMQ Official Stream Perf Test:
docker run --rm --network ingress-benchmark_default --cpus="2.0" --memory="2048m" \
  pivotalrabbitmq/stream-perf-test:latest \
  -u rabbitmq-stream://guest:guest@rabbitmq-bench:5552 \
  -s 1024 -x 16 -y 0 -z 30 --streams scen_a_stream -bs 256

# Redis Official Memtier Benchmark (Pipelined XADD):
docker run --rm --network ingress-benchmark_default --cpus="2.0" --memory="2048m" \
  redislabs/memtier_benchmark:latest \
  -s redis-bench -p 6379 --protocol=redis -t 2 -c 8 --pipeline=16 --test-time=30 \
  --command="XADD scen_a_key * payload __data__" -d 1024
```

### 3. Broker Ingress Monitoring Stack (Prometheus & Grafana)
A dedicated, lightweight monitoring stack provisions Prometheus (with Kafka Exporter, Redis Exporter, and native RabbitMQ Prometheus plugin) and Grafana.

```bash
# 1. Start Prometheus, Grafana, and broker exporters
pnpm monitoring:up

# 2. Access Dashboards:
# - Grafana: http://localhost:3005 (Anonymous Viewer enabled, or admin/admin)
#   Dashboard: "Broker Ingress Benchmark" (real-time 2s refresh of offset advance & confirm rate)
# - Prometheus: http://localhost:9090 (targets: http://localhost:9090/targets)

# 3. Stop monitoring stack
pnpm monitoring:down
```

---

## 3. Project Structure

```text
.
├── apps/
│   ├── producer-api/          # NestJS API: POST /api/v1/demo/purchases -> Outbox
│   │   └── prisma/            # Prisma schema and PostgreSQL migrations
│   ├── loyalty-worker/        # NestJS Worker: Consumer group 'loyalty-v1' -> Points Calculator & scaling
│   │   └── prisma/            # Prisma schema and PostgreSQL migrations
│   ├── analytics-worker/      # NestJS Worker: Consumer group 'analytics-v1' -> Sales Aggregation
│   │   └── prisma/            # Prisma schema and PostgreSQL migrations
│   ├── replay-worker/         # NestJS Worker: On-demand unique consumer group replay & isolated staging
│   │   └── prisma/            # Prisma schema and client for replay staging
│   └── demo-ui/               # React 19 + Vite Interactive Topology & Benchmark Studio
├── docker/
│   └── debezium/              # Outbox connector configuration & PostgreSQL replication setup
├── packages/
│   ├── contracts/             # Integration event schemas, Zod validation, factories, serializer
│   └── observability/         # Structured logger, unified Kafka execution coordinator, error hierarchy
├── scripts/
│   ├── start-lab.mjs          # One-command lab orchestrator (Docker + CDC + Build + 5 services)
│   ├── stop-lab.mjs           # One-command lab teardown (terminates ports and stops containers)
│   ├── setup-cdc.mjs          # Provisions Debezium connector and applies PostgreSQL migrations
│   ├── verify-cdc.mjs         # End-to-end CDC delivery, ordering, and rollback exclusion proof
│   ├── verify-cdc-recovery.mjs # CDC connector pause/resume and WAL buffer delivery recovery proof
│   ├── verify-kafka.mjs       # Automated Kafka readiness and topic lifecycle verification
│   ├── smoke-phase1.mjs       # Phase 1 end-to-end smoke runner with broker inspection
│   ├── reset-topic.mjs        # Controlled destructive topic reset and 6-partition migration
│   ├── generate-ordered-workload.mjs # Multi-customer ordered load generator and 82-point verification
│   ├── inspect-partitions.mjs # Live partition inspector reporting actual broker metadata and key distribution
│   ├── verify-consumer-groups.mjs    # Multi-group independent stream consumption verification
│   ├── verify-lag.mjs                # Manual offset commit N + 1 and UNCOMMITTED lag reader verification
│   ├── demo-pause-resume.mjs         # Consumer pause, lag accumulation, and catch-up verification
│   ├── verify-postgres-concurrency.mjs # Concurrent customer sequence row locking and PostgreSQL verification
│   ├── verify-outbox-offline.mjs     # Transactional outbox persistence and offline Kafka survival verification
│   ├── verify-durable-loyalty.mjs    # Durable loyalty processing and consumer deduplication verification
│   ├── verify-durable-analytics.mjs  # Durable analytics processing and consumer deduplication verification
│   ├── verify-purchase-invalidation.mjs # Purchase invalidation event flow verification
│   ├── verify-replay-boundary.mjs    # Bounded replay & fixed boundary verification
│   └── verify-two-pass-correction.mjs # Two-pass correction calculation (82 -> 40 points) verification
├── docker-compose.yml         # Apache Kafka 4.3.1 (KRaft) + Kafbat UI + PostgreSQL 16 + Debezium Connect
├── .env.example               # Centralized configuration schema documentation
├── package.json               # Monorepo scripts and shared dev tooling
├── pnpm-workspace.yaml        # pnpm workspace package patterns
├── tsconfig.base.json         # Shared strict TypeScript configuration
├── eslint.config.mjs          # Modern ESLint 9 flat configuration
└── vitest.config.ts           # Vitest root multi-project test runner
```

---

## 4. Architecture & Engineering Standards

- **Clean Architecture & SOLID**: Inversion of Control, loosely coupled modules, high cohesion.
- **Dependency Injection**: No direct `process.env` calls inside application logic. Configuration is validated via Zod schemas and injected through `AppConfigService`.
- **Zero Hardcoded Values**: All infrastructure parameters, ports, images, and cluster identifiers are configurable via environment variables with validated defaults.
- **Single Source of Truth**: Partition keys are strictly derived from `aggregateId` (`customerId`) via `@lab/contracts` factories and accessors.
- **Evidence-Based Proof**: Every milestone is verified with automated tests and live container observations rather than speculative assertions.

---

## 5. Milestone Progress

### Phase 0 — Foundation

- [x] **P0-01: Workspace Setup** — Monorepo foundation, strict TypeScript, ESLint 9 flat config, Vitest multi-project, NestJS shell with injectable configuration, smoke unit tests.
- [x] **P0-02: Local Kafka Environment** — Single-node KRaft broker RF1, Kafbat UI, persistent named volume, health checks, topic lifecycle verification script.
- [x] **P0-03: Contracts Foundation** — `IntegrationEventEnvelopeV1` Zod validation, deterministic ID/correlation fixtures, typed serializer/deserializer.

### Phase 1 — Pipeline MVP

- [x] **P1-01: HTTP Producer** — `POST /api/v1/demo/purchases` generates purchase event, sends via KafkaJS with `acks=-1`, awaits ack, returns 202 Accepted with Kafka metadata. Real Kafka record confirmed with `key=customerId`.
- [x] **P1-02: Loyalty Consumer** — Independent worker subscribing to `purchase.events` in group `loyalty-v1`, calculating loyalty points fixture ($120/$200/$100 -> 82 points). Live broker integration test verified.
- [x] **P1-03: Phase 1 Smoke Script** — Reproducible end-to-end startup, purchase production, consumer verification, and broker inspection (`pnpm demo:smoke`).

### Phase 2 — Partitioning and Ordering

- [x] **P2-01: Six-Partition Reset & Migration** — Explicit controlled reset recreating `purchase.events` with 6 partitions (`pnpm demo:reset -- --confirm`). Refuses unconfirmed reset.
- [x] **P2-02: Ordered Workload Fixture** — Multi-customer load generator with per-customer sequential ordering (U001 P001/P002/P003 -> 82 points).
- [x] **P2-03: Partition Inspector** — Live inspection of key distribution across partitions and broker offsets.

### Phase 3 — Consumer Groups and Offsets

- [x] **P3-01: Independent Analytics Group** — Separate worker process in consumer group `analytics-v1`, independently calculating sales aggregations (revenue, order counts, unique customers) from the same `purchase.events` stream (`pnpm demo:groups`).
- [x] **P3-02: Manual Commit + Lag Reader** — Explicit manual offset commit discipline (`autoCommit: false`, `N + 1`) and live Kafka Admin lag calculation `(logEndOffset - committedOffset)` per partition with `UNCOMMITTED` handling (`pnpm demo:lag`).
- [x] **P3-03: Pause/Resume Scenario** — CLI and HTTP allowlisted control to pause Analytics, observe lag accumulation while Loyalty proceeds, and catch up back to 0 lag upon resume (`pnpm demo:pause-resume`).

### Phase 4 — Reliable DB/Kafka Integration & Outbox

- [x] **P4-01: Postgres Migrations & Concurrency** — Customer sequence allocation with row locks and atomic transactions (`pnpm demo:concurrency`).
- [x] **P4-02: CQRS Purchase Flow & Outbox Persistence** — Command handler persisting purchase + outbox atomically, query handler for read model (`pnpm demo:outbox`).
- [x] **P4-03: Debezium CDC Outbox Streaming** — Zero-polling PostgreSQL WAL logical replication via Debezium Connect and Outbox Event Router (`pnpm cdc:verify`, `pnpm cdc:recovery`, ADR-017).
- [x] **P4-04: Durable Loyalty Processing** — At-least-once consumer with unique `processed_events` deduplication in loyalty database (`pnpm demo:loyalty-dedup`).
- [x] **P4-05: Durable Analytics Processing** — Independent analytics projection with consumer idempotency and isolation (`pnpm demo:analytics-dedup`).
- [x] **P4-06: Fault Injection Integration Suite** — Post-DB-commit pre-offset crash injection and consumer recovery (`pnpm demo:faults`).

### Phase 5 — Replay & Stream Correction

- [x] **P5-01: Purchase Invalidation Event** — Monotonic sequence allocation for invalidation events with single-partition ordering (`pnpm demo:invalidation`).
- [x] **P5-02: Replay Group & Boundary Isolation** — On-demand unique consumer group scanning from offset 0 to fixed high-watermark boundaries without affecting live groups (`pnpm demo:replay`).
- [x] **P5-03: Two-Pass Stream Correction** — Two-pass state reconstruction (discovering invalidations then recomputing historical points 82 -> 40) in isolated staging tables (`pnpm demo:correction`).
- [x] **P5-04: Retention Failure Guardrails** — Explicit failure detection when earliest retained offsets exceed required historical sequence.

### Phase 6 — Scaling & Dynamic Consumer Rebalancing

- [x] **P6-01: Dynamic Fleet Scaling** — Live scale up / scale down of consumer group instances with partition rebalancing (`RoundRobinAssigner`).
- [x] **P6-02: Worker Kill & Failover** — Graceful disconnect and failover reassigning partitions with zero duplicate points.

### Phase 8 — Observability Platform & Interactive Topology Studio

- [x] **P8-01: Interactive Topology Studio** — Real-time interactive canvas with live broker, partition, producer, consumer group, and database nodes (`apps/demo-ui`).
- [x] **P8-02: Benchmark Studio & Audit Evidence** — End-to-end telemetry measurement, percentile latency tracking, and Markdown/JSON audit export.
- [x] **P8-04: Live Workload Simulator** — Single, burst, and continuous traffic generator with partition routing and latency controls.

### Standalone Ingress Benchmark: Kafka vs Redis Streams vs RabbitMQ Streams (2026-11-01)

- [x] **ING-01 to ING-07: Isolated Stream Benchmark**: Dedicated CLI `@lab/ingress-benchmark` comparing Apache Kafka (KRaft, `acks=-1`), Redis Streams (AOF `appendfsync no`, `noeviction`), and RabbitMQ Streams (native stream protocol 5552, server `PublishConfirm` tracking).
  - **Single-Node Async Profile**: Like-for-like asynchronous persistence baseline.
  - **Commands**:
    - `pnpm benchmark:up`: Starts isolated benchmark broker containers.
    - `pnpm benchmark:matrix`: Runs the complete automated matrix (3 repetitions across concurrencies 1, 16, 64) and prints the median comparison table.
    - `pnpm benchmark:ingress -- --broker <kafka|redis-streams|rabbitmq-streams>`: Runs an individual trial with customizable payload, concurrency, and count.
    - `pnpm benchmark:down`: Stops isolated benchmark containers.
  - **Shared Monitoring**: Auto-scraped by Prometheus on port 3006; Grafana dashboard provisioned at `ingress-benchmark.json`.

### Kafbat UI migration (2026-11-01)

Both single-broker and replicated Compose profiles use Kafbat UI (`ghcr.io/kafbat/kafka-ui:v1.5.0`). Configure `KAFKA_UI_IMAGE` and `KAFKA_UI_PORT` in `.env`; the default UI address is http://localhost:8080. Service name: `kafbat-ui`. Run `docker compose up -d kafbat-ui` for the single-broker profile, or use `-f docker-compose.cluster.yml` for the replicated profile. If an older UI container remains after upgrading, stop and remove only that old UI container before starting Kafbat; do not delete broker or database volumes.


## Real Application workspace implementation

The approved workspace is being implemented incrementally. Read the [persistent progress and next steps](docs/real-application-implementation-progress.md) before resuming. The read APIs, coordinated cutover locking and server-push foundation are implemented and tested. Simulation repairs are committed separately; the sidebar and read-only real overview are implemented. Fresh-service browser push acceptance and the remaining business/recovery pages are unfinished.

Real UI checkpoint C7: purchase entry, retained idempotency key, paginated history and canonical detail implemented; browser acceptance and remaining business/recovery features pending. See `docs/real-application-implementation-progress.md`.

Real UI checkpoint C8: separate event evidence, independent consumer receipts and confirmed invalidation implemented; browser acceptance and pagination-independent evidence lookup pending. See persistent progress documentation.

Real UI C9: pagination-independent evidence, Loyalty/Analytics pages and independent consumer controls implemented; UI purchase/idempotency/push and Analytics pause/resume verified. Recovery and final acceptance remain.

Real UI C10: same-customer burst form and actual allocated sequence/evidence results verified in browser. Recovery and final acceptance remain.

Real UI C11: replay lifecycle, complete all-customer staging review and explicit global confirmation implemented;95-customer browser review verified without executing global promotion. Phase6 acceptance/polish remains.

Real UI C12: validated tool links/proxy origins, bounded HTTP requests and presenter walkthrough added;253 unit tests pass. Mobile and partial-outage browser acceptance remains pending.

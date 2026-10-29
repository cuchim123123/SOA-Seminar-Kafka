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

### Local Kafka Environment (KRaft RF=1)

```bash
# Start Apache Kafka 4.3.1 and Kafka UI in background
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
```

- **Kafka Broker (Host listener)**: `localhost:9092`
- **Kafka Broker (Internal network)**: `kafka:29092`
- **Kafka UI**: `http://localhost:8080`

---

## 3. Project Structure

```text
.
├── apps/
│   ├── producer-api/          # NestJS API: POST /api/v1/demo/purchases -> Kafka Publisher
│   ├── loyalty-worker/        # NestJS Worker: Consumer group 'loyalty-v1' -> Points Calculator
│   └── analytics-worker/      # NestJS Worker: Consumer group 'analytics-v1' -> Sales Aggregation
├── packages/
│   ├── contracts/             # Integration event schemas, Zod validation, factories, serializer
│   └── observability/         # Structured logger, unified Kafka execution coordinator, error hierarchy
├── scripts/
│   ├── verify-kafka.mjs       # Automated Kafka readiness and topic lifecycle verification
│   ├── smoke-phase1.mjs       # Phase 1 end-to-end smoke runner with broker inspection
│   ├── reset-topic.mjs        # Controlled destructive topic reset and 6-partition migration
│   ├── generate-ordered-workload.mjs # Multi-customer ordered load generator and 82-point verification
│   ├── inspect-partitions.mjs # Live partition inspector reporting actual broker metadata and key distribution
│   └── verify-consumer-groups.mjs    # Multi-group independent stream consumption verification
├── docker-compose.yml         # Apache Kafka 4.3.1 (KRaft) + Kafka UI
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
- [x] **P0-02: Local Kafka Environment** — Single-node KRaft broker RF1, Kafka UI, persistent named volume, health checks, topic lifecycle verification script.
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
- [ ] **P3-03: Pause/Resume Scenario** — CLI or allowlisted control to pause Analytics and later catch up.

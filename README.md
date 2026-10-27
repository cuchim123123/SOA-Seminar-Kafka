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

- **Kafka Broker (Host listener)**: `localhost:9092`
- **Kafka Broker (Internal network)**: `kafka:29092`
- **Kafka UI**: `http://localhost:8080`

---

## 3. Project Structure

```text
.
├── apps/
│   └── producer-api/          # Minimal NestJS API shell (Health endpoints, Config injection)
├── kafka-streaming-agent-spec/# Authoritative specification documents and ADRs
│   ├── AGENTS.md
│   ├── README.md
│   └── docs/                  # Architectural specs, backlog, runbooks, ADRs
├── scripts/
│   └── verify-kafka.mjs       # Automated Kafka readiness and topic lifecycle verification
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
- **Evidence-Based Proof**: Every milestone is verified with automated tests and live container observations rather than speculative assertions.

---

## 5. Milestone Progress

- [x] **P0-01: Workspace Setup** — Monorepo foundation, strict TypeScript, ESLint 9 flat config, Vitest multi-project, NestJS shell with injectable configuration, smoke unit tests.
- [x] **P0-02: Local Kafka Environment** — Single-node KRaft broker RF1, Kafka UI, persistent named volume, health checks, topic lifecycle verification script.
- [x] **P0-03: Contracts Foundation** — `IntegrationEventEnvelopeV1` Zod validation, deterministic ID/correlation fixtures, typed serializer/deserializer.
- [x] **P1-01: HTTP Producer** — `POST /api/v1/demo/purchases` generates purchase event, sends via KafkaJS with `acks=-1`, awaits ack, returns 202 Accepted with Kafka metadata. Real Kafka record confirmed with `key=customerId`.
- [x] **P1-02: Loyalty Consumer** — Independent worker subscribing to `purchase.events` in group `loyalty-v1`, calculating loyalty points fixture ($120/$200/$100 -> 82 points). Live broker integration test verified.
- [ ] **P1-03: Phase 1 Smoke Script** — Reproducible end-to-end startup, purchase production, consumer verification, and broker inspection.

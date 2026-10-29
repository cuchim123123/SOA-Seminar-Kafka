# Engineering Instructions & Coding Standards — Kafka Streaming Lab

> **Mandatory Agent Instruction File**: This document governs all architectural and implementation activities across this repository. It must be consulted and strictly adhered to during every coding step.

---

## 1. Core Mission & Laboratory Identity

1. **Kafka Mechanics are the Central Subject**:
   - The laboratory exists to demonstrate and master Apache Kafka mechanics: topics, partitions, record keys, offsets, consumer groups, lag measurement, transactional outbox, manual offset commits, consumer idempotency, retention, bounded replay, consumer rebalancing, KRaft multi-broker clustering, and evidence-backed broker benchmarking.
2. **Business Domain is Strictly a Minimal Fixture**:
   - Purchases, loyalty points, and sales summaries are **synthetic fixtures** designed to trigger Kafka events and prove correctness.
   - **Do NOT** over-engineer complex e-commerce or retail business machinery. Keep domain logic pure, deterministic, and minimal so Kafka operations remain the primary focus.

---

## 2. Architectural & Engineering Principles

### SOLID & Clean Architecture
- **Single Responsibility Principle (SRP)**:
  - Inbound adapters (HTTP controllers, Kafka consumers) only validate, deserialize, and dispatch.
  - Application handlers coordinate business use cases and interact with ports.
  - Pure domain functions contain calculation logic with zero I/O or framework dependencies.
  - Infrastructure adapters implement persistence, broker messaging, or external protocols.
- **Open / Closed Principle (OCP)**:
  - Design for extension through interfaces, strategy patterns, and polymorphic handlers without mutating core algorithms.
- **Liskov Substitution Principle (LSP)**:
  - Adapter implementations must strictly satisfy port contracts without unexpected behavior or bypassed constraints.
- **Interface Segregation Principle (ISP)**:
  - Keep ports small and focused. Prefer small, role-specific interfaces (`EventPublisher`, `Clock`, `CustomerRepository`) over monolithic interfaces.
- **Dependency Inversion Principle (DIP)**:
  - Application and domain layers define ports (interfaces); infrastructure implements them. The composition root wires concrete implementations into abstractions.

### Loose Coupling & High Cohesion
- **Bounded Context Isolation**:
  - **Purchase Simulation**, **Loyalty**, **Sales Analytics**, **Kafka Operations**, and **Streaming Benchmark** are strictly separated.
  - Never share mutable domain models or database tables across bounded contexts.
  - Only versioned event envelopes (`@lab/contracts`) cross context boundaries.
- **No Anti-Patterns**:
  - **NO Repetitive Code**: Extract cohesive utilities or helper abstractions.
  - **NO Nested If-Else / Switch Nightmares**: Use strategy patterns, discriminated unions, or lookup tables.
  - **NO Premature Complexity**: Only build what is required for the active phase ticket.

### Configuration & Environment Management
- **NEVER Call `process.env` Directly**:
  - Direct calls to `process.env` inside controllers, services, or domain logic are forbidden.
  - Raw environment variables must be validated at application bootstrap using versioned Zod schemas.
  - Configuration must be registered in a dedicated module (e.g., `ConfigModule`) and injected via dependency injection (`AppConfigService` or injection tokens).
- **Zero Hardcoding**:
  - No hardcoded ports, hostnames, credentials, cluster IDs, or topic configurations.
  - Every variable must be parameterized through documented environment variables in `.env.example` with validated defaults.

### Architectural Uncertainty & Decision Protocol
- **NEVER Assume on Ambiguity**: In case of any uncertainty regarding architectural design, technology choices, bounded context boundaries, database schema strategies, or operational mechanics, the agent **must never assume or silently guess**.
- **Mandatory User Consultation**: Always pause and explicitly ask the user for clarification, presenting options and trade-offs before proceeding with any implementation.

---

## 3. Kafka Correctness & Safety Rules

1. **Ordering Invariant**:
   - Kafka guarantees ordering **only within a single partition**.
   - Partition keys must consistently use `customerId`.
   - Business ordering must be established before records reach Kafka (using transactional sequence allocation in Phase 4+).
2. **Offset Semantics**:
   - Consumed message offset `N` must be committed as `N + 1`.
   - Offsets are scoped to `(group, topic, partition)`. Never sum offsets across partitions into a fake global sequence.
   - Treat offsets as decimal strings or `BigInt` to prevent 64-bit integer precision loss.
3. **Consumer Groups**:
   - Independent consumer groups (`loyalty-v1`, `analytics-v1`) maintain independent offset tracking. Pausing or lagging one group must never affect another.
4. **Reliability & Idempotency**:
   - Broker producer idempotence does not guarantee end-to-end database idempotency.
   - In consumer processing (Phase 4+), commit the database transaction first, then commit Kafka offset second. Deduplicate via unique `(consumer_name, event_id)` records.
5. **Stream Key & Aggregate Authority (ADR-016)**:
   - Kafka record partition keys must be derived from `envelope.aggregateId` via `getEventPartitionKey(envelope)`.
   - `aggregateId` and `payload.customerId` must remain identical. To prevent inconsistency, envelopes must be created via single-source-of-truth factories (`createPurchaseCompletedEnvelope`, `createPurchaseInvalidatedEnvelope`) taking `customerId` once.


---

## 4. Modern Tooling & Evergreen Stack

- **Apache Kafka**: Native KRaft mode on modern releases (`apache/kafka:4.3.1`+).
- **Node.js**: Active LTS `v22.x` (`.nvmrc` pinned).
- **Package Manager**: `pnpm` workspaces with hoisted root dev dependencies.
- **TypeScript**: Strict mode enabled (`strict: true`, `noUnusedLocals: true`, `noUnusedParameters: true`).
- **Linter & Formatter**: Modern ESLint 9 flat configuration (`eslint.config.mjs`) and Prettier.
- **Testing**: Vitest multi-project runner (`vitest.config.ts`).

---

## 5. Verification & Quality Gate Workflow

Before completing any task or moving between milestones:
1. **Format Check**: `pnpm format:check` (must pass without styling issues).
2. **Lint**: `pnpm lint` (0 errors, 0 warnings).
3. **Typecheck**: `pnpm typecheck` (0 TypeScript errors across all workspace projects).
4. **Unit Tests**: `pnpm test:unit` (100% passing tests).
5. **Infrastructure Verification**: `pnpm kafka:verify` (when Kafka infrastructure is active).
6. **Documentation**: Update backlog status and milestone progress in `README.md` and `docs/11-agent-backlog.md`.

# Engineering Standards and Architecture Rules

This rule is automatically enforced on every prompt and coding iteration.

## 1. Laboratory Identity: Kafka First
- The central subject of this project is **Apache Kafka mechanics** (partitions, keys, consumer groups, lag, outbox, replay, scaling, KRaft clustering).
- The business domain (purchases, loyalty points, revenue count) is **synthetic and simulated only**. Do not overcomplicate business domain models.

## 2. SOLID and Clean Architecture
- **Single Responsibility**: Controllers/consumers are thin inbound adapters. Handlers execute use cases. Domain is pure calculation without I/O.
- **Dependency Inversion**: Application and domain layers define ports. Infrastructure implements ports.
- **No Direct `process.env` Calls**: Configuration must be parsed via Zod schemas and injected through NestJS DI (`AppConfigService` or tokens).
- **Loose Coupling & High Cohesion**: Strict separation of bounded contexts (`Purchase Simulation`, `Loyalty`, `Sales Analytics`, `Benchmark`).
- **No Anti-Patterns**:
  - No copy-paste repetitive logic.
  - No deep nested if-else ladders; use strategy or discriminated union patterns.
  - Zero hardcoding of ports, images, cluster IDs, or magic strings. Use `.env.example` with validated defaults.

## 3. Modern Tooling
- Apache Kafka 4.3.1 (KRaft pure mode)
- Node.js 22 LTS
- pnpm workspaces
- ESLint 9 flat config (`eslint.config.mjs`)
- Vitest multi-project runner (`vitest.config.ts`)

## 4. Verification Gate
Every step must execute and pass:
1. `pnpm format:check`
2. `pnpm lint`
3. `pnpm typecheck`
4. `pnpm test:unit`

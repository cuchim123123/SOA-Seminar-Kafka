# 17 — Specification Audit, Quality Enforcement and Definition of Done

This document records a **documentation consistency review**, not a review of an implemented system. No source code, live throughput results, Redis/RabbitMQ adapters or benchmark runs are claimed to exist yet.

## Core requirements mapped to source of truth

| Requirement | Normative docs | Key validation |
|---|---|---|
| Kafka is seminar topic; Loyalty synthetic only | 00, 01, 12 | No extra e-commerce scope |
| Clean Architecture + DDD + CQRS + SOLID | AGENTS, 02, 13 | Domain modules import no framework/database/broker libraries |
| Versioned source event contracts | 04 | Invalid/unknown schema rejected visibly |
| Kafka ordering / group offsets / lag | 03, 09 | Real broker metadata and sequence tests |
| Outbox + durable idempotency | 05, 08 | Crash-before-offset yields one DB update |
| Intentional Kafka replay | 03, 05, 07 | Fresh group, fixed watermark, 82→40 in staging |
| Horizontal consumer scaling | 03, 06, 07 | Multiple OS processes/containers and actual reassignment |
| Three-broker RF3/ISR/failover target | 10, 06 | Leader election with one broker down |
| Throughput and latency measurement | 09, 14, 16 | Raw samples, percentiles, counters, rate units |
| Kafka vs Redis Streams vs RabbitMQ | 13, 15, 16 | Honest mode/capability and durability labels |
| Incremental coding-agent implementation | AGENTS, 06, 11 | One ticket, tests/gate before next |
| Repeatable test and result quality | 08, 14, 16 | Real broker integration, no invented results |

## Design review checkpoints for every implementation ticket

### Clean Architecture

- Domain is pure and independently unit-tested.
- Application handlers orchestrate commands/queries, transactions and domain; no KafkaJS/AMQP/Redis/Postgres import.
- Adapters translate external DTOs/events and are the only home for client SDKs and raw SQL.
- Composition root is the only place concrete ports/adapters are wired.
- Controller, Kafka message handler and benchmark worker do not calculate rewards or query DB tables directly.
- Shared packages don't become a hidden shared domain; DTO/schema versioning is explicit.

### DDD and CQRS

- Context ownership of state is explicit; no cross-context table mutation.
- Command side is separate from query/read side, but no mandatory physically separate command and query databases.
- `Event` (domain), `IntegrationEvent` (broker), `Command` and `Query` are distinct concepts.
- No artificial aggregate graph or Event Sourcing framework for the simulator.

### Broker correctness

- Group IDs unique when independence is needed; same group ID used deliberately for worker sharing.
- Kafka offset is string or `bigint`, never unsafe JS `number` in core calculations.
- DB transaction commits before offset/ack; duplicate gate condition stops repeated mutation.
- Retry/backpressure finite; poison record, DLQ and sequence gap policies explicit.
- Per-key order established before Kafka publish; no fabricated global ordering claim.
- Kafka ISR and leader selection demonstrated with actual metadata; broker replication not confused with consumer fan-out.

### Comparative methodology

- Kafka, Redis Streams, RabbitMQ Quorum and optional RabbitMQ Streams named explicitly (not generic Redis/RabbitMQ).
- Kafka native capabilities remain visible. Unsupported competitor capabilities are marked `UNSUPPORTED`/`N/A`.
- Common workload generator, exact payload and benchmark metric formulas unchanged across adapters.
- Payload size, concurrency, delivery acknowledgment, persistence, batch, retention and topology recorded per trial.
- Throughput reported with clear denominator and confirmed/processed distinction, percentiles use mergeable distributions.
- Comparison charts group runs by compatible benchmark track/profile; errors/incomplete runs not hidden.
- Baseline must run repeatably before adding Redis/RabbitMQ, not three unfinished adapters in Phase 0.

## Static-documentation cross-checks (performed on this package)

- Phase 1 intentionally has no Postgres and no Outbox; Phase 4 introduces durable state. Do not claim Phase 1 durable application processing.
- Phase 2 intentionally recreates an empty single-node Kafka topic with six partitions; do not change live ordered partition count.
- Phase 7 final profile is three **combined** KRaft broker/controller processes with RF3 and a 3-voter quorum; it is a local teaching topology, not production-independent controllers.
- `acks=all` and minISR are not synonymous; producer ack waits on ISR and minISR is a minimum eligibility threshold.
- Paused consumer stays a group member; killed consumer causes membership change/rebalance, timing depends on failure detection.
- Catch-up, intentional replay and event-sourced state design remain separate.
- Metric labels distinguish producer attempted vs confirmed and consumer delivery attempts vs unique acknowledged processing.
- Cross-broker comparisons are added as a **later independent workstream** after the Kafka demo and its instrumentation are correct.

## Exit criteria for the whole project

1. A fresh clone can run Phase 1 and the full Kafka profile via pinned Compose images and documented scripts.
2. The Kafka demonstration has reproducible ordering, lag, scaling, duplicate, replay, ISR and failover cases with real telemetry.
3. Instrumentation stores raw time-series and histogram data and produces real throughput/latency/resource charts.
4. The same deterministic payload and benchmark logic runs against Kafka, Redis Streams and RabbitMQ Quorum Queue (RabbitMQ Streams is optional), with capability caveats and configuration manifests.
5. At least three successful trials exist for every charted result; tests and failure cases documented; no hardcoded benchmark outputs.
6. All relevant lint/typecheck/unit/integration/E2E gates pass and documented commands reproduce the demonstrations.
7. Backend APIs are local-only for destructive controls, credentials aren't committed, workloads contain no PII.
8. A coding agent delivers exact changed paths, commands it ran, evidence, known limits and next ticket for each iteration.

## Agent instruction when asked to "implement the project"

Start from Phase 0/Ticket P0-01, unless the user names a later ticket. Report a short plan, implement that ticket, and run validations. **Do not scaffold all broker adapters, dashboards and cluster profiles before a working Kafka vertical slice exists.**

# Apache Kafka Distributed Event Streaming Lab

**Implementation specification for a coding agent**  
**Language:** English  
**Focus:** Apache Kafka fundamentals, consumer coordination, delivery reliability, replay, scalability, replication, **throughput/latency observability, and a later fair comparison with Redis Streams and RabbitMQ**.  
**Simulated workload:** e-commerce `PurchaseCompleted` / `PurchaseInvalidated` messages processed by Loyalty and Sales Analytics workers.

> The business scenario is intentionally small. This is a Kafka engineering and demonstration project, **not** an e-commerce platform, an event sourcing framework, or a production-ready payment system.

## What will be built

A TypeScript monorepo containing:

- **Purchase Simulator / HTTP API**: accepts single purchase requests and generates events.
- **Event Producer / Outbox Publisher**: sends typed integration events into Kafka; direct publication for the Phase 1 learning slice, transactional outbox from Phase 4 onward.
- **Kafka cluster**: one KRaft broker initially; final target is three combined broker/controller nodes, six partitions, replication factor three, minimum ISR two.
- **Loyalty Worker Group**: consumers that perform a tiny stateful calculation to make ordering and idempotency observable.
- **Analytics Worker Group**: independent consumer group producing simple metrics.
- **Replay Worker**: reads retained records via a separate group, optionally recomputing a bounded test projection.
- **Monitoring/Control API and React Dashboard**: observes **real** partition/offset/group/broker state and runs controlled local demonstrations.
- **Load Generator / Fault Injection**: repeatable backlog, crash, rebalance, and leader-failover scenarios.
- **Benchmark Runner + Adapter Ports** (Phase 9): same synthetic workload and measurement contract implemented for Kafka, Redis Streams, and RabbitMQ Quorum Queues; optional native RabbitMQ Streams adapter. Transport-only throughput separated from end-to-end application performance.

## Source of truth and scope

PostgreSQL is authoritative for purchase and loyalty state once introduced in Phase 4. Kafka is the **partitioned transport and retained log**, with bounded retention. Replaying from Kafka does not make this an event-sourced architecture. The replay demo must not create duplicate external effects.

## Read in this order

1. [AGENTS.md](AGENTS.md) — **binding instructions for coding agents**.
2. [Scope and acceptance](docs/00-scope-and-goals.md).
3. [Architecture and dependency rules](docs/01-system-architecture.md).
4. [DDD, Clean Architecture and CQRS](docs/02-domain-ddd-cqrs.md).
5. [Kafka guarantees, partition strategy and topic setup](docs/03-kafka-design.md).
6. [Event and API contracts](docs/04-contracts-and-apis.md).
7. [Persistence, outbox and idempotency](docs/05-persistence-and-reliability.md).
8. [Milestone implementation plan](docs/06-implementation-phases.md).
9. [Live demonstration runbook](docs/07-demo-runbook.md).
10. [Testing and quality gates](docs/08-testing-and-quality.md).
11. [Monitoring, security and operation](docs/09-observability-and-operations.md).
12. [Final three-broker cluster](docs/10-kraft-replication.md).
13. [Agent issue backlog](docs/11-agent-backlog.md).
14. [Architecture decisions and constraints](docs/12-architecture-decisions.md).
15. [Broker interfaces and clean adapter boundaries](docs/13-broker-ports-and-adapters.md).
16. [Benchmark methodology, throughput and metrics](docs/14-benchmark-methodology-and-metrics.md).
17. [Kafka vs Redis Streams vs RabbitMQ comparison](docs/15-kafka-redis-rabbitmq-comparison.md).
18. [Benchmark runbook, CLI and result schemas](docs/16-comparison-runbook-and-data-contracts.md).
19. [Quality review and definition of done](docs/17-audit-and-definition-of-done.md).

## Milestone strategy

| Phase | Focus | Intended completion state |
|---|---|---|
| 1 | Producer → one broker → consumer | HTTP request produces one real record; worker logs key, partition, offset. |
| 2 | Multi-partition ordering | Six partitions; stable key; demonstrably ordered per customer. |
| 3 | Groups, offsets, lag | Independent Loyalty/Analytics consumers; pause/resume with real lag. |
| 4 | Database and reliable delivery | PostgreSQL, outbox, manual commit, at-least-once, deduplication. |
| 5 | Retention and replay | Independent replay group; repeatable fraud-invalidation dataset. |
| 6 | Consumer scaling | Multiple Loyalty containers in same group; rebalances and recovery. |
| 7 | Broker replication | Three-node KRaft cluster; RF=3, ISR, leader election, `acks=all`. |
| 8 | Kafka observability and baseline | Live Kafka metrics and reproducible Kafka-only transport benchmarks. |
| 9 | Kafka vs Redis Streams vs RabbitMQ | Isolated broker adapters, same workload and fair measured profiles. |
| 10 | Final dashboard and seminar | Real comparison charts, audited runbooks and evidence archive. |

**Start at Phase 0 / Ticket P0-01 only.** Do not build features from future phases until their prerequisites and tests pass. The final architecture is a **destination**, not the starting implementation.

## Technology decisions

- **Language:** TypeScript (strict mode), Node.js active LTS verified at implementation time.
- **Framework:** NestJS for HTTP/application wiring, `@nestjs/cqrs` for application commands/queries.
- **Kafka client:** KafkaJS (pinned compatible release; verify features against installed version).
- **State:** PostgreSQL from Phase 4; parameterized SQL via `pg` with migrations for explicit atomic transaction semantics.
- **Frontend:** React + TypeScript (introduced gradually).
- **Validation:** Zod for runtime integration-event contracts; NestJS request DTO validation at HTTP boundary.
- **Environment:** Docker Compose; use Kafka UI as the initial observability interface.
- **Tests:** Jest or Vitest consistently across packages, real Kafka/Postgres integration tests for guarantees.

## Definition of done for the complete project

- Can demonstrate topics, partitions, keys and offsets with data from a real broker.
- Can show independent consumer-group offsets and backlog/catch-up.
- Can demonstrate at-least-once processing **without** duplicate database effects.
- Can replay retained history with a new consumer group, not by querying Postgres and calling it Kafka replay.
- Can change consumer instance count and observe actual assignments and lag.
- Can kill one broker in the final three-broker lab and see replica leadership recovery.
- Records benchmark and failure observations rather than claiming theoretical scalability.
- Architecture obeys clean dependencies and handler separation.

## Reference documentation

- [Kafka documentation](https://kafka.apache.org/documentation/)
- [KafkaJS producing](https://kafka.js.org/docs/producing)
- [KafkaJS consuming](https://kafka.js.org/docs/consuming)
- [NestJS CQRS](https://docs.nestjs.com/recipes/cqrs)

Documentation describes the planned repository. Commands, files and services described here do not exist until implemented by the agent.

## New instructor requirement — benchmarking and comparison

The finished project must **observe real throughput, latency, lag/backlog and broker resource use** and support comparison against **Redis Streams** and **RabbitMQ Quorum Queues** without compromising Kafka-first architecture. A native **RabbitMQ Streams/Super Streams** adapter is a distinct optional mode. The primary comparison uses a **transport-only runner** with byte-identical payloads, bounded concurrency, native broker acknowledgments, p50/p95/p99 metrics and repeated runs. An optional application end-to-end track includes PostgreSQL and is not presented as a pure broker comparison.

**Sequence:** Phases 0–7 Kafka functionality, Phase 8 instrumentation + Kafka baseline, Phase 9 Redis Streams and RabbitMQ Quorum Queue comparison adapters (RabbitMQ Streams optional), Phase 10 dashboard/bench report polish. No benchmark results are prefilled. A coding agent must follow the new docs 13–17 and add benchmark capability only at its assigned ticket.

**One ZIP delivery:** this folder is the only source tree needed for the coding agent. Extract the archive; point the agent at `AGENTS.md`; implement **P0-01 only** unless a ticket is specified.

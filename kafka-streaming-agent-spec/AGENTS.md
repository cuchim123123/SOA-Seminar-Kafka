# Coding Agent Instructions — Kafka Streaming Lab

This document is authoritative for any AI coding agent working on this repository. Read `README.md` and the relevant files in `docs/` **before changing code**.

## Task execution contract

1. **Implement only the user-requested phase or ticket.** If none is specified, start with **Phase 0, Ticket P0-01** in `docs/11-agent-backlog.md`; move to Phase 1 only after the foundation gate passes.
2. State the planned files and the **single measurable outcome** before coding.
3. Make the smallest cohesive change that passes the phase's acceptance criteria. No premature abstractions or infrastructure for later phases.
4. Run relevant format, lint, typecheck, unit tests, and the current phase's integration/smoke test. If Docker or a dependency is unavailable, disclose what could not be verified.
5. End with changed paths, commands actually run, results, operational instructions, and remaining risks. Do not claim success for unrun tests or invent benchmark scores.
6. Never silently skip a failed phase gate or change the architecture. If a design needs changing, update `docs/12-architecture-decisions.md` first.

## Non-negotiable architecture rules

- Apply Clean Architecture/Hexagonal boundaries: **domain → no framework**, application → domain and ports, adapters/infrastructure → application ports. Composition root wires dependencies.
- Use bounded contexts: **Purchase Simulation**, **Loyalty**, **Sales Analytics**, **Kafka Operations**. Do not share mutable domain models across contexts.
- Use CQRS at the application boundary: commands mutate state; queries read state. A Kafka integration event is **not automatically** a local domain event or a CQRS command.
- HTTP controllers and Kafka message handlers are **inbound adapters**. They validate/deserialize and dispatch an application use case. They must not contain reward calculations, raw SQL, or event publication logic.
- Domain logic is pure, deterministic, testable without NestJS, Kafka, Postgres, the network, or `Date.now()`.
- Repository interfaces, clock, ID generator, transaction/unit-of-work, event publisher, and telemetry boundaries are **ports** owned by application/domain as appropriate. Infrastructure implements them.
- Keep shared packages minimal: event contracts, tiny Kafka utilities, logging, and test fixtures; no shared catch-all `utils` and no cross-context repository imports.
- Use explicit types, error classes and structured logs. `strict: true`. Never use `any` to avoid typing the boundary.
- Prefer one short meaningful method to a generic class hierarchy. No unnecessary DDD machinery for a simulated workload.

## Kafka safety and correctness rules

- Kafka **ordering is only within a topic partition**. `customerId` is the record key, with stable partition count/partitioner for ordering demonstrations. Business transaction order must be established **before** records reach Kafka.
- Consumer groups are independent; consumers **inside a group** share partitions. Three distinct groups are **not** three-way worker scaling.
- Committed offset is the **next offset to consume**. Report lag per partition as log-end offset minus next committed offset, with edge cases for uninitialized/expired offsets.
- Phase 4 onward: **commit DB transaction first, Kafka offset second**. Use a unique `(consumer_name, event_id)` insert and branch on whether a new row was inserted. All related state changes must occur in that same transaction.
- `INSERT ... ON CONFLICT DO NOTHING` by itself does **not** skip subsequent updates. Check `RETURNING` and stop processing duplicates.
- No automatic offset commit for stateful workers; configure manual behavior explicitly. Do not fire-and-forget asynchronous processing from Kafka callbacks.
- Broker-level idempotent producer/transactions **do not make PostgreSQL side effects exactly once**.
- Outbox publication is **at least once**. A duplicate publish after broker ack and before marking the outbox is a valid failure case.
- Event IDs are stable across retries; never mint a new event ID on outbox retry.
- Kafka retention is finite/configurable; replay works only while needed history exists or there is an explicit snapshot/archival strategy.
- Read-only replay and state rebuild are **separate** paths. Rebuild writes an isolated staging projection and never resends external side effects.
- On invalid event/schema/poison record, use bounded retry + inspectable failure state. Never silently commit a failed event or skip a necessary predecessor to conceal lag.
- Do **not** modify partition count on a live ordered dataset. Reset/recreate a demo topic in a destructive, documented procedure when moving from Phase 1 to Phase 2.
- Do not label one broker RF=1 as replicated HA; final demonstration requires three brokers and RF=3.

## Production-quality hygiene even in a demo

- Validate payloads at all trust boundaries; use cents/integer for money and UTC timestamps.
- Prefer parameterized SQL and DB constraints for correctness. No SQL concatenation with inputs.
- Config from validated environment; no embedded passwords, tokens, or broker endpoints in application code.
- No real payment/PII. Synthetic customer IDs only.
- Add correlation IDs and event IDs to logs and tests; do not log secrets.
- Graceful shutdown: stop intake, await in-flight work, disconnect Kafka clients, close DB pools.
- Ensure Docker volumes persist Kafka data when needed; destructive reset must be explicit and labelled.
- Disable fault injection and Docker control APIs by default. Never expose a Docker socket to a public web UI.
- Avoid unrelated infrastructure: no Kubernetes, Debezium, schema registry, event sourcing framework, saga, microservice gateway or workflow engine without a dedicated approved ticket. **Redis Streams and RabbitMQ are approved only for the later comparison track** (Phase 9); never introduce them into the Kafka application in early phases.

## Review checklist for every pull request

- Is each new type/module in the correct bounded context and layer?
- Do handlers have one responsibility and depend on interfaces rather than concrete brokers/ORMs?
- Do exceptions, retries and commits preserve correctness under a crash?
- Are data contracts versioned and backward-compatible or explicitly migrated?
- Are all demo numbers grounded in actual measurements rather than hard-coded into the UI?
- Are unit/integration tests included for positive, duplicate, out-of-order and failure paths where applicable?
- Does the phase demo work from documented commands?

## Agent response format

```text
Milestone / Ticket:
Implemented:
Architecture decisions:
Files changed:
Commands executed and results:
Acceptance criteria met / not met:
Known limitations:
Next ticket:
```

If a tool or environment is unavailable, report the blocker and give reproducible instructions. Do not invent command output, screenshots, benchmarks or test results.

## Broker comparisons and benchmark non-negotiables

- Read `docs/13-broker-ports-and-adapters.md` through `docs/17-audit-and-definition-of-done.md` before implementing any benchmarking or alternative broker ticket.
- **Kafka-first:** measure a functioning Kafka-only pipeline and baseline before building Redis/RabbitMQ adapters. Comparison work starts in **Phase 9** after instrumentation in Phase 8.
- Keep **Transport Benchmark** as a separate bounded context. The deterministic workload generator, measurement formulas, result schema, domain calculations and UI reporting must not be duplicated across broker adapters.
- Distinguish Redis **Streams** from Pub/Sub, and RabbitMQ **Quorum Queues** from **Streams/Super Streams**. Never normalize away delivery, retention, ordering, ACK or failure differences.
- For comparisons, always state: broker mode/topology, replication/persistence, producer confirmation rule, consumer acknowledgment rule, payload bytes, batching/compression, concurrency, worker count, hardware and software versions.
- Measure and report **attempted vs broker-confirmed publishes**, **deliveries vs unique successful processing**, p50/p95/p99 latency, backlog/lag **by native definition**, resource cost, failed/ambiguous IDs and trial variance.
- Benchmark transport-only without NestJS/DB/React in the measured hot path; optionally benchmark full application separately. Use a bounded generator, warm-up, fixed trials and machine-readable run manifests.
- Do not produce a result chart until real trials completed. Do not assume one broker is faster based on theory, single-run peak, or mismatched durability.
- Extension proof: adding a new broker adapter must not modify the benchmark-core metric formulas, event corpus or Loyalty/Purchase bounded contexts. Add adapter contract/integration tests.
- No application abstractions solely for possible future brokers in Phases 0–7. Build interfaces only when there is concrete use.

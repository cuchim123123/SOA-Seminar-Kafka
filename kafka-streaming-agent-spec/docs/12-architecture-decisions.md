# 12 — Architecture Decision Records, Trade-Offs and Known Risks

The following decisions are settled. An agent should not choose an alternative without documenting the trade-off and asking for an explicit decision when a substantive divergence is required.

## ADR-001 — Kafka seminar, not an event sourcing product

**Decision:** Kafka is the central subject; simulated purchases/loyalty exist only to demonstrate stream semantics. PostgreSQL holds authoritative current state from Phase 4; Kafka retained event history supports demonstration replay but is not presumed eternal. **Consequence:** minimal business APIs, no general event store or snapshot framework. **Status:** accepted.

## ADR-002 — Incremental architecture over big-bang scaffolding

**Decision:** one broker, one partition, direct Kafka publication for Phase 1; introduce multi-partition groups then DB/outbox, then replay/scaling, finally multi-broker. **Consequence:** Phase 1 lacks reliable DB→Kafka publication and is explicitly non-durable; later transition requires adapter refactor. **Status:** accepted.

## ADR-003 — Clean Architecture with pragmatically scoped DDD and CQRS

**Decision:** bounded contexts and dependency inversion; pure loyalty domain; command/query use cases; Kafka consumers as inbound adapters. **Consequence:** modest initial boilerplate but independently testable domain and message-handling pipeline. Avoid creating layers/classes without an implemented behavior. **Status:** accepted.

## ADR-004 — Independent worker processes for real scaling

**Decision:** one Loyalty group can have multiple workers; Analytics uses a separate group. Each worker must be independently launchable in Phase 6. **Consequence:** one shared NodeJS process with multiple group consumers does not prove process-failure/rebalancing scalability. **Status:** accepted.

## ADR-005 — Partition by customer ID, serial publication

**Decision:** all customer events keyed by `customerId`, `customerSequence` allocated transactionally, single ordered publisher initially. **Consequence:** partition hot spots possible; one customer's events cannot be parallelized across partitions within a given topic/consumer group while preserving this sequence; scaling relies on many customers. **Status:** accepted.

## ADR-006 — PostgreSQL transactional outbox + at-least-once consumer

**Decision:** Phase 4 uses atomic purchase+outbox, a retrying publisher, manual consumer offsets and DB processed-event uniqueness. **Consequence:** duplicate records can exist in Kafka under crash/retry; domain side effects must remain deduplicated. Kafka producer idempotence does not cover HTTP requests or Postgres mutations. **Status:** accepted.

## ADR-007 — Kafka-centric, bounded replay

**Decision:** use a new replay group, frozen dataset/high-watermarks and a two-pass scan for invalidations, staging projection and optional controlled cutover. **Consequence:** deliberately not online arbitrary-time event-sourcing rebuild; write freeze is acceptable for seminar. **Status:** accepted.

## ADR-008 — Final replicated 3-broker KRaft cluster

**Decision:** fresh separate final Compose profile: 3 combined broker/controller nodes, 3 KRaft voters, 6 partitions, RF3, minISR2, acks=all. **Consequence:** higher local RAM/CPU, local process-failure proof only, quorum coupled to brokers; not production KRaft topology. **Status:** accepted.

## ADR-009 — Explicit SQL for atomic correctness

**Decision:** use PostgreSQL `pg` adapter with parameterized SQL and migrations, not ORM magic, for locks, `INSERT .. ON CONFLICT .. RETURNING`, and transaction boundaries. **Consequence:** more explicit SQL but predictable round trips and clear concurrency semantics. Keep SQL exclusively in infrastructure adapters. **Status:** accepted.

## ADR-010 — Observability must be real

**Decision:** offsets, lag, partition assignment, ISR and leadership shown only from broker/client data; no hardcoded topology or modulo-derived assignment on production screens. **Consequence:** monitoring API/metrics may require multiple sources; unavailable values stay unknown. **Status:** accepted.

## ADR-011 — Local-only fault operations

**Decision:** first implement CLI/Compose fault scenarios; only then add allowlisted local monitoring controls, disabled by default. **Consequence:** no public Docker daemon access. **Status:** accepted.

## ADR-012 — Keep external HTTP acknowledgment honest

**Decision:** API `202 Accepted` means producer/broker accepted event in Phase 1, and DB+outbox accepted purchase in Phase 4; response does not assert that downstream Loyalty completed. Status endpoints show eventual completion separately. **Consequence:** source acceptance and final processing are intentionally distinct; clients must read/poll for completion. **Status:** accepted.

## Risks, mitigations and open proof obligations

| Risk | Mitigation / required test |
|---|---|
| Kafka key ordering assumed sufficient | Customer row-lock sequence assignment + ordered publisher; concurrency test. |
| Live invalidation yields stale loyalty | `correction_required` until bounded replay/cutover; dashboard explicitly distinguishes. |
| Consumer duplicate points | Unique processed-event constraint in same tx as mutation; crash injection test. |
| Outbox multiclaim reorders | Only one publisher, explicit per-aggregate ordering; no SKIP LOCKED multiworker. |
| Replay history incomplete | Check retained offsets/required events, fail clearly; no silent DB substitution. |
| KafkaJS offset precision lost | Treat offsets as decimal strings/BigInt. |
| KafkaJS idempotent producer setting changes across releases | Verify installed version and test; never infer app-level exactly-once. |
| Weak throughput improvement | Report actual measurements and potential Postgres bottleneck/hot partitions. |
| 3 nodes all on one host | Document process-failure-only HA demonstration. |
| Stopping two combined nodes confounds ISR/quorum | Distinguish errors; targeted ISR test if practical. |
| Monitoring API compromises host | No browser Docker socket, local-only allowlisted commands, disabled by default. |
| Over-engineering consumes seminar time | Phase gates, only build actively needed contexts/adapters. |

## Documentation change procedure

If a design choice changes, update this ADR file, the relevant architecture/contract/phase section, and impacted tests in the **same change**. A coding agent may propose an ADR but must not silently contradict accepted decisions.

## ADR-013 — Kafka-first observability and isolated cross-broker comparison

**Decision:** instructor-requested throughput/latency monitoring is required; Redis Streams and RabbitMQ Quorum Queue comparison is approved, but only in a separate Streaming Benchmark bounded context after Kafka baseline instrumentation is validated. RabbitMQ Streams/Super Streams is a separate optional comparison mode. **Consequence:** Kafka application remains Kafka-specific where useful; identical workload/measurement/output contracts are shared, not fake common delivery guarantees. No speculative abstractions in early phases. **Status:** accepted.

## ADR-014 — Benchmark fairness and immutable evidence

**Decision:** compare transport-only tests separately from end-to-end Postgres-inclusive tests; require broker-native ACK/persistence/replication/topology disclosures, bounded load, raw measurement, histogram aggregation, 3+ repeat trials and machine-readable manifests. Distinguish attempts vs confirmed publishes and deliveries vs unique successful processing. **Consequence:** unsupported capabilities are marked N/A; results are scenario-specific and cannot support universal product rankings. **Status:** accepted.

## ADR-015 — Kafka client selection and compatibility verification

**Decision:** Defer Kafka client library selection and native dependency installation to milestone P1-01 (Pipeline MVP). Do not describe `@confluentinc/kafka-javascript`'s KafkaJS-compatible API as completely interchangeable with KafkaJS without rigorous verification. Compatibility with KRaft, Admin API feature coverage, clustering behavior, and cross-platform native compilation constraints will be explicitly verified and documented during P1-01 prior to finalizing application client adapters. **Consequence:** Phase 0 remains minimal, free of unnecessary native dependencies and unverified client assumptions. **Status:** accepted.

## ADR-016 — Stream Key Authority and Aggregate ID Inconsistency Prevention

**Decision:** In integration event envelopes (`IntegrationEventEnvelopeV1`), `aggregateId` represents the single authoritative identity of the stream (`CustomerPurchaseStream`) and the Kafka record partition key (`customerId`). While `payload.customerId` exists in the v1 envelope JSON for consumer schema compatibility with `docs/04-contracts-and-apis.md`, having two fields creates an inconsistency risk if callers construct envelopes manually or consumers read from different sources. To prevent data divergence:
1. Event creation in application code and fixtures must use single-source-of-truth factory methods (`createPurchaseCompletedEnvelope`, `createPurchaseInvalidatedEnvelope`) taking `customerId` once and synchronizing both fields by construction.
2. Fixture override merges must never allow shallow spread operations to overwrite `aggregateId` or `payload.customerId` independently.
3. Kafka partition key routing must strictly extract `envelope.aggregateId` via `getEventPartitionKey(envelope)`.
4. Runtime deserialization must reject any envelope where `aggregateId !== payload.customerId`.
**Consequence:** Eliminates dual-source-of-truth drift, partition-key routing mismatches, and brittle manual object construction. **Status:** accepted.



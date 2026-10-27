# 00 — Goals, Scope and System Acceptance

## Problem framing

We need a small, credible event workload that allows rigorous investigation of **Kafka mechanics**: producing records, partition-local ordering, independent consumer groups, offset/lag, rebalancing, at-least-once delivery, replay, and broker-side replication. The purchase/loyalty example is a **fixture**, not the product.

## Intended audience

Students or reviewers evaluating Kafka's distributed log, failure behavior, cluster configuration, and trade-offs. The dashboard should make Kafka internals visible without requiring knowledge of the reward business domain.

## Simulated business rules (frozen for the demo)

- Every synthetic purchase is assumed completed; no payment initiation/settlement.
- `customerId`, `purchaseId`, integer `amountCents`, `customerSequence`, `eventId`.
- Bronze < $100 accumulated eligible spend, Silver $100–<$300, Gold >= $300.
- Base points = `floor(amountCents / 1000)`; multiplier determined by tier **before** purchase: 1, 2, 3.
- Example: customer U001 purchasing $120, $200, $100 earns 12 + 40 + 30 = 82 points; invalidating first purchase yields 20 + 20 = 40 for retained purchases.
- No redemption, coupons, shipping, tax, returns, chargebacks beyond a synthetic invalidation marker, or financial ledger.

Only implement this business behavior to verify **ordering**, **deduplication** and **replay**. Do not expand it without an explicit project change.

## Functional demonstrations

1. A real Kafka broker stores records with visible topic/key/partition/offset.
2. Records of one customer stay in one partition and consume in publish order.
3. Loyalty and Analytics **different groups** read the same data with independent offsets.
4. Pause/stop one group, generate more events, observe backlog and catch-up.
5. Start/stop multiple workers **within one group**, observe assigned partitions and rebalance.
6. Crash a worker after its DB commit but before offset commit; verify redelivery without duplicate effects.
7. Start independent replay from earliest **retained** offset; recalculate demo output in isolation.
8. Final cluster: three brokers, six partitions, RF=3, ISR and elected leaders; fail one broker and observe leader promotion/recovery.
9. Produce from a load generator and chart actual throughput, processing latency and lag.

## Nonfunctional goals

- All critical scenarios repeatable via scripts, tests or documented operator steps.
- Domain logic deterministic and unit-tested.
- Clear command/query, transport/domain and application/infrastructure separation.
- Failures surfaced explicitly. Consumers must not silently acknowledge unsuccessful writes.
- Works on a developer machine with Docker Compose and no paid services.
- No implied proof of multi-machine/high-availability performance when all containers share one host.
- Fair cross-broker throughput and latency evaluation must disclose unequal delivery guarantees, persistence/replication, topology, worker count and workload settings.
- No fake Kafka metrics. Illustrative data on wireframes must be marked as illustrative.

## Out of scope

A production e-commerce product; user accounts/authentication service; payment gateway; stock management; ordering checkout workflow; email/SMS; rewards redemption; general-purpose event sourcing; state snapshots; Kafka Streams topology; CDC/Debezium; distributed transactions between Kafka and Postgres; Kubernetes; hosted cloud resources.

## Measurable success criteria

| Area | Evidence |
|---|---|
| Core pipeline | Record exists in Kafka UI/CLI; consumer logs identical event ID/key, broker-supplied partition and offset. |
| Ordering | Per-customer `customerSequence` progresses 1,2,3 in a single partition; controlled counterexample documents ordering limits. |
| Group independence | Pause Analytics while Loyalty advances; real committed offsets and lag differ. |
| Deduplication | Forced post-DB/pre-offset crash leads to >=2 delivery attempts but exactly 1 business mutation. |
| Replay | Replay group reads from a recorded earliest offset, outputs correct 82→40 isolated recomputation. |
| Scaling | Real group membership and partition ownership change as worker instances increase; report measured, not assumed, speedup. |
| Replication | Metadata proves RF=3, ISR membership and leader failover after one broker exits. |
| Quality | Lint, typecheck, unit/integration tests, docs, graceful shutdown and repeatable Compose scripts pass. |

## Evaluation priority

Technical correctness > repeatability of live demonstration > observability > architectural maintainability > visual styling > business complexity.

## Additional objective: evidence-based Kafka comparison

The instructor requires an observable Kafka throughput/latency demo and an extensible comparison with **Redis Streams** and **RabbitMQ Quorum Queues**. This is **not** a rewrite of the project into a generic broker-agnostic application. Build a separate `Streaming Benchmark` bounded context sharing exact payloads, metric definitions, output schema, and workload generation across transport-specific adapters. RabbitMQ Streams/Super Streams is optional and must be labeled as a different broker mode. The project must show rate, p50/p95/p99, backlog/lag, retries, duplicate attempts, resource usage, and the experiment's declared durability/topology. Source code remains maintainable under adding an adapter without modifying core domain or benchmark formulas. See docs 13–17.

A benchmark chart is not a success criterion unless it is based on raw trial results, reproducible manifest, and at least 3 successful equivalent-condition runs. Do not state a universal broker ranking.

# 13 — Extensible Messaging Architecture and Broker Adapters

**Decision:** Apache Kafka remains the main system and subject of the seminar. Redis Streams and RabbitMQ are **comparison implementations** built later. Keep the production Kafka application Kafka-aware; share the workload, measurements, and portable contracts where that is valid. Never erase different brokers' semantics behind a misleading universal API.

## Architectural boundary: two related but distinct paths

1. **Kafka application path:** Purchase API → PostgreSQL/outbox → Kafka → Loyalty/Analytics. Uses domain/application ports and Kafka-specific delivery metadata in inbound adapters. Kafka infrastructure never enters the domain model.
2. **Transport comparison path:** deterministic benchmark producer → `BenchmarkTransport` contract → Kafka / Redis Streams / RabbitMQ Quorum Queue implementations → benchmark consumers → durable or no-op handler, depending on scenario. This is an isolated **Streaming Benchmark** context, not three parallel versions of the loyalty domain.

Comparisons must never rewrite domain handlers, persistence code or reporting logic for each broker. They may require broker-specific topology setup, concurrency and acknowledgment mapping. Those differences must be explicitly documented.

## Target package structure (create only when needed)

```text
apps/
  producer-api/                # HTTP inbound adapter; Purchase commands/queries
  loyalty-worker/              # Kafka inbound adapter -> ApplyPurchaseEventCommand
  analytics-worker/            # Independent Kafka group
  outbox-publisher/            # Postgres outbox -> Kafka infrastructure adapter
  replay-worker/               # Kafka replay only; isolated staging
  monitoring-api/              # Broker metadata and app telemetry read side
  dashboard/                   # React metrics, traces and benchmark comparison
  load-generator/              # Reproducible synthetic workload generator
  benchmark-runner/            # NEW: isolated orchestrator, no NestJS required
packages/
  contracts/                   # Schema-versioned integration event envelope
  benchmark-core/              # NEW: neutral workload, ports, metrics, manifests
  broker-kafka/                # NEW: benchmark adapter only, distinct from app Kafka adapter
  broker-rabbitmq-quorum/      # NEW: RabbitMQ queue adapter
  broker-rabbitmq-stream/      # OPTIONAL: native RabbitMQ Streams/Super Streams adapter
  broker-redis-streams/        # NEW: Redis Streams adapter
  observability/              # Metric names/types; no business domain types
  test-fixtures/               # Deterministic event corpus and validation
infra/
  compose/{core,replicated,comparison}.yml
  benchmark/{profiles,results}/
```

Do **not** create all the packages in Phase 0. In Phase 8 introduce a **minimal benchmark-specific transport boundary with one Kafka implementation**; in Phase 9 extend it to Redis Streams and RabbitMQ. Do not scaffold competitor packages before Phase 9. Only `packages/contracts` should be shared between app bounded contexts; `benchmark-core` is its own context.

## Ports: smallest honest common interface

```ts
export type BenchmarkTransportKind =
  | 'kafka'
  | 'redis-streams'
  | 'rabbitmq-quorum'
  | 'rabbitmq-stream';

export interface BenchmarkRecord {
  readonly runId: string;
  readonly eventId: string;
  readonly key: string;             // customer ID; routing/sharding hint
  readonly payload: Uint8Array;    // exact serialized byte buffer
}

export interface PublishReceipt {
  readonly eventId: string;
  readonly confirmedAtMonotonicNs: bigint;
  readonly nativePosition?: string; // offset / stream ID / none
}

export interface BrokerDelivery {
  readonly eventId: string;
  readonly key: string;
  readonly payload: Uint8Array;
  readonly attemptMetadata?: string;
  readonly nativePosition?: string;
  acknowledge(): Promise<void>;   // adapter-specific offset/ack semantics
  // failure/retry behavior belongs in adapter; no magical generic nack
}

export interface BenchmarkPublisher {
  connect(): Promise<void>;
  publish(record: BenchmarkRecord): Promise<PublishReceipt>;
  drain(): Promise<void>;
  close(): Promise<void>;
}

export interface BenchmarkSubscriber {
  connect(): Promise<void>;
  start(onMessage: (delivery: BrokerDelivery) => Promise<void>): Promise<void>;
  stop(): Promise<void>;
  close(): Promise<void>;
}

export interface BenchmarkTransportFactory {
  readonly kind: BenchmarkTransportKind;
  createPublisher(config: PublisherConfig): BenchmarkPublisher;
  createSubscriber(config: SubscriberConfig): BenchmarkSubscriber;
}
```

These are **illustrative API contracts**: build only the Kafka-used subset in Phase 8, then finalize and extend them when adding another adapter in Phase 9. `PublisherConfig` / `SubscriberConfig` are explicitly validated, scenario-specific types. Pass opaque delivery handles only to benchmark infrastructure, **never to the Loyalty domain**. No domain repository, DTO or CQRS command contains `kafkajs`, `amqplib`, Redis library types, offsets or delivery tags. Prefer `Uint8Array` to force exact-byte comparable payloads.

### Normalizing without hiding differences

- The common runner asks for a **broker-confirmed publish** and an **application-acknowledged consume**, but must attach a **durability profile** recording what “confirmed” means for that broker. Never claim equivalent disk/replica durability from a common Promise resolution.
- For Kafka, `acknowledge` means commit processed offset **after** successful handler/database commit, per partition. For RabbitMQ Quorum Queue it means `basic.ack` after handler completion. For Redis Streams it is `XACK`; pending recovery requires `XAUTOCLAIM`/equivalent. RabbitMQ Streams use offset tracking/consumer offsets, not queue ACK behavior.
- Kafka rebalances **partition ownership**; RabbitMQ Quorum Queue consumers compete for a queue with prefetch; Redis Streams consumer group workers distribute delivered entries and maintain pending entry lists; RabbitMQ Super Streams can partition streams. No universal `rebalance()` method.
- Replay capabilities are **mode-dependent**. Kafka, Redis Streams and RabbitMQ Streams have retained-record read paths; RabbitMQ Quorum Queue does not support replay of previously acknowledged messages as a retained-log capability. A feature comparison must say `NOT_APPLICABLE` rather than implement a hidden database substitute.
- Kafka's 6 partitions are not implicitly equivalent to six RabbitMQ consumers on one queue or a single Redis stream. Define and report topology explicitly; use sharded comparison topology only in a separate profile.

## DDD/CQRS/clean-code boundaries

- **Purchase bounded context** owns request validation, synthetic purchase creation, sequencing, and outbox data. Its application command can depend on `PurchaseRepositoryPort` and `UnitOfWorkPort`; no broker-specific APIs.
- **Loyalty bounded context** owns pure rewards policy and an `ApplyPurchaseEventCommandHandler`. Broker adapters map validated envelopes to commands. Queries use their own query handlers/read-model adapters.
- **Benchmark bounded context** owns `RunBenchmarkCommand`, `GetBenchmarkRunQuery`, `WorkloadSpec`, `MeasurementPlan`, `RunManifest`, and result aggregation. It **does not** import Loyalty aggregates and **does not** use a domain-specific repository as an implicit dependency.
- **Broker infrastructure adapters** own connection pools, producer batching, native ACK/offset behavior, retry and topology setup. One adapter per technology and mode; never use `if (broker === ...)` scattered across application handlers.
- **Runner composition root** selects a registered factory via a validated enum. An adapter registry is acceptable; a reflection-based plugin framework is not.
- **Observability adapters** consume metrics from app instrumentation, native broker telemetry and host/container metrics. The domain never emits Prometheus metrics directly.

## Interface segregation and error handling

Keep narrow optional capability interfaces rather than adding methods unsupported by all adapters:

```ts
interface ReplayCapability { replay(spec: ReplaySpec): AsyncIterable<BenchmarkRecord>; }
interface BrokerInspectionCapability { inspect(): Promise<NativeBrokerSnapshot>; }
interface WorkerControlCapability { pause(): Promise<void>; resume(): Promise<void>; }
```

Report unavailable capability as explicit `UNSUPPORTED`; do not fake or emulate one broker's native semantics to force a green comparison. Translate native errors into typed `TransientPublishError`, `UnroutableMessageError`, `UnsupportedCapabilityError`, `PoisonRecordError` **at the adapter edge**, retaining native metadata for diagnostic logs.

## Proof that extension is clean

After Kafka baseline is complete, adding Redis Streams must touch only:

1. `packages/broker-redis-streams/**` and topology configuration;
2. registry/composition root;
3. broker-specific runbook/capability map and tests.

It must **not** change `packages/benchmark-core` workload generation, metric formulas, exported result schema, Purchase/Loyalty domain modules, or Kafka adapter implementations. A regression test compares a generated workload's event IDs, serialized bytes and result schema across adapters.

## Rejected approaches

- A shared `GenericEventBus` that assumes ordering, replay, deduplication or transactional processing are identical everywhere.
- Replacing all Kafka infrastructure in the main demo with Redis just to make a chart.
- Duplicating loyalty calculation, load generator or measurement code for each broker.
- Measuring React/UI/WebSocket round trips as broker-only latency.
- Adding RabbitMQ Classic Queue and Redis Pub/Sub as if they were the same retained-log class as Kafka.

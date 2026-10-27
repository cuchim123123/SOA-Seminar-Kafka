# 15 — Kafka vs Redis Streams vs RabbitMQ: Honest Feature and Performance Comparison

## The exact comparison products

1. **Apache Kafka**, topic partitions and independently committed consumer groups.
2. **Redis Streams**, not Redis Pub/Sub: `XADD`, `XREADGROUP`, `XACK`, `XPENDING`/`XAUTOCLAIM`, `XRANGE`, and retention/trim policies.
3. **RabbitMQ Quorum Queues** using durable queues, publisher confirms and manual consumer acknowledgments. One exchange fans out to two distinct queues when comparing independent processing roles.
4. **RabbitMQ Streams / Super Streams** as a **separate optional adapter** (preferred for a fair *retained-log* comparison with Kafka). Native stream clients and offset-based reads are different from AMQP Quorum Queues.

Redis Pub/Sub is a non-durable broadcast API and RabbitMQ Classic Queues have different trade-offs; they may be shown in an explanatory feature matrix but **are not the baseline** for quantitative retained streaming comparison.

## Capability matrix: do not conceal semantics

| Dimension | Kafka | Redis Streams | RabbitMQ Quorum Queue | RabbitMQ Streams / Super Streams |
|---|---|---|---|---|
| Append/read model | Partitioned retained log | Append-only stream ID entries | Replicated queue; acked messages removed from usable queue | Retained stream log; super streams partitioned |
| Multiple independent processors | Separate groups | Separate stream groups | Separate queues bound to exchange | Separate stream consumers / offsets |
| Work sharing | One member owns each partition inside a group | Group deliveries distributed; pending ownership/claim | Competing consumers + prefetch/ACK | Stream consumer strategies, Super Stream partitions |
| Per-key sequence/ordering | Partition-local on stable key | Stream insertion order, but group parallelism needs app key discipline | Queue delivery order is not equal to ordered completion across consumers | Stream offset order / partition-local for super streams |
| After-ack replay from retained source | Yes, subject to retention | Yes, subject to trimming and data availability | **No, not the same capability** | Yes, subject to retention |
| Consumer progress marker | Committed partition offset | Group pending entries + last-delivered ID; ack | Queue delivery tag/ack and queue depth; not a persisted replay cursor | Offset/store offset (native stream protocol) |
| Replication/durability | RF/ISR/acks and broker configuration | AOF/RDB/replica/cluster configuration varies | Quorum replicas + publisher confirms | Stream replication/retention configuration |
| Simple scaling equivalence | 6 partitions constrain group parallelism | Sharding required for comparable per-key partition ownership | Can parallelize competing consumers, but ordering guarantees differ | Super Streams support partition-like layout |

**Do not label Quorum Queue “cannot replay” without saying “acked messages”; an unacknowledged/pending message can be redelivered.** Likewise, do not falsely say Redis Streams cannot replay or RabbitMQ has no stream log. Acks, persisted positions, and retention behave differently across products.

## Topology profiles for meaningful comparisons

### Profile C1 — single-node throughput baseline

- Kafka: 1 broker, RF1, bounded partitions.
- Redis Streams: 1 Redis server, explicit persistence policy.
- RabbitMQ Quorum Queue: 1 RabbitMQ node, clearly one-member quorum queue (limited node fault tolerance).
- RabbitMQ Stream: optional single-node stream.

**Use:** basic client/broker throughput and API latency; **do not call durability or HA guarantees equal**.

### Profile C2 — strongly durable/replicated (advanced)

- Kafka: 3 brokers, RF3, minISR2, producer `acks=all`.
- RabbitMQ Quorum: 3 nodes/members, durable queue, publisher confirms, manual consumer ACK.
- RabbitMQ Stream: declare replication and stream offset settings, if implemented.
- Redis: explicitly chosen AOF policy and replication/cluster configuration; explain that write confirmation versus disk durability differs from Kafka and RabbitMQ quorum semantics.

**Use:** cost/performance under **declared**, not necessarily equivalent, failure guarantees. No invented equivalence or rankings.

### Profile C3 — fan-out

- Kafka: 2 independent groups each see every published event.
- Redis Streams: 2 independent groups each see stream entries.
- RabbitMQ Quorum: a durable exchange routes each publish into 2 durable queues, each with its own consumers.
- RabbitMQ Streams: 2 independent stream reader positions.

Measure throughput and **duplicated delivery work** honestly. Do not equate two Rabbit queues with one Kafka partition or omit the additional queue resource cost.

### Profile C4 — per-customer ordered scaling (advanced)

- Kafka: fixed number of partitions, key=`customerId`.
- Redis: explicitly sharded streams with deterministic customer routing (or mark unordered worker processing), one owner strategy per shard.
- RabbitMQ Quorum: sharded/routed queues by customer to preserve per-key sequence (separate topology). One queue with multiple consumers is **not** automatically equivalent.
- RabbitMQ Super Stream: partition routing by key, proper consumer configuration.

Record topology implementation cost, not just performance. Never use all transports' default one-queue consumer modes then claim they have equivalent ordering.

## What to demonstrate side by side

| Experiment | Primary takeaway |
|---|---|
| Small messages and bounded concurrency | Protocol and broker overhead at a documented durability setting |
| Increasing payload bytes | Serialization/network/disk sensitivity |
| 1→3→6 consuming workers | Distinguish Kafka partition ownership from RabbitMQ/Redis work sharing |
| Pause independent analytics group | Compare backlog and isolation model |
| Crash before acknowledge/commit | Compare retries, redelivery, pending recovery and idempotency needs |
| Intentional replay of already-acknowledged history | Show support in Kafka, Redis Streams, RabbitMQ Streams; Quorum Queue N/A |
| Kill leader / node | Only under equivalent **documented** replicated topology; report explicit availability boundary |

## Adapter-specific correctness checklist

### Kafka
- Stable key/partitioner for per-customer order; immutable partition count during workload.
- Offset N+1 after handler; check consumer commit behavior under failure.
- Topic retention covers trial; capture log start/end and group offsets.
- Record RF, ISR and `acks` in manifest.

### Redis Streams
- Explicit `XGROUP CREATE` starting ID policy (`0` for retained history or `$` for new arrivals; validate the chosen semantics).
- `XREADGROUP` with bounded `COUNT` and `BLOCK`; ACK via `XACK` after handler.
- Recover pending messages after consumer failure via `XAUTOCLAIM` (or compatible method in pinned version) or demonstrate the missing recovery intentionally.
- Capture `XPENDING`, stream length, last-delivered ID and trim policy; do not present pending count as equivalent to Kafka lag.
- Include explicit AOF/RDB/replication settings and memory cap/trim policy in run manifest.

### RabbitMQ Quorum Queues
- Explicit durable exchange/queue declarations and routing bindings; publisher confirm channel.
- Manual `basic.ack` after handler; bounded prefetch and consumer concurrency; ensure rejects/requeues are visible.
- Independent processor roles require independent durable queues with routing fan-out.
- Previously acknowledged queue messages are not a retained replay log; show N/A in replay table.
- Record quorum size, member/node placement and confirm settings.

### RabbitMQ Streams (optional)
- Use a native Streams client, stream retention and offset tracking; do not implement Quorum ACK API semantics as a Streams facade.
- Use Super Streams to compare partitioned log parallelism; record partition count and routing.
- Document native producer confirmation and stream offset recovery model.

## Presenting results responsibly

The comparison is **scenario-based**, not a universal broker leaderboard. Each graph must show broker mode, acknowledgment/durability setting, cluster topology, payload size, concurrency, consumer count and environment. A transport may lead in one profile and lose in another. Prefer explaining *why* a measured difference occurred to claiming an inherent product superiority.

## Official primary references (verify pinned implementation versions)

- Kafka monitoring and JMX client metrics: https://kafka.apache.org/documentation/#monitoring
- KafkaJS producer/consumer semantics: https://kafka.js.org/docs/producing and https://kafka.js.org/docs/consuming
- Redis Streams: https://redis.io/docs/latest/develop/data-types/streams/
- Redis stream groups: https://redis.io/docs/latest/commands/xreadgroup/
- RabbitMQ Streams / Super Streams: https://www.rabbitmq.com/docs/streams
- RabbitMQ consumer ACKs and publisher confirms: https://www.rabbitmq.com/docs/confirms
- RabbitMQ Quorum Queues: https://www.rabbitmq.com/docs/quorum-queues

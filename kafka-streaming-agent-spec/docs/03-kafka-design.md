# 03 — Kafka Design, Guarantees and Configuration

## Kafka is the subject of the seminar

An event stream is a Kafka topic containing records. The broker stores records in append-only partition logs subject to retention, regardless of whether a consumer has already read them. A record belongs to exactly one partition of its topic; its offset is unique **only inside that partition**.

### Fixed topic contract

| Setting | Phase 1 | Phases 2–6 | Phase 7+ final |
|---|---|---|---|
| Topic | `purchase.events` | `purchase.events` (fresh demo dataset) | `purchase.events` (fresh replicated cluster) |
| Partitions | 1 | 6 | 6 |
| Brokers | 1 | 1 | 3 |
| Replication factor | 1 | 1 | 3 |
| Minimum ISR | 1 | 1 | 2 |
| Record key | `customerId` | `customerId` | `customerId` |
| Producer acks | `all` supported, but RF1 gives no replication | `all` | `all` |
| Group: Loyalty | `loyalty-v1` | `loyalty-v1` | `loyalty-v1` |
| Group: Analytics | — | `analytics-v1` from Phase 3 | `analytics-v1` |
| Group: Replay | — | `replay-<runId>` from Phase 5 | `replay-<runId>` |

Phase 1 uses one partition to make the first pipeline easy. **Before Phase 2, recreate the demo topic on an empty disposable dataset** with six partitions. Increasing partition count in place can change the key-to-partition mapping and break assumptions for existing ordered customer histories. Make deletion/reset explicitly destructive.

### Partition and ordering

- Select key from **customer ID**, never purchase ID. All events for a customer should be routed consistently to one partition.
- Customers are not necessarily unique per partition; many customer IDs share a partition.
- Kafka preserves append/consumption order **per partition**, not across partitions/topics, and not business-time order before publication.
- Assign per-customer sequence under a PostgreSQL row lock (Phase 4) and publish outbox entries in that order using **one** sequential publisher initially.
- A raw `ORDER BY created_at` does not enforce business order under concurrent transactions. Prefer explicit sequence and durable event identity.
- Consumer rejects/defer sequence gaps; never commit a record whose prerequisite update is missing. In Phase 1–3, restrict generator to ordered single-producer creation for a given customer and document its weaker guarantee.
- Running multiple consumers within one group divides **partitions**, not customers individually. Processing across partitions may overlap while processing *within* a partition must stay sequential for stateful Loyalty.
- Avoid side-effectful unawaited work inside `eachMessage`. If introducing per-partition concurrency in advanced optimizations, prove ordering remains valid.

### Producer configuration and acknowledgments

- Pin KafkaJS version, disable silent topic auto-creation after topic-bootstrap scripts exist.
- `acks=-1` / `acks=all` requires acknowledgments from current ISR members; `min.insync.replicas` is a threshold, **not** a count of acknowledgments when ISR has more members.
- `acks=1` only requires leader acknowledgment; `acks=0` does not wait for broker acknowledgment. Compare reliability trade-offs, not absolute guarantees.
- KafkaJS documents its idempotent producer option as experimental in some versions. Verify behavior against the installed release before enabling and claiming its guarantee. Idempotent producer deduplicates eligible producer retries at the broker, **not** repeated API requests or DB writes.
- Phase 1 direct producer: send and await Kafka acknowledgment, then return accepted response. Phase 4+: commit purchase+outbox and return; publisher sends asynchronously, can retry and can duplicate after an ambiguous ack.
- Stable event ID and bounded retries with exponential backoff/jitter for retryable failures. No retry loops that swallow an unrecoverable schema error.

### Consumer groups

- `loyalty-v1` and `analytics-v1` read independently. Pausing Analytics does **not** block Loyalty or remove retained records.
- One consumer instance per partition **at most for active ownership** in a given group, though a consumer may own many partitions. With 6 partitions, at most 6 consumers will actively own partitions in that group.
- Group join/leave triggers rebalance and reassignments; show broker/consumer-reported assignment, not an assumed modulo diagram.
- Pausing assigned partitions is not equivalent to killing a process; a paused but healthy group member remains a member. Killing a member causes rebalance after failure detection or a graceful departure.

### Offsets and lag

- A consumed Kafka message offset `N` is committed as **`N + 1`** after successful processing.
- Offsets are per **group, topic, partition**. The client must never add offsets across partitions as if they were one sequence.
- `lag = logEndOffset - committedNextOffset` (per partition). If group offset is uninitialized, show `UNCOMMITTED` or derive against earliest available offset, **not automatically zero**.
- Distinguish `logStartOffset` (earliest retained) from `0`. Deletion/retention can advance start offsets.
- Lag measured in records is not identical to elapsed latency or number of successfully committed DB writes.
- Use broker-reported offsets and actual client metrics; avoid in-memory guessed offsets as source of truth.

### Manual commit strategy

- Phase 1–3 can log without DB. Set desired commit strategy deliberately and document semantics; Phase 3 demonstrates manual offset commits.
- Phase 4 onward use KafkaJS `autoCommit: false`. For each record: validate → execute transactional command → commit DB → `consumer.commitOffsets([{topic, partition, offset: String(BigInt(message.offset) + 1n)}])`.
- Use `BigInt` or other safe integer treatment for Kafka offsets; they are strings and may exceed JS safe integer range.
- Do not commit on a transient failure or sequence gap. Retry a bounded number of times, then **pause that partition** or stop worker with a visible error; do not advance past a poisoned event silently.
- A permanently invalid message needs a documented operator decision (quarantine/DLQ or manual correction). A DLQ alone may violate ordering if the problematic record is skipped. Do not make automatic DLQ-skip the default for Loyalty.
- Consider heartbeat/session timeout for slow processing; KafkaJS `eachMessage` requires avoiding long blocking operations.

### Retention, catch-up, replay: different concepts

- **Catch-up:** resume normal group consumption from the latest committed offset after a pause/failure.
- **Replay:** deliberately read older retained records through new group ID or seek/reset, potentially to recompute results.
- **Retention:** broker policy that may remove old records, including already committed/processed records. `retention.ms = -1` disables time-based deletion; **size-based deletion and disk capacity still matter**.
- Phase 5 replay group uses a unique group ID per run and `fromBeginning: true` only when there is no prior committed offset. The phrase `fromBeginning` means **earliest retained record**, not necessarily offset zero.
- Replay uses isolated staging and no outward business side effects. For the controlled invalidation demo see `05-persistence-and-reliability.md`.

### Replication and ISR

- Final profile: 3 brokers/controllers, six partitions, topic RF=3, `min.insync.replicas=2`, producer `acks=all`, unclean leader election disabled.
- Leader handles reads/writes of the partition; follower replicas replicate from leader. ISR are replicas currently eligible as in-sync per broker rules.
- Acknowledgment from `all` waits on ISR, and enough ISR must exist to meet minimum. If one broker is down and 2 remain synchronized, writes can continue; with only 1 ISR, `acks=all` writes fail due to minISR, even if a leader remains.
- KRaft controller quorum in a 3-voter combined cluster also requires a majority. Losing 2 nodes loses quorum.
- A 3-container cluster on one machine tests process/broker failure, **not** whole-host availability.

## KafkaJS reference API guardrails

- Review `https://kafka.js.org/docs/producing`, `/docs/consuming`, `/docs/admin` against installed version.
- Topic records and event metadata are owned by Kafka; do not invent offset values or infer partition by `customerId % partitionCount` in UI.
- Any use of KafkaJS transactions/producer idempotence must include an explicit test proving behavior with the chosen version; it is optional, not required for DB exactly-once processing.
- Close all consumers/producers/admin clients on shutdown; rebalances and retries should be observable.

## Kafka throughput instrumentation and cross-broker comparison constraints

Kafka throughput is not a single KPI: expose attempted publishing, **broker-confirmed** unique records/sec, consumed deliveries/sec, unique DB-applied and offset-committed/sec, and payload MiB/sec **separately**. Record producer ACK duration and consume-to-commit duration p50/p95/p99, lag curves, partition skew, rebalance windows and under-replicated/ISR status. Measure Kafka-only transport processing without NestJS+PostgreSQL in the benchmark's measured hot path. Retain broker-specific metrics in Kafka panels even when a shared comparison report exists. See `14-benchmark-methodology-and-metrics.md` and `15-kafka-redis-rabbitmq-comparison.md`.

No Kafka API claim carries over to Redis Streams or RabbitMQ Quorum Queues. Redis pending IDs are not Kafka offsets; RabbitMQ Quorum manual ACK does not provide replay of already-acked records; RabbitMQ Streams/Super Streams do support retained-log offsets under their own semantics.

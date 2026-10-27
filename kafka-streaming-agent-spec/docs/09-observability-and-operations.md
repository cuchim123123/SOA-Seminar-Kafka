# 09 — Observability, Dashboard, Safe Operations and Local Security

## Goal

Make the state transitions of Kafka **visible**. The UI is a real observability/control panel, not a fake diagram of assumed message movement. Read Kafka state through trusted backend adapters, not from the browser.

## Signals and provenance

| Signal | Source of truth | Display detail |
|---|---|---|
| Broker/partition leadership | Kafka Admin metadata | broker ID, topic, partition, current leader, replica broker IDs, ISR IDs |
| Log start/end offsets | Broker Admin/API or CLI | decimal strings per partition |
| Group members/assignments | Consumer group metadata/consumer instrumentation | group ID, client/worker ID, assigned partitions, rebalance events |
| Group committed offsets | Kafka consumer group offsets | next offset to read, possibly null/uninitialized |
| Consumer lag | Derived from **broker** log end - committed next | numeric string per partition; unknown state if missing |
| Producer throughput | Instrumented producer success/error counters | records/s, bytes/s, retries and error rates |
| Consumer throughput | Worker successful processing counters | applied/s, duplicate skips/s, error/s |
| Processing latency | Correlated event timestamps + monotonic runtime measurement | p50/p95 with timestamp assumptions noted |
| Replay | Replay-run state and Kafka consumption | group ID, bounded start/end/current, output comparison |
| Replication/ISR | Kafka metadata/JMX/CLI | partition leaders, ISR, under-replicated partitions, controller quorum |
| Business output | PostgreSQL read models | points/tier and analytics as **proof fixtures**, not central dashboard content |

Do **not** calculate a displayed partition from guessed key hashing. Kafka partitioner may change; use broker-assigned partition from produce response / consumed record.

## Metrics semantics

- Offset values exposed by KafkaJS are often strings; use `BigInt` internally; serialize as strings for JSON.
- Consumer lag is not guaranteed meaningful before a group has committed a starting offset; show `UNCOMMITTED` or compute with explicit documented baseline.
- `lagRecords=0` is not equivalent to every downstream side effect being complete if the consumer acknowledges too early; this is why manual offset discipline is tested.
- Consumer rebalancing momentarily pauses processing. Display a status and timestamp, not a fabricated smooth throughput line.
- Compare active and idle consumer instances separately.
- `replicationFactor` is a configured assignment count; `ISR` is the set currently in-sync. These are not interchangeable.
- KRaft controller quorum observations may require `kafka-metadata-quorum.sh` or broker JMX; a KafkaJS Admin call does not expose every broker-internal metric.

## Dashboard sections

### Overview / pipeline trace (Phase 1+)

- Producer request and event ID.
- Published topic/key/partition/offset acknowledgment.
- Worker consumed offset and process status.
- Per-stage timestamps, correlation ID, errors.
- Explicitly distinguish `ACKNOWLEDGED_BY_BROKER` from `APPLIED_TO_DATABASE`.

### Partitions & ordering (Phase 2+)

- Partition timeline with real offsets and recent events.
- Key-to-partition distribution, sequence checks, hot partition indicator.
- Do not use partition indices inferred from customer IDs for display.

### Consumer groups & scaling (Phase 3/6)

- Group membership, partition assignments, per-partition log-end and committed-next offset, lag and group rebalances.
- Distinguish independent groups (fan-out) from workers in the same group (parallel consumption).

### Replay and reliability (Phase 4/5)

- Fault injection run ID and point, duplicate delivered vs DB applied count.
- Replay start/end/current offsets, selected group ID, retained-history availability, staging results.
- No automatic side effects from replay.

### Brokers & replicas (Phase 7)

- Three Kafka brokers, leader/follower roles by partition, ISR, under-replicated count, controller quorum, produce success/error and recovery.
- Broker stopped/down state from actual metadata or environment health.

## Operations model

- Use CLI and Docker Compose scripts first. UI controls are optional wrappers around allowlisted local commands.
- Do not mount `/var/run/docker.sock` into the frontend or an unauthenticated public API. If using a control service, only allow predeclared services/IDs, bound to localhost, with explicit `DEMO_CONTROL_ENABLED=true` and no arbitrary command execution.
- A broker kill control is inherently disruptive; require operator confirmation in UI and identify which demo profile/data will be impacted.
- `demo:reset` must log exactly which disposable topic, groups, DB tables, and volumes it will clear, and require an explicit affirmative flag outside tests.
- Dashboard should never write directly to PostgreSQL business tables. Business mutations happen through commands/use cases.
- Keep per-service logs machine-readable, include `service`, `eventId`, `correlationId`, `groupId`, `topic`, `partition`, `offset` where relevant.
- Expose `/health/live` and `/health/ready` independently; readiness should fail if required client/DB is unavailable, liveness should not bounce the whole process for every transient dependency issue.

## Environment setup

- Use `.env.example` with no credentials, and typed schema to validate variables on startup.
- Inject Kafka brokers through `KAFKA_BROKERS` as comma-separated `host:port`; never hardcode `localhost` for intra-container clients.
- Expose Kafka UI and API on localhost by default.
- Persist broker data through named volumes. Use separate volumes for single-node and replicated profiles.
- Generate only synthetic IDs and amounts; no payment information/PII.
- Avoid hardcoded `latest` images for reproducibility; pin known compatible versions when implementation begins.

## Expected demo telemetry sample

```json
{
  "groupId": "loyalty-v1",
  "topic": "purchase.events",
  "partition": 2,
  "logEndOffset": "108",
  "committedNextOffset": "105",
  "lagRecords": "3",
  "status": "RUNNING"
}
```

This is an **example schema**, not live data. In production code, return the actual values and mark unavailable fields accordingly.

## Throughput and cross-broker comparison dashboard (Phase 8–10)

Add distinct views:

- **Kafka Live:** broker-confirmed publishes/sec, delivery attempts/sec, unique DB-applied and offset-committed/sec, payload MiB/sec, producer ACK p50/p95/p99, consume-to-commit p50/p95/p99, client retries/errors, group lag, partitions, leadership/ISR, CPU/RAM/disk/network. Mark absent signals as unavailable.
- **Transport Benchmark:** run selection by profile and broker mode; side-by-side confirmed events/s, unique handled events/s, latency percentiles, error/duplicate/ambiguous count and resource cost. Show `broker=RabbitMQ Quorum Queue` separately from `RabbitMQ Streams`.
- **Reliability / Feature Differences:** native Kafka offset/replay vs Redis Stream ID/pending list vs RabbitMQ Quorum ready/unacked/confirm semantics; do not create a fake unified offset.
- **Results Provenance:** each chart links to run manifest, exact test command, input seed, serialized payload size, broker/client version, hardware, topology/replica/durability, per-run status and timestamp. Never use static sample metrics as real data.

Monitor/benchmark domain/query boundaries: the dashboard reads metrics/report APIs; it does not directly control Redis/RabbitMQ/Kafka clients, compute business effects or spawn arbitrary shell commands. The transport runner collects measurements, persists files and exposes typed query results. See docs 13–16.

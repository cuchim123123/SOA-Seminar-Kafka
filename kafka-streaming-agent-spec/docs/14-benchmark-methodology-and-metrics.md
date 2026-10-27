# 14 — Throughput, Latency, Observability and Benchmark Methodology

**Purpose:** The instructor requires direct observation of throughput, latency and operational behavior, including a defensible Kafka versus Redis/RabbitMQ comparison. All charts must come from executed runs, not simulated metrics. **No benchmark result is included in these specifications.**

## Separate the measurement planes

| Plane | Metric | Definition / evidence |
|---|---|---|
| Producer | attempted messages/s | Number of publish attempts submitted per measured wall-clock second (may include retries) |
| Producer | **broker-confirmed messages/s** | Unique event IDs with successful broker/client confirmation divided by interval; report duplicates and ambiguous confirms separately |
| Producer | payload MiB/s | Actual serialized application payload bytes confirmed / interval / 2^20; label exclusion of framing/replication/network bytes |
| Producer | acknowledgment latency p50/p95/p99 | Monotonic duration from publish API invocation until broker-confirmed completion; no HTTP/API or DB time in transport-only test |
| Consumer | received deliveries/s | Number of delivery callbacks including duplicates and retries / interval |
| Consumer | **unique processed messages/s** | Unique event IDs whose handler finished and delivery was successfully acknowledged/committed / interval; explicitly define processing mode |
| Consumer | delivery-to-ack latency p50/p95/p99 | Monotonic callback arrival to ack/commit completion in same process |
| End-to-end | publish-to-processed latency p50/p95/p99 | Event broker-confirmed to business processing complete; require synchronized clocks or single-host monotonic trace correlation, otherwise label as approximate |
| Backlog | Kafka lag / Rabbit ready+unacked / Redis pending+new | Display **native units and definitions**, do not show as an apples-to-apples number unless carefully normalized |
| Correctness | produced / confirmed / processed / duplicates / errors | Reconcile unique event IDs; report missing/delayed/duplicate side effects, unconfirmed events and timeouts separately |
| Resource | CPU, RSS/memory, disk and network I/O | Record per process/broker/container and host totals with sampling interval |
| Cluster | leader, replicas, ISR, rebalances | Kafka-native details for Kafka tab only; equivalent-specific data for other systems |

Never label `produced == handled`, `latency == queue waiting time`, or `consumer lag == dropped events`. `offset` and Redis stream IDs are NOT globally comparable units. `p99` is not an average of per-worker p99 values: merge histograms/raw observations using a correct quantile method.

## Measurement instrumentation

- Put telemetry at producer and consumer adapter edges: attempt start, transport confirmed, delivery received, handler completed, ACK/commit succeeded, error, retry and duplicate detected.
- Attach `runId`, `eventId`, `correlationId`, `transport`, `mode`, `payloadBytes`, `workerId`, and timestamps. Do not emit one log per event in performance runs unless explicitly testing logging overhead.
- Use monotonic clocks for within-process durations (`process.hrtime.bigint()` in Node), not `Date.now()` alone. A cross-process end-to-end sample requires synchronized wall clocks, a common host clock, or a trace infrastructure that explicitly records skew. A broker's own message timestamp is not proof of synchronized clocks.
- Histogram bucket/quantile library must be tested on known distributions. Expose count, min/max, p50, p95, p99 and overflow / dropped samples. Aggregate histograms, not averages of percentiles.
- Counters are cumulative; rate calculations use deltas between timestamps, including denominator and reset detection. Use a sliding window for dashboard live rates and summarize trial-specific rates separately.
- Read true Kafka committed offsets/log boundaries and replica/ISR metadata where possible. Do not derive metrics by assuming a `customerId % partitions` mapping.
- Use a stable machine-readable `MetricSample` schema. Every unavailable metric has `null` or `UNSUPPORTED` plus a reason; never default to zero silently.

## Benchmark tracks (mandatory separation)

### Track A — Transport-only (primary cross-broker comparison)

`Deterministic workload → BenchmarkPublisher → Broker → BenchmarkSubscriber → no-op/cheap checksum → native acknowledge`

- Run without Purchase HTTP API, outbox, Loyalty DB or React in the timed hot path.
- Consumers validate event identity/schema and compute a tiny deterministic checksum without database side effects.
- One process or bounded separate processes can host the runner; record producer/consumer resource usage separately.
- Measure broker/client protocol and broker behavior, not the cost of NestJS routing or Postgres transactions.
- Compare Kafka, Redis **Streams**, RabbitMQ **Quorum Queues**. RabbitMQ **Streams/Super Streams** are a separate (recommended) comparison mode, not interchangeable with Quorum Queues.

### Track B — End-to-end application (secondary comparison)

`HTTP request → identical Purchase command + PostgreSQL outbox → chosen broker adapter → identical consumer command → identical PostgreSQL state update`

- This track includes DB contention, publisher polling, network, handler execution and outbox delay. Results are **system throughput**, not a broker-only benchmark.
- Only implement after Track A passes and adapter boundaries are stable. If an adapter cannot meet ordering requirements without extra sharding/topology, declare the extra topology and cost.
- Do not use Track B numbers to conclude which broker is intrinsically faster.

### Track C — Kafka-internal sensitivity (Kafka only)

Compare 1/3/6 consumers with fixed partitions; 1 versus 3 brokers; `acks=1` versus `acks=all` (controlled and explicitly labeled), compression/batching, follower lag, and failover windows. This is not a cross-broker fairness claim.

## Workload specification: identical inputs, controlled variables

Start with a *registered* deterministic generator using an integer seed; create stable `runId` and `eventId`. Export and optionally checksum a corpus so Kafka/Redis/RabbitMQ receive identical serialized UTF-8 bytes. Exclude broker-specific headers from canonical body, but report added wire overhead and topology differences.

Suggested matrix (not all combinations initially):

| Dimension | Values | Why |
|---|---|---|
| Application payload | 256 B, 1 KiB, 4 KiB | Size sensitivity; exact serialized length validated |
| Total unique events | 10k, 50k, 100k | Repeatability, sufficient steady-state samples |
| Publisher concurrency | 1, 8, 32 | Client pipeline sensitivity |
| Consumer count | 1, 3, 6 | Scale behavior, with topology constraints documented |
| Key cardinality | 1, 100, 10,000 synthetic customers | Ordering/hot-key/skew demonstration |
| Traffic mode | fixed/open-loop rate and saturation/closed-loop | Distinguish loss of capacity from client backpressure |
| Consumer work | no-op and fixed deterministic CPU work | Separate broker from processing bottlenecks |
| Durability | broker-native default vs explicitly documented durable profile | Never silently compare differing failure guarantees |

Use a **bounded queue / bounded in-flight publish count**. On capacity pressure, record dropped/rejected submissions and timeouts; do not accumulate unlimited promises in memory. Start with one default matrix cell, then add dimensions gradually.

## Fairness and run protocol

1. Record machine CPU/RAM/OS/storage, container resource limits, runtime/library/broker image versions, broker/cluster topology, replication, persistence and retention, queue prefetch/batches, compression and ACK confirmation policy.
2. Run only **one broker system at a time** with equivalent available host resources. Do not run Kafka, RabbitMQ and Redis concurrently during timed comparisons unless explicitly testing contention.
3. Before each run, prepare disposable fresh topics/queues/streams and unique consumers/groups; no stale backlog from earlier runs. Label cold versus warmed cache tests.
4. Warm up clients for a fixed duration or fixed messages; exclude warm-up metrics from the steady-state interval. Do not silently exclude initial startup from published numbers.
5. Run a fixed offered-load trial with a defined duration, allow drain until a deadline, and separately report steady-state throughput, backlog remaining and total completion time.
6. Repeat each point at least **three** times; preserve every raw run, then report median and spread (min/max or IQR). Randomize/order-alternate broker runs where practical to reduce thermal/cache bias.
7. Validate that all expected event IDs reached confirmed/processed states or list missing/ambiguous IDs, errors and timeouts. Never hide incomplete drains.
8. Export data and generate charts **from the saved results**; use the same histogram/summary logic across brokers. Attach configuration and experimental limitations to every comparison slide.
9. Timeouts, process crashes and topology transitions are **separate fault experiments**; do not mingle them with steady-state throughput unless that is explicitly the scenario.

## Interpretation requirements

- Never declare “Kafka is faster” or “Redis is faster” based on one local chart. State **which exact test/profile** and which constraints were active.
- Compare Kafka 3-broker RF3 `acks=all` to RabbitMQ Quorum Queue RF3 publisher confirms only in a *documented durable profile*, and note Redis persistence/replication does not supply automatically equivalent durability. A single-node baseline is a different comparison category.
- Fan-out mode, hot key mode, consumer saturation mode, and replay mode are distinct benchmarks, not one score.
- Broker/client memory, CPU, disk cost and operational complexity belong in findings, even if raw messages/s is higher.

## Minimum chart set in UI (real data)

1. Publish attempts vs **confirmed messages/sec** per transport and run.
2. Unique processed messages/sec, along with delivered attempts/sec and duplicate counts.
3. p50/p95/p99 producer ACK latency and delivery-to-ACK latency (ms).
4. Backlog/lag over time with *broker-specific metric labels*.
5. Worker count versus throughput/latency/CPU (Kafka scaling scene).
6. Per-broker CPU/RAM/disk/network or `not collected`.
7. Comparison bar plot of median trial results with error range and metadata badge indicating topology/durability profile.

## Acceptance gate

A benchmark scenario is complete only when it produces a timestamped, machine-readable run manifest plus raw samples/histograms, event-count reconciliation, reproducible CLI, repeatable trials, and a dashboard view based on those files. No hardcoded chart series. See `16-comparison-runbook-and-data-contracts.md` and `17-audit-and-definition-of-done.md`.

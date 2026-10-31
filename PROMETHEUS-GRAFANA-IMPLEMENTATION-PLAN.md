# Prometheus and Grafana implementation plan

Date: 2026-11-01. Status: proposed implementation and handoff plan, not implemented.

## 1. Outcome and boundaries

Add reproducible monitoring of the real application now, then extend the same measurement contract to defensible Kafka versus RabbitMQ Quorum Queue versus Redis Streams experiments. Grafana must show measured data and explain what each number means. No simulated topology values enter these dashboards.

This plan complements `APP-RELIABILITY-IMPLEMENTATION-PLAN.md`. Instrumentation does not fix missing events, unsafe replay cutover, or startup configuration. Complete the relevant reliability fixes before certifying benchmark results. Metrics plumbing can be implemented earlier to help diagnose those fixes.

Preserve confirmed requirements: CDC-only outbox publication, Docker PostgreSQL, Kafbat, existing business ordering/idempotency guarantees, and independent bounded contexts. Prometheus scraping and read-only monitoring collection are not an outbox publication mechanism; no polling publisher may be restored. If the user's prohibition on polling is intended to cover monitoring too, clarify that before adding scheduled collectors.

Read `AGENTS.md` and these existing specifications:

- `kafka-streaming-agent-spec/docs/09-observability-and-operations.md`
- `kafka-streaming-agent-spec/docs/13-broker-ports-and-adapters.md`
- `kafka-streaming-agent-spec/docs/14-benchmark-methodology-and-metrics.md`
- `kafka-streaming-agent-spec/docs/15-kafka-redis-rabbitmq-comparison.md`
- `kafka-streaming-agent-spec/docs/16-comparison-runbook-and-data-contracts.md`
- `kafka-streaming-agent-spec/docs/17-audit-and-definition-of-done.md`

Some backlog items are marked complete, but that is not evidence that Prometheus instrumentation exists. At planning time `@lab/observability` provides shared logging/error/Kafka processing facilities and has no Prometheus client dependency. Reuse actual working benchmark code where present; do not implement a second runner from a document alone. Existing methodology mentions publisher polling in its application track; amend that historical wording to the confirmed CDC-only requirement when implementing documentation updates.

## 2. Proposed architecture

```text
Producer / Loyalty / Analytics / Replay -- /metrics ----+
Kafka JVM ---------------------------- JMX exporter ---+
Connect + Debezium JVM ---------------- JMX exporter ---+
Kafka group positions ---------------- read-only collector
PostgreSQL --------------------------- exporter --------+--> Prometheus --> Grafana
Future RabbitMQ ---------------------- native plugin ---+
Future Redis ------------------------- exporter --------+
Benchmark runner --------------------- /metrics --------+
       |
       +--> immutable manifest, exact counts, histograms, time series
                  --> validated comparison report
                  --> optional read-only report API for historical Grafana panels
```

Prometheus stores operational time series. Grafana queries those series. The benchmark runner owns experimental truth: workload identity, lifecycle, exact unique counts, trial windows, histogram artifacts, correctness results, and configuration manifests. An interrupted scrape must not destroy benchmark evidence.

Kafbat remains the Kafka inspection UI. Do not replace it with Grafana or duplicate its record browser. Keep React as the application/demo interface with links to relevant Grafana dashboards.

### Recommended components

- A compatible pinned `prom-client` release for Node/NestJS process-local registries and application metrics. Its upstream project is now documented at [prometheus/client_js](https://github.com/prometheus/client_js).
- Prometheus and Grafana in an optional Compose monitoring overlay, with persistent named volumes.
- Prometheus JMX exporter as an agent in Kafka and Connect JVMs; verify supported image/JVM startup hooks. [JMX exporter](https://github.com/prometheus/jmx_exporter).
- One read-only Kafka group lag collector, either an approved existing exporter or a narrowly scoped collector using the existing admin inspection port. JMX broker metrics alone are not a complete consumer-group lag source.
- A PostgreSQL exporter with a least-privilege monitoring role; bound any custom slot/WAL query cost.
- Future RabbitMQ built-in `rabbitmq_prometheus` plugin, rather than scraping its management UI. [RabbitMQ monitoring](https://www.rabbitmq.com/docs/prometheus).
- Future Redis exporter, verifying selected-stream/group support against the pinned version. Add bounded read-only stream collection only where needed. [Redis exporter](https://github.com/oliver006/redis_exporter).

Pin versions/digests during implementation after compatibility checks. Do not copy unverified latest image tags into the plan or change Kafka/PostgreSQL versions just to install monitoring.

## 3. Measurement contract before instrumentation

Use seconds, bytes, and cumulative counters with `_total`; names below are proposed application-owned metrics, not promises about exporter-native names. Exporter mappings must be tested against actual exposed metrics. [Prometheus instrumentation guidance](https://prometheus.io/docs/practices/instrumentation/).

| Proposed metric | Type | Precise observation boundary |
| --- | --- | --- |
| `lab_source_events_committed_total` | Counter | Outbox event transaction successfully commits, not HTTP handler entry |
| `lab_publish_attempts_total` | Counter | Actual transport publish attempt, including retry attempts |
| `lab_publish_confirmations_total` | Counter | Client observes successful transport completion; not consumer completion |
| `lab_confirmed_unique_events_total` | Counter | Benchmark runner establishes first confirmation for event identity within its run |
| `lab_confirmed_payload_bytes_total` | Counter | Canonical application bytes of those unique confirmed events; excludes framing/replication |
| `lab_deliveries_total` | Counter | Consumer receives a record, including redeliveries |
| `lab_handler_commits_total` | Counter | New business effect transaction commits; duplicates excluded |
| `lab_duplicate_deliveries_total` | Counter | Handler establishes this event was already processed |
| `lab_ack_operations_total` | Counter | Successful offset commit / native ACK operation; may cover several records |
| `lab_processed_unique_events_total` | Counter | Benchmark reconciles unique handler-completed events covered by successful acknowledgement |
| `lab_processing_failures_total` | Counter | Failure classified into a finite reason set |
| `lab_publish_confirmation_duration_seconds` | Histogram | Monotonic publish invocation to confirmation in same process |
| `lab_delivery_to_ack_duration_seconds` | Histogram | Delivery callback to successful acknowledgement observation |
| `lab_handler_duration_seconds` | Histogram | Handler processing duration; distinguish success/failure |
| `lab_source_to_handler_duration_seconds` | Histogram | Explicitly documented source timestamp to handler commit; cross-process clock caveat |
| `lab_http_request_duration_seconds` | Histogram | HTTP request completion by route template/method/status class |
| `lab_consumer_state` | Gauge | Finite state labels reflecting real lifecycle, not just pause flags |
| `lab_replay_runs_total` | Counter | Terminal replay outcomes, without run IDs as labels |
| `lab_replay_duration_seconds` | Histogram | Completed/failed run duration with defined start/end |

Use separate `service` or `consumer_role` labels for loyalty and analytics. One event processed by both is two projection effects, not twice the source throughput. Do not sum those consumers into one cross-broker success score.

In-process counters can miss the interval between a successful DB commit and a process crash. They are operational telemetry, not an exact audit ledger. Reconcile exact correctness from database/event identities and benchmark artifacts. Do not add metric database writes to every business transaction just to make Prometheus exact.

For CDC application mode, the producer does not observe a broker confirmation. Export source commits and Debezium/Connect-native progress separately; do not increment a Kafka confirmation counter when HTTP returns 202. An outbox row remaining in the table does not mean it is unpublished, because CDC does not mark each row published.

### Labels and cardinality

Allowed bounded labels: service, transport, mode, consumer_role, event_type, finite outcome/reason, route template, controlled scenario/profile. Native metrics may include allowlisted topic/queue/stream/group/partition/node dimensions. Relabel dynamic replay groups out of long-lived operational collection unless explicitly needed.

Never use eventId, customerId, purchaseId, correlationId, raw URL, exception text, or arbitrary runId on high-frequency operational metrics. Keep these in artifacts and structured logs. Avoid generating an unbounded Prometheus series set for every benchmark trial.

Proposed run association: an isolated measurement target per active trial, explicit start/end annotations, and complete run identity in artifacts. If concurrent runs must share a target, design a bounded experiment-slot label and isolation rules first. Historical comparison should read immutable reports, not reinterpret a reused slot as a permanent run ID.

## 4. Latency and percentile rules

Use monotonic clocks within one process. Cross-process latency needs documented clock synchronization/error bounds; show approximate or unavailable if that condition is unmet. A field generated before the source transaction is not a database commit timestamp. Name the metric according to the actual boundary.

Consumers may observe an event before the publisher receives its confirmation. Therefore confirmation-to-consumption can be negative or causally misleading across processes. Preserve the existing methodology's intended metric only with a documented definition; prefer publish-start-to-processing for the main end-to-end histogram and record publish confirmation separately. Update the methodology through an explicit reviewed contract change.

Use aggregatable histograms. Evaluate native histogram support across the pinned client, Prometheus, and Grafana versions; use classic histograms with identical explicit buckets if native support is incomplete. Do not average worker p99 values. [Prometheus histogram guidance](https://prometheus.io/docs/practices/histograms/).

Proposed classic bucket ranges, to validate with real distributions:

- Transport duration: sub-millisecond through several seconds.
- HTTP/DB handler duration: milliseconds through tens of seconds.
- CDC/application end-to-end: milliseconds through minutes during outages.

Record sample count and overflow visibility. Quantiles from sparse or overflowing buckets must not be presented as precise measurements. Keep mergeable higher-resolution benchmark histograms in artifacts for final trial reports.

Example queries after the metric contract is implemented:

```promql
sum by (transport, mode) (rate(lab_confirmed_unique_events_total[1m]))

sum by (transport, mode, consumer_role) (
  rate(lab_processed_unique_events_total[1m])
)

sum by (transport) (rate(lab_confirmed_payload_bytes_total[1m])) / 1024 / 1024

histogram_quantile(0.99,
  sum by (le, transport, mode) (
    rate(lab_publish_confirmation_duration_seconds_bucket[1m])
  )
)
```

These are live-window trends, not exact whole-trial results. `rate`/`increase` handle resets and extrapolation, so their integrals are not an exact event audit. Final throughput uses exact reconciled counts divided by the declared steady-state interval; report drain throughput separately. Keep warmup and drain out of the steady-state score.

## 5. Broker-native visibility without false equivalence

| System | Native panels | Interpretation limits |
| --- | --- | --- |
| Kafka | Per-partition/group committed-position lag, broker ingress/egress, request timing, ISR, offline partitions, rebalance activity | Lag is offset distance, not guaranteed count of unprocessed business events; retained log size is not backlog |
| Debezium/Connect | Task health, snapshot state, source event counts, queue capacity, source processing delay, errors | RUNNING is not proof of current end-to-end delivery; native event counts may include different event types |
| PostgreSQL | Transaction/query activity, connections, locks, storage, replication slot retained WAL and progress | WAL retention bytes are not outbox message count |
| RabbitMQ Quorum | Ready messages, unacknowledged deliveries, confirm rate, redelivery, queue/node resources | Publisher confirms and consumer acknowledgements represent different boundaries |
| Redis Streams | Stream length, group lag where available, pending entries, pending age/reclaims, memory, persistence/replication health | Stream length includes retained acknowledged history; pending is delivered-but-unacknowledged, not unread backlog |

Redis group lag can be unavailable for some histories; preserve unknown, never coerce it to zero. [Redis XINFO GROUPS](https://redis.io/docs/latest/commands/xinfo-groups/). RabbitMQ confirms do not prove consumer processing. [RabbitMQ acknowledgements and confirms](https://www.rabbitmq.com/docs/confirms). Debezium exposes connector monitoring through JMX; map the actual pinned connector's metrics. [Debezium PostgreSQL monitoring](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).

Do not create one stacked cross-broker chart called equivalent queue depth. The comparable score is application/benchmark unique event completion under declared guarantees; native backlog explains each system separately.

## 6. Infrastructure and configuration

Proposed file layout, to reconcile with existing directories before creating:

```text
docker-compose.monitoring.yml
docker/monitoring/prometheus/prometheus.yml.template
docker/monitoring/prometheus/rules/application.yml
docker/monitoring/prometheus/rules/benchmark.yml
docker/monitoring/prometheus/tests/
docker/monitoring/grafana/provisioning/datasources/
docker/monitoring/grafana/provisioning/dashboards/
docker/monitoring/grafana/dashboards/
docker/monitoring/jmx/kafka.yml
docker/monitoring/jmx/connect.yml
packages/observability/src/metrics/
scripts/render-monitoring-config.mjs
scripts/verify-monitoring.mjs
```

Provision the datasource and dashboards from version-controlled files with stable UIDs. Treat UI dashboard edits as experiments that must be exported/reviewed before becoming canonical. [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/).

Document and validate image versions, bind addresses, host ports, metrics enablement/path, scrape/evaluation intervals, scrape timeout, retention time/size, Grafana credentials, and target addresses. Suggested local defaults are Grafana host port 3005 to avoid existing application ports, Prometheus 9090, normal scrape interval 15 seconds, and a separate measured benchmark setting around 5 seconds. These are proposals, not hardcoded requirements. Benchmark measurement duration must allow enough scrapes for meaningful live rates.

Prometheus runs in Docker while the applications currently run on Windows. Generate host application scrape targets using the configured Docker Desktop host address, not container localhost. Container exporters use Compose DNS. Support other host platforms explicitly rather than assuming the Windows address is universal. Render Prometheus configuration from validated settings; do not assume a mounted YAML file expands shell variables automatically.

Expose dashboards only on the intended local interface; do not publish raw JMX/RMI unnecessarily. Provide secrets through ignored environment/secret configuration, not committed dashboard JSON. Metrics endpoints should be internal/appropriately restricted and excluded from ordinary HTTP metrics to avoid self-noise. Scrapes must not consume records, publish events, commit offsets, or mutate business state.

Use bounded asynchronous/cached collection for broker/DB queries with timeout and freshness metrics. Avoid unbounded scans and expensive queries on every HTTP scrape. A dependency failure should expose stale/error state instead of blocking all application metrics.

Container CPU/disk metrics describe the Docker Linux VM, not automatically native Windows Node processes. Collect Node process metrics separately; choose and verify a Windows-compatible host collector if host totals are required. Mark unsupported disk/network attribution unavailable. Include monitoring resource use and measure overhead with monitoring enabled versus disabled.

## 7. Dashboard plan

### Dashboard A: Application pipeline

- Source committed events/s, deliveries/s, unique handler commits/s, duplicate/error rate.
- Separate loyalty and analytics progress; actual consumer lifecycle and last progress age.
- HTTP and handler p50/p95/p99, CDC/application latency with clock-quality note.
- Connect task/snapshot status, Kafka group lag by partition, retained WAL.
- Replay outcomes/duration and correction status after reliability fixes.
- Scrape health and stale data prominent; no data is never rendered as healthy zero.

### Dashboard B: Kafka and CDC diagnostics

- Broker throughput/bytes, request latency, ISR and offline partition conditions.
- Group positions/lag, rebalances, consumer processing/commit timing.
- Debezium source progress/queue utilization and PostgreSQL bottlenecks.
- Clear separation of infrastructure bytes from canonical application payload MiB/s.

### Dashboard C: Live benchmark

- Transport/mode/profile/scenario selectors with manifest link and phase annotations.
- Offered rate, attempt rate, unique confirmation rate, unique acknowledged processing rate.
- ACK and delivery-to-ACK quantiles with sample counts; duplicates, errors, unresolved events.
- In-flight work/backpressure, resources, native backlog panels conditional on transport.
- Warmup, measurement, drain and fault windows visibly separated.

### Dashboard D: Historical comparison, later phase

- Median trial throughput and range across at least three successful comparable trials.
- Trial latency distributions; distinguish median of trial p99s from pooled-event p99.
- Resource cost per completed workload and recovery time after faults.
- Compatibility badges: payload hash, durability, fan-out, topology, processing mode, software versions, measurement quality.
- Read-only validated result API or an approved Grafana data-source integration over immutable report files. Prometheus alone is not the historical report store. Decide the integration before adding plugins or importing per-run labels into the operational TSDB.

## 8. Phased tickets and acceptance criteria

### OBS-01 — Freeze telemetry contract and inventory

Map every proposed measurement to an existing adapter/transaction boundary. Identify already implemented runner/ports. Resolve unique-processing versus ACK semantics and latency naming discrepancies in the existing methodology. Define finite labels, cardinality budget, histogram support, and exporter versions. Produce a short ADR and metric dictionary.

Acceptance: no metric depends on fabricated browser values; every count/timer has a specific owner, start/end, duplicate policy, and failure behavior. Architectural choices needing user approval are recorded before dependent code changes.

### OBS-02 — Application metrics MVP

Add process-local registry integration in `@lab/observability`, a small injectable metrics facade, and `/metrics` to producer, loyalty, analytics, and replay. Avoid global registry collisions in tests. Domain entities remain free of Prometheus dependencies. Instrument transaction completion and transport edges; labels must not be accepted directly from arbitrary request input. Add Node process/heap/event-loop metrics supported by the pinned client.

Acceptance: real requests produce expected metrics; failed transactions do not increment committed-effect counters; duplicate events do not increment new-effect counters; creating thousands of customer IDs does not multiply series; telemetry failures cannot alter purchase or offset semantics.

### OBS-03 — Provision Prometheus and Grafana

Implement optional Compose overlay, validated config renderer, persistent storage, stable datasource/dashboard UIDs, and startup/verification commands. Do not silently start additional brokers or modify volumes. Suggested future commands: `monitoring:up`, `monitoring:down`, `monitoring:verify`; mark them unimplemented until actually added.

Acceptance: fresh provisioning needs no dashboard clicks; all four host app targets can be scraped from Docker; restart retains configured history; monitoring down does not stop business processing; secrets are not committed.

### OBS-04 — Kafka, CDC, and PostgreSQL signals

Attach verified JMX agents, implement/pin one group-lag collector, add DB exporter, and map actual metrics into documented recording rules. Apply exporter allowlists and query limits. Verify metrics against Kafka admin results and DB/Connect status.

Acceptance: pausing one consumer grows its lag without changing the other's group; CDC outage is distinguishable from consumer outage; snapshot/streaming state and retained WAL are visible; unsupported values remain unknown. Runtime failure tests require isolated fixtures and must not delete existing data.

### OBS-05 — Operational dashboards and local alerts

Provision dashboards A and B. Add rule tests for target down, persistent processing failure, unexpected lack of progress while workload/backlog exists, connector failure, and WAL growth. Make thresholds configurable and require sustained conditions. Suppress expected pause/warmup states appropriately. No-event idle time alone is not a stalled pipeline.

Acceptance: broken scrape cannot look healthy; expected consumer pause is labeled correctly; each alert links to an actionable runbook. Initial alerts can remain visible in Prometheus/Grafana; outbound email/Slack notification requires separate authorization and configuration. Do not introduce duplicate alert engines unnecessarily.

### OBS-06 — Kafka benchmark instrumentation and evidence

Use an isolated benchmark topic and transport-only runner. Expose metrics throughout warmup/measurement/drain and keep its endpoint available through final scraping, but always persist exact artifacts independently. Correlate Grafana annotations with UTC run windows and manifest identity. Implement bounded open-loop offered load and/or closed-loop saturation with explicit mode labels; record schedule misses instead of hiding client overload.

Acceptance: workload count/hash reconciles; timeouts/ambiguous confirmations are separate; histogram checks use known distributions; no average-of-p99 aggregation; repeated runs reproduce an explained range; instrumentation overhead is measured. Never send malformed benchmark envelopes to the real purchase topic.

### OBS-07 — RabbitMQ and Redis extensions, deferred

Implement benchmark-only adapters behind the existing ports. RabbitMQ uses the documented Quorum Queue mode with publisher confirms and manual ACK. Redis uses Streams groups, XADD/XREADGROUP/XACK and pending recovery; specify AOF/fsync/replication/acknowledgement behavior. XADD success alone must not be advertised as equivalent to replicated durable quorum confirmation.

Reuse the same serialized workload, processing/checksum contract, and measurement definitions. Add native scrape sources and transport-specific panels. Keep RabbitMQ Streams as a separate optional mode. Application end-to-end adaptation is deferred: Debezium's current Kafka path does not automatically become an equivalent RabbitMQ/Redis publisher. Approve a CDC-compatible design first; do not introduce polling or silently put Kafka in only one comparison path.

Acceptance: at least three trials per comparable cell; redelivery recovery tests pass; mismatched durability/fan-out/resource profiles are rejected or clearly separated; native backlog is never mislabeled as directly comparable work count.

### OBS-08 — Historical reports and final validation

Add validated comparison report rendering and the approved Grafana integration. Preserve immutable artifacts and quality flags; exclude failed/incomplete trials from performance rankings without hiding them. Update README, backlog, methodology, runbook, and this plan's status with evidence.

Acceptance: another agent can reproduce dashboards and a Kafka baseline from documented commands; exact reports remain available after Prometheus retention expires; RabbitMQ/Redis panels show unavailable until their adapters actually run.

## 9. Fair-comparison rules

Keep three tracks separate: transport-only, end-to-end application, and Kafka-specific tuning. A direct Redis write must never be compared with Kafka HTTP + PostgreSQL + CDC and labeled broker throughput.

Each manifest records seed and payload hash/bytes, message count, key skew, publisher/consumer concurrency, batching/compression, topology, durability/ack settings, persistence, retention, prefetch/in-flight limits, host/VM/container resources, Node/client/broker/exporter versions, git revision plus dirty-tree indication, warmup/measurement/drain, clock quality, and scrape interval.

Use sequential isolated trials by default, randomized transport order, and fixed resource budgets. Document shared-host contention, cache state, and storage characteristics. Include at least three successful trials per comparison cell; do not claim statistical certainty from three samples. Distinguish capacity under saturation from latency at matched offered load. Define fan-out count and work-queue semantics consistently.

Save exact offered/attempted/confirmed/processed/duplicate/failed/ambiguous/unresolved counts. Kafka batch commits and Rabbit/Redis acknowledgements can cover different numbers of events; compare reconciled event identities rather than raw ACK call counts. Payload throughput excludes protocol and replication bytes and must say so.

## 10. Verification and completion

Run required format/lint/typecheck/unit gates and Kafka verification when active. Add `promtool` configuration/rule checks and rule unit tests; verify dashboard JSON, datasource UIDs, queries, actual metric names, and reset behavior. Validate Compose configuration before launching. Use real delivery evidence for integration checks, not a successful `/metrics` HTTP response alone.

Test: counter reset on process restart, duplicate delivery, failed transaction, failed ACK after successful DB commit, batch ACK, empty/idle workload, unavailable native lag, stale scrape, high label cardinality attempts, sustained overload, short trial, CDC outage/recovery, and monitoring-disabled execution.

Safe first deliverable: OBS-01 through OBS-03 plus a basic application dashboard. Useful Kafka monitoring: through OBS-05. Defensible Kafka performance baseline: through OBS-06 and relevant application reliability fixes. Cross-broker comparison is complete only after OBS-07/08 and equivalent-profile evidence.

No implementation, dependency installation, container startup, destructive action, commit, or push is authorized merely by this plan. Preserve the existing dirty tree and stage only reviewed changes if later asked to commit. Hand off completed ticket IDs, actual commands/results, unresolved decisions, exact environment state, and the next single ticket if work stops early.

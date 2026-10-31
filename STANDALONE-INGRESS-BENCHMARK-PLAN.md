# Standalone ingress benchmark: Kafka, Redis Streams, RabbitMQ Streams

Date: 2026-11-01. Status: analysis and proposed implementation only.

## 1. Binding scope

Build an isolated synthetic publisher benchmark measuring successful broker-acknowledged messages/second, decimal payload MB/second, and publish acknowledgement latency p50/p95/p99.

This is not a Purchase pipeline extension. Do not modify, refactor, import runtime services from, or route requests through producer-api, outbox-publisher, loyalty-worker, analytics-worker, or replay-worker. Do not change their configuration, domain contracts, databases, topics, consumer groups, deployment, or behavior. Do not restore the deleted polling publisher. Preserve the existing UI and its features unchanged.

All three brokers receive independent synthetic records on benchmark-owned destinations. No benchmark consumers, business handlers, consumer groups, XACK, loyalty effects, replay, or delivery-to-consumer latency are required. RabbitMQ means native RabbitMQ Streams, not classic queues, Quorum Queues, or an AMQP queue comparison.

The latest user request supersedes the broader consumer/application comparison scope in earlier plans. Application reliability fixes are separate work, not dependencies for this isolated benchmark.

## 2. Findings from the current checkout

| Existing component | Actual finding | Reuse decision |
| --- | --- | --- |
| `apps/demo-ui/src/components/BenchmarkStudio.tsx` | Runs browser workers against Purchase and raw-publish HTTP APIs; uses random amounts, browser timings, and application fixture keys | Leave unchanged. It is not a standalone publisher or valid source of ingress measurements |
| Same component, final result calculation | Throughput includes completed plus failed requests; batch duration is divided across messages | Do not reuse these measurement formulas; do not fix this unrelated feature as part of this task |
| `apps/producer-api/src/ops/infrastructure/adapters/kafkajs-inspector.adapter.ts` | Contains a KafkaJS raw send, but it belongs to producer-api and is exposed through HTTP | Reuse KafkaJS dependency/version conventions, not this service or endpoint |
| `scripts/generate-ordered-workload.mjs` | Starts/calls application services and generates Purchase workload | Not reusable for the standalone execution path |
| `packages/observability` | Shared logging/errors/Kafka processing; no Prometheus dependency or metrics registry found | Reuse existing pure utilities only if helpful; do not change shared behavior just to support the benchmark |
| Workspace tools | pnpm apps/packages globs, TypeScript, ESLint, Vitest, Node 22 | Reuse directly with one small workspace package |
| Specs 13–16 under `kafka-streaming-agent-spec/docs/` | Define isolated benchmark ports, manifest, reproducible workload, measurements, and artifacts | Reuse concepts and compatible schemas; take only publisher subset, exclude consumer and Quorum Queue requirements |
| Standalone runner/adapter packages | No implemented benchmark-runner, benchmark-core, or competitor adapter packages found | Add one small CLI package; do not scaffold the many packages listed as illustrative targets |
| Prometheus/Grafana | No deployed Compose services, provisioning, client instrumentation, or matching Docker containers found; `PROMETHEUS-GRAFANA-IMPLEMENTATION-PLAN.md` is still a plan | Treat shared monitoring as an integration prerequisite; add one scrape target/dashboard to it when available, never a duplicate stack |
| Docker | Daemon reported 7,984,050,176 bytes, approximately 7.44 GiB, and 16 CPUs; inspected containers were stopped | Plan for a 16 GB ceiling, but preflight the actual allocation; do not assume 16 GB is already assigned |

Read root `AGENTS.md` and applicable rules before implementation. The dirty working tree contains extensive unrelated work; do not reset, broadly stage, or commit it. Source findings are a snapshot and should be rechecked at the relevant ticket, not used to justify an unrelated refactor.

## 3. Recommended minimal architecture

```text
one standalone Node CLI process
  deterministic fixed-size synthetic payloads
  shared bounded scheduler + monotonic timers + result collector
  one selected publisher adapter
     KafkaJS ------------> isolated Kafka benchmark topic
     Redis client -------> isolated Redis stream
     Stream protocol ----> isolated RabbitMQ stream
  /metrics -------------> existing Prometheus -> existing Grafana
  JSON/CSV/histograms ---> immutable trial artifacts
```

Only one transport is active in a measured trial. Use one CLI/package with three publisher adapters and no NestJS server, REST orchestration service, database, plugin framework, or frontend rewrite. A tiny metrics HTTP listener in the same process is sufficient.

Proposed additions:

```text
apps/ingress-benchmark/
  src/main.ts
  src/config.ts
  src/workload.ts
  src/run.ts
  src/publisher.ts
  src/adapters/{kafka,redis-streams,rabbitmq-streams}.ts
  src/metrics.ts
  src/report.ts
  test/
  package.json
  tsconfig.json
  vitest.config.ts
benchmark/ingress/profiles/
benchmark/ingress/results/              # ignored generated artifacts
docker-compose.ingress-benchmark.yml  # isolated Compose project, no app dependencies
docker/ingress-benchmark/              # broker config only as needed
```

Add only root command aliases, necessary test discovery, workspace lockfile entries, benchmark-specific documented configuration, and additive shared monitoring provisioning. Existing application files remain untouched. If monitoring lives outside this checkout, provide the scrape/dashboard artifacts and integration instructions at that boundary; do not claim integration is complete until the actual shared target is verified.

### Small publisher contract

Use connect, publish, drain, and close operations. A record contains a stable synthetic ordinal/identity and an exact byte buffer. A receipt means a confirmed publish, not a queued socket write. Each adapter owns native confirmation correlation and connection setup. Setup/destination ownership may be adapter-specific outside the timed loop; do not create a generic broker administration framework.

Maintain a finite registry keyed by `kafka | redis-streams | rabbitmq-streams`. Configuration is a discriminated union validated once at bootstrap. Do not create subscriber APIs or reuse Purchase event factories: these payloads are a separate synthetic benchmark contract, not business events.

## 4. Isolation and Kafka reuse trade-off

Recommended scored profile: a benchmark-only single-node Kafka container using the existing pinned Kafka image/version and relevant KRaft conventions, alongside independently selected Redis/RabbitMQ profiles. Use a separate Compose project, networks, destinations, ports, and volumes. Start only the selected broker. This reuses existing tooling without changing or sharing application data/runtime settings.

A cheaper optional smoke mode may connect to the existing Kafka broker using a strictly allowlisted benchmark topic. It must not change broker-wide settings or touch application topics/groups, and results must be labeled shared-broker/uncontrolled if application traffic exists. It should not be the default performance baseline because shared broker resources and existing retention settings contaminate comparisons.

The dedicated Kafka container is optional infrastructure isolation, not a new application service. Do not start both Kafka profiles unnecessarily. Leave running application services alone; if their contention invalidates a scored run, report it rather than automatically stopping them.

## 5. Exact metrics and timing

### Acknowledged ingress

For a fixed measurement interval `[t0,t1)`, count the first successful confirmation of each synthetic event observed during that interval:

```text
messages_per_second = confirmed_unique_events_in_window / measured_seconds
payload_MB_per_second = confirmed_unique_payload_bytes_in_window / measured_seconds / 1,000,000
publish_latency = monotonic_confirmation_time - monotonic_publish_invocation_time
```

MB means decimal MB. Optionally add separately labeled MiB/s, but never silently divide by 1,048,576 under an MB/s label. Count only the canonical payload body, excluding Kafka record framing/key, Redis field/ID framing, RabbitMQ protocol framing, network traffic, replication, and persistence overhead. Record these exclusions.

Publish invocation begins before passing the record to the client, so client queue/batch wait is included. Synthetic encoding, connect, authentication, stream/topic creation, and broker readiness are outside the timed hot path. Monotonic timing stays inside the publisher process, so no synchronized cross-process clocks are required.

Warmup stops and drains before the measurement phase. At measurement end, stop scheduling new work and drain outstanding confirmations under a deadline. Late confirmations contribute to final reconciliation and a separately labeled cohort result, not to fixed-window throughput. Save the chosen latency cohort explicitly: primary latency samples for measurement-started records successfully confirmed by the drain deadline, with late-confirmation count and timeout/error counts alongside. Do not compare live rolling p99 with final cohort p99 as if their windows were identical.

Publish latency describes successful publishes. Timed-out/unconfirmed requests must not disappear from the report; show failure and ambiguous counts and mark a trial invalid for clean ranking when correctness criteria fail. Saturation throughput and latency at matched offered load are different experiments.

### Retry/accounting policy

Default scored baseline: no application-level automatic retry, explicit client retry/reconnect policy recorded, and no replay of disconnected/offline buffered commands that could silently create duplicates. Disable client offline queues/retransmission where supported or abort/flag the affected trial when guarantees cannot be established. Count each event at most once on duplicate confirmation callbacks.

A lost connection after a write can mean the broker accepted data without the client seeing confirmation. Classify it ambiguous, not success and not proven loss. Do not add subscribers to settle ambiguity; abort the clean trial and preserve evidence. Native protocol retries/dedup options and IDs belong in the manifest. If an SDK cannot disable internal retries, record them and invalidate faulted scored runs instead of claiming exact persisted uniqueness.

Use a bounded in-flight map and streaming histogram, not an unbounded array of every payload and latency. A per-publisher monotonic publishing-ID scheme can support correlation without a global database.

## 6. Broker adapters and acknowledgement boundaries

| Adapter | Destination | Success boundary | Critical verification |
| --- | --- | --- | --- |
| KafkaJS | Benchmark-only topic, one partition initially | `send` completes successfully with required acknowledgements | Explicit `acks=-1`, RF1/minISR1 baseline; not `acks=0`, not HTTP return |
| Redis client | Benchmark-only stream | Successful `XADD` response for that record | Persist settings recorded; XADD success is not automatically fsync/replication proof |
| Native RabbitMQ Streams client | Benchmark-only stream | Matching server `PublishConfirm` publishing ID | A resolved `send` may only enqueue/write; inspect pinned SDK and wire callback semantics |

Reuse KafkaJS already used by the repository, while creating a publisher entirely inside the new package. For Redis, prefer one maintained Node client with explicit buffering/reconnect control; choose/pin after a small capability check. For RabbitMQ, evaluate `rabbitmq-stream-js-client`, linked by RabbitMQ's Node Streams tutorial; it is a third-party client, not a reason to assume its Promise is a server confirmation. No `amqplib` queue adapter is needed.

Before implementing the common success contract, inspect the selected RabbitMQ SDK source and test confirmation/error handling, publishing-ID mapping, batch behavior, timeout, and close/drain. If the SDK does not expose reliable confirms, that is a capability blocker requiring a better supported client or a scoped adapter solution; do not measure socket enqueue as success.

For RabbitMQ Docker networking, configure stream metadata advertised host/port for the runner's chosen location. Bootstrap connection success is insufficient if metadata points to an unreachable address. Kafka advertised listeners have the same topology consideration. Run all three clients from the same location and network path.

References: [KafkaJS publishing](https://kafka.js.org/docs/producing), [RabbitMQ Node Streams tutorial](https://www.rabbitmq.com/tutorials/tutorial-one-javascript-stream), [Stream protocol](https://github.com/rabbitmq/rabbitmq-server/blob/main/deps/rabbitmq_stream/docs/PROTOCOL.adoc), [Node Streams client](https://github.com/coders51/rabbitmq-stream-js-client).

## 7. Durability: comparable profiles, no false equivalence

There is no single checkbox making the three systems' acknowledgement guarantees identical. Especially, RabbitMQ Streams are not Quorum Queues: Streams write to disk-backed storage but do not explicitly fsync each confirmed write. [RabbitMQ Streams durability](https://www.rabbitmq.com/docs/streams).

Recommended first profile: **single-node, persistence enabled, asynchronous disk flush, no replicated or power-loss durability claim**.

| Broker | Baseline settings | Interpretation |
| --- | --- | --- |
| Kafka | One broker/partition, RF1, minISR1, `acks=all`, persistent volume, normal OS-managed flush | Acknowledged append, no replicated copy and no per-message fsync claim |
| RabbitMQ Streams | One node/stream, persistent volume, publish confirmations | Native confirmed append with single-node limitations; not Quorum Queue semantics |
| Redis Streams | One node, AOF enabled, `appendfsync no`, persistent volume, no eviction | Persistence enabled with OS-managed fsync; no per-XADD durable flush guarantee |

This approximates a common asynchronous-persistence class, not identical crash behavior. Verify actual pinned-version behavior and document AOF buffer/write distinctions. Redis `everysec` may be included as a separately labeled common-operating-policy sensitivity, not silently mixed into the baseline. Redis memory-only is excluded from the main comparison because it drops persistence work entirely. [Redis persistence policies](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/), [Kafka storage design](https://kafka.apache.org/41/design/design/).

Do not force Redis `always` or `WAITAOF` into one side of the baseline while other brokers acknowledge without equivalent fsync. A strict fsync-before-success three-way profile is not promised, because RabbitMQ Streams do not provide the same per-publish fsync boundary. Redis WAITAOF is a separate stronger guarantee whose additional round trip/wait must be timed and whose returned counts must be checked. [WAITAOF](https://redis.io/docs/latest/commands/waitaof/).

Replicated three-node profiles are deferred: they add major memory and acknowledgement-policy differences and are unnecessary for the initial ingress question. Never compare the application's Kafka RF3 profile against single-node Redis/RabbitMQ and call the result equal durability. The report must refuse a single ranking when profile classes differ.

## 8. Workload and concurrency fairness

Use the same seed, schema, fixed-width event ordinal, deterministic payload contents, byte length, and corpus hash for every transport. Keep broker-specific run resource names outside payloads so corpus bytes remain identical across trials. A synthetic key may be included consistently in the canonical body; baseline single partition/stream does not test customer ordering or fan-out.

Initial matrix:

- Payload: 256 B, 1 KiB, 4 KiB, validated after serialization.
- In-flight record limit: 1, 16, 64.
- One logical publisher connection/session per trial initially; record actual SDK connections.
- One destination partition/stream; no Super Streams or Redis sharding initially.
- Compression disabled; explicit message batching disabled in the first baseline where practical.
- Same fixed message count and byte cap per comparison cell; duration-based steady-state trials only when retained-data capacity is proven safe.
- At least three successful repetitions per cell, randomized broker order, separate warmup.

Concurrency means outstanding unconfirmed messages, not UI worker count or unbounded promises. Protocol/library batching differs even with one message per API call; record actual auto-pipelining/coalescing, producer request limits, connection count, and buffers. A later batch profile can use a common application batch size and maximum wait, but never divide batch ACK latency by batch size to claim per-message latency. Each message experiences its own start-to-confirm duration.

Bound load-generator CPU/GC and report event-loop utilization. If it saturates before the broker, the result is client-limited. The claim is throughput of these broker/client/configurations, not an absolute ranking of broker engines. Use fixed offered-rate tests for latency and bounded closed-loop saturation for throughput; do not mix the two score sets.

## 9. 16 GB Docker resource plan

Run only one benchmark broker at a time, plus the shared monitoring stack and runner. No additional PostgreSQL, Connect, business consumers, exporter fleet, or broker replicas are required. Do not alter the existing application resource limits.

Proposed initial limits to validate, not capacity guarantees:

| Component | Starting cap / budget |
| --- | --- |
| Active benchmark broker | 2 GiB container limit and 2 vCPU quota, same outer limits for all three |
| Kafka JVM | Approximately 768 MiB heap inside its 2 GiB limit, leaving native/page-cache room |
| Redis | Approximately 1 GiB maxmemory inside 2 GiB, `noeviction`, explicit retained-data cap |
| RabbitMQ | Memory watermark below container ceiling; verify container-aware detection and disk-free alarm |
| Runner | 512 MiB–1 GiB and a separate CPU allocation |
| Shared Prometheus/Grafana | Observe existing limits; planning allowance approximately 1.5 GiB total |
| Docker/OS/cache/headroom | Reserve at least 2 GiB; inspect actual availability rather than sum limits alone |

This is roughly a 6–6.5 GiB active footprint budget before unrelated workloads, designed to work sequentially even below 16 GB if measurements confirm headroom. Current Docker reports only about 7.44 GiB, so the preflight must reject configurations that leave insufficient reserve. Do not automatically change Docker Desktop memory or stop user workloads.

Ingress-only retained data grows continuously because no consumers remove it. Redis keeps stream data in memory, while disk-backed brokers use disk/page cache differently. Size experiments using payload plus measured per-entry overhead, warmup entries, allocator/AOF/rewrite overhead, and existing data. Start with a conservative canonical-body cap (for example 128 MiB per run) and calibrate actual memory. Enforce a common message-count cap per cell and preserve a resource-limited result instead of trimming only Redis mid-run.

Disable/avoid retention deletion, eviction, compaction, and AOF rewrite during the controlled initial window where safely configurable on benchmark-only resources; record the policy. Larger steady-state/background-maintenance experiments are separate. No `MAXLEN` on only Redis to make an otherwise oversized run finish.

Create fresh benchmark-owned resources outside timing. Delete only those named in the completed run manifest after evidence capture and under explicit cleanup intent. No FLUSHALL, application-topic reset, shared-volume deletion, or system-wide cache clearing. Verify disk free space before each run and annotate disk alarms, OOM, swap, throttling, and memory pressure; resource-failed runs cannot rank as successful throughput tests.

## 10. Prometheus and Grafana reuse

The current checkout does not contain an implemented stack. The plan therefore has a concrete dependency: locate/reuse the actual external/shared stack if present at implementation time, otherwise complete the already planned shared monitoring foundation separately. Do not provision a new Prometheus/Grafana pair inside the benchmark project.

Add a single benchmark target and dashboard to that stack. Reuse any implemented registry/helper once verified; if none exists, use a process-local metrics module in this package without modifying application telemetry. Required metrics:

- `lab_ingress_attempts_total`
- `lab_ingress_confirmed_messages_total`
- `lab_ingress_confirmed_payload_bytes_total`
- `lab_ingress_errors_total{reason}` with finite reasons
- `lab_ingress_ambiguous_messages_total`
- `lab_ingress_publish_duration_seconds` histogram
- `lab_ingress_inflight_messages` gauge
- bounded run phase/status and exporter freshness

Bounded labels: broker, profile, phase. No customer/event/run UUID labels on hot time series. One active runner target is sufficient; use timestamps/annotations and immutable manifests to associate trials. Explicitly exclude warmup/drain from steady-state panels. Process restart between trials resets counters; do not reset them mid-process without a defined contract.

Dashboard: confirmed msg/s, decimal MB/s, p50/p95/p99 with sample counts, in-flight work, errors/ambiguity, run phase, resource pressure if existing collectors provide it, and profile details. Use `rate` for live trends and correctly aggregate histogram buckets before quantiles. Missing data must display unavailable, not zero.

Exact results come from runner counters and mergeable histogram artifacts. Scrape-derived `increase` is not an exact count, and a short run may finish between scrapes. Keep the metrics endpoint available for a bounded final-scrape interval and persist artifacts regardless. Do not add Pushgateway just to avoid that lifecycle. All brokers must have the same scrape interval and monitoring conditions.

Historical cross-trial bar charts can consume the shared reporting capability when available. Do not add a reporting service or Grafana plugin as a prerequisite for the first ingress MVP; JSON/CSV reports and live Grafana panels are enough initially. [Prometheus histogram guidance](https://prometheus.io/docs/practices/histograms/), [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/).

## 11. Implementation tickets

### ING-01: Inventory and capability proof

Locate shared monitoring endpoints/config owner; verify actual Docker allocation and workload headroom; identify any newly added standalone runner before creating one. Pin SDKs and confirm native ACK semantics, especially RabbitMQ. Record decisions in a benchmark-only manifest/schema note. No application changes.

Acceptance: each SDK has a verified positive confirmation and failure/timeout path; shared monitoring integration location is known or explicitly blocked; clean application-file boundary recorded.

### ING-02: Minimal runner and deterministic workload

Add one workspace package, publisher contract, bounded scheduler, seeded byte generator, monotonic timer, lifecycle, streamed histograms, and immutable reports. Lifecycle: prepare, warmup, warmup-drain, measure, final-drain, completed/failed/cancelled/timed-out. Unit-test with a fake publisher; it must never produce real broker benchmark claims.

Acceptance: byte-identical corpora, bounded memory/in-flight count, correct MB arithmetic, confirmation-only success, duplicate callback protection, honest deadline handling, and precise window/cohort accounting.

### ING-03: Isolated Kafka baseline

Implement independent KafkaJS publisher and optional benchmark-only Kafka Compose profile. Explicit topic creation and ACK settings, no application endpoints. Validate Kafka-only sample reports before adding other transports.

Acceptance: broker acknowledgements observed, send failures excluded, no business topics/groups/databases touched, and baseline artifact reproducible.

### ING-04: Redis Streams publisher

Add XADD adapter, AOF profile, noeviction/maxmemory, and bounded pipeline policy. Prevent hidden offline replay and detect OOM/persistence errors. No XREADGROUP, subscriber, or XACK implementation.

Acceptance: same payload hash and measurement contract as Kafka, each successful XADD counted once, persistence profile read back/recorded, capacity/ambiguity paths tested.

### ING-05: RabbitMQ native Streams publisher

Enable stream protocol in a benchmark-only node, configure advertised endpoints, create run-owned stream, implement publishing-ID confirmation/error tracking and drain. No Quorum Queue fallback and no consumer implementation.

Acceptance: success cannot be recorded before server PublishConfirm; duplicate/late confirmations and errors are accounted correctly; node disconnect does not turn queued sends into success.

### ING-06: Shared monitoring integration

Expose the runner's metrics endpoint, add the target to the existing Prometheus, provision one Grafana dashboard using the existing datasource, and annotate measurement windows. Do not add monitoring services or change business-service instrumentation.

Acceptance: all three live runs are visible in the same dashboard with valid units/windows; exact artifact totals reconcile with the runner; scrape gaps do not lose the final result.

### ING-07: Fairness matrix and handoff

Run small feasibility trials first, then repeated matched cells under actual memory constraints. Validate effective broker settings, manifest compatibility, correctness, resource limits, and SDK bottlenecks. Publish comparisons only for compatible successful cells and keep failures visible.

Acceptance: >=3 successful trials per reported cell, median/range reported, no average-of-percentiles, reproducible CLI/config/seed, and existing application unchanged.

## 12. Artifacts, tests, and completion gate

Each trial saves manifest.json, result.json, latency histogram, bounded time-series samples, and sanitized execution log. Manifest includes source revision and dirty-tree flag, client/broker versions, host/Docker memory/CPU, payload hash/count/size, transport topology, exact ACK/persistence/retry/batch/compression settings, publisher count/in-flight limit, window times, warmup/drain, limits, and monitoring interval. Include stopped/failed run status and unresolved messages.

Essential tests: no ACK/no success; delayed confirmation; duplicate confirmation; batch partial error; lost connection with ambiguous write; successful confirmations after measurement end; timeout; cancellation; exact payload size and decimal MB; known-distribution percentiles; strict resource allowlists; 16 GB and lower-memory preflight; and no application imports/endpoints/resources.

Adapter integration tests may inspect benchmark-owned metadata outside timing to validate resource creation and successful writes, but do not create ongoing consumers or include read throughput. Any optional bounded readback must be clearly verification-only, outside the ingress score.

Run required repository formatting/lint/typecheck/unit gates for implementation and scoped real broker tests. Use `promtool` and dashboard/config validation for monitoring additions. Capture application path diffs before and after; no source/config changes in prohibited applications, no newly wired adapters there, and no new application dependencies. Do not start the business stack as a test harness.

Suggested future command shape, not implemented:

```text
pnpm benchmark:ingress --broker kafka --profile single-node-async --config <cell.json>
pnpm benchmark:ingress --broker redis-streams --profile single-node-async --config <cell.json>
pnpm benchmark:ingress --broker rabbitmq-streams --profile single-node-async --config <cell.json>
```

Completion means all three independent publishers report verified broker-acknowledged ingress under documented comparable profiles, reuse the shared monitoring stack, stay within measured resource limits, and leave every existing application feature unchanged. No implementation, dependency install, container mutation, commit, or push occurred in creating this plan.

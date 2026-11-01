# Ingress benchmark deep audit and redesign

Date: 2026-11-01. Analysis and implementation plan only. This document supersedes earlier assumptions that the runner and monitoring stack did not exist.

## 1. Executive assessment

The current implementation is a useful isolated, broker-confirmed, single-record baseline. It is not a measurement of maximum sustainable broker ingress capacity. The reported 2,487 Kafka / 8,818 Redis / 4,285 RabbitMQ messages/second are reproducible values in saved artifacts, but the evidence cannot attribute their differences to broker limits.

All three adapters have plausible genuine acknowledgement boundaries. Kafka and RabbitMQ deliberately issue single-record publishes; Redis allows concurrent outstanding commands but does not use explicit or automatic client batching. Trials are extremely short, the client scheduler can impose significant overhead, and neither client nor broker resource saturation was recorded. No predetermined winner is justified.

Retain the existing standalone package, adapters, deterministic workload concept, CLI, comparison script, dedicated broker deployment, and shared Prometheus/Grafana. Refactor the benchmark where measurement correctness requires it. Do not rewrite the business application or create a new benchmark service ecosystem.

## 2. Scope and audit evidence

Protected paths: producer-api, outbox-publisher, loyalty-worker, analytics-worker, replay-worker, their configuration/databases/contracts, and the Purchase → Outbox → Kafka → Workers deployment. Existing unrelated modifications in those paths belong to other work. This plan neither endorses nor changes them. RabbitMQ remains native Streams, not Quorum Queues. No subscribers, consumer processing, Redis XACK, or business integration are part of measured runs.

Inspected actual benchmark TypeScript, installed SDK source, CLI/orchestration, dedicated Compose, monitoring configuration/dashboard, tests, comparison summary, and 55 saved trial result directories. Installed clients: KafkaJS 2.2.4, ioredis 5.11.1, rabbitmq-stream-js-client 1.1.0. Ten benchmark unit tests across four files passed. Read-only in-memory diagnostics against existing compiled code reproduced the ineffective drain timeout and in-flight gauge issue; corresponding TypeScript was inspected. No broker/load experiments were run, no containers started/stopped, and no source/configuration files edited.

Docker reported approximately 7.44 GiB available and 16 logical CPUs, despite 16 GB physical RAM. No containers were running at inspection. Compose files show intended limits; the audit did not establish the effective limits of historical runs. No saved resource time series prove past broker/client utilization.

References below use repository-relative files and verified source locations. These are navigation anchors, not guaranteed stable line numbers after implementation.

## 3. Current architecture and execution flow

1. `scripts/run-ingress-comparison.mjs` starts each dedicated broker in fixed Kafka → Redis → RabbitMQ order, waits for health and another two seconds, then executes three repetitions at concurrency 1/16/64. Each trial is a separate Windows Node process.
2. `apps/ingress-benchmark/src/main.ts` parses configuration, starts a process-local metrics HTTP endpoint, chooses one adapter, and creates the workload and runner.
3. `runner.ts` initializes one publisher, sends 200/500 warmup records for the scripted matrix, then sends 2,000/5,000 measurement records using an in-flight Promise map and repeated `Promise.race` calls.
4. Kafka publishes each record to partition zero; Redis issues XADD; RabbitMQ uses the binary Stream protocol and maps server confirmation IDs to promises.
5. Measurement ends only after every outstanding publish promise settles. The later drain block normally has nothing to drain, and cannot rescue a never-settling publish.
6. The runner derives confirmed count / elapsed time and exact retained-sample percentiles. `report.ts` writes manifest/result JSON. The metrics listener closes immediately in `finally`.
7. Prometheus scrapes the shared endpoint every two seconds. Grafana graphs 10-second counter rates and 30-second histogram rates. The comparison script selects the median-throughput trial and copies that trial's latency numbers into fields named median latency.

Good foundations: separate Compose project and volumes, real confirmation paths, payload-byte accounting in decimal MB, monotonic within-process timing, bounded logical publish concurrency, warmup excluded from result counters, and saved per-trial artifacts. These should survive the redesign.

## 4. Confirmed findings and severity

Severity describes impact on benchmark validity, not application production impact.

### F01 — High: no throughput batching for Kafka; RabbitMQ forced to one record

`src/adapters/kafka.publisher.ts` constructs `messages: [{...}]` for every `publish`, always partition zero, with maxInFlightRequests 64. Installed KafkaJS `src/producer/sendMessages.js` groups the supplied messages and creates broker requests within that invocation. It does not provide a Java producer-style cross-call accumulator in this path. A Kafka record batch exists on the wire, but here it contains one application record; multiple concurrent send calls do not constitute effective multi-record batching.

`src/adapters/rabbitmq.publisher.ts` sets maxChunkLength=1. Installed SDK `dist/publisher.js:128` enqueues records, `:247` tests chunk length, and `:250` pops at most that number. The configured adapter prevents multi-message chunks. The SDK default is 100 and supports normal multi-message frames; these capabilities are not exercised.

Consequence: current throughput rankings measure unbatched request cost and this client configuration, not each transport's optimized sustainable ingress. Preserve this mode for scenario A; add separate throughput modes rather than deleting the baseline.

### F02 — High: Redis concurrency is not explicit batching, and retry policy is incomplete

`src/adapters/redis.publisher.ts` sends independent XADD promises without `pipeline()` and without enableAutoPipelining. Installed `ioredis/built/redis/RedisOptions.js:54` defaults auto-pipelining false. Concurrent calls can still have several RESP commands outstanding on a connection; saying Redis is fully serialized or has no protocol pipelining would be inaccurate. What is missing is explicit client write coalescing/batch control and measurement of its effectiveness.

The adapter sets enableOfflineQueue=false and maxRetriesPerRequest=0, but does not explicitly override default reconnect strategy or autoResendUnfulfilledCommands=true (`RedisOptions.js:11,46`). Their combined failure behavior must be tested. This is not evidence that duplicates occurred in saved trials; it is evidence the claimed no-hidden-retry policy is incompletely specified.

### F03 — Critical: timeout and cancellation do not bound the run

`src/runner.ts:136,142` waits on Promise.race/allSettled before reaching final drain. A publish promise that never settles can hang warmup or measurement indefinitely. DrainTimeoutMs is therefore not an operation/run deadline. Diagnostic: a fake never-confirming publisher remained pending after 200 ms with drainTimeoutMs=10.

`main.ts` has no bounded cancellation protocol, and `run-ingress-comparison.mjs:80` uses execSync without a timeout. A trial failure throws out of orchestration before normal broker stop/summary persistence. Add deadlines to admission, confirmations, run, final drain, and shutdown; a timer Promise alone must not leave background tasks mutating a finalized result.

### F04 — High: count-to-completion mislabeled as steady state

`runner.ts:84–146` sets isWindowActive true until all target records finish. There is no fixed-duration offered-load window. Late confirmations are normally zero by construction, not proof that all ACKs met a deadline. Fill/drain and generator/client overhead are included in elapsed throughput. This is a valid finite-cohort metric when labeled honestly, but not the steady-state interval described by comments and earlier plans.

`scripts/run-ingress-comparison.mjs:59` chooses 2,000/5,000 messages and 200/500 warmup. Warmup length is count-based, differs in wall time per broker, and warmup errors are swallowed by `executeBoundedBatch`. `cleanTrial` at runner.ts:172 does not require complete warmup or a fully reconciled target set. Future validity must include those conditions.

### F05 — High: scheduler and allocation may limit ingress; no attribution evidence

Repeated `Promise.race(inFlight.values())` attaches handlers across the active set on each wait; pending slow promises may accumulate callbacks. This is a confirmed algorithmic overhead path; its share of observed runtime remains unmeasured. `workload.ts` copies a Buffer and writes an ordinal per message during measured elapsed time, before adapter latency starts. Thus throughput includes generator/scheduling costs not reflected in ACK latency.

`histogram.ts` stores all samples and copies/sorts them at report time. Sorting occurs after the timed loop, so it does not directly explain low measured throughput; allocation/GC during sampling can. `metrics-server.ts` stores a second full latency array and rescans all samples for all histogram buckets on every scrape. Long runs would make monitoring increasingly expensive. No CPU profile/event-loop/GC data proves whether these were dominant in historical runs.

### F06 — High: Prometheus can miss the trial; live metrics contain correctness defects

`main.ts` immediately closes the endpoint after a result; the target's scrape interval is two seconds. Some entire measured runs are shorter than one scrape. Even a single snapshot cannot establish a rate. Existing Grafana 10/30-second windows cannot reconstruct exact subsecond trials and can mix adjacent trials/configurations with the same labels.

`runner.ts` removes a receipt then calls setInflight(map.size), followed by recordConfirmation/recordError; `metrics-server.ts:54,62` decrements again. Diagnostic: setInflight(2), then recordConfirmation, exports 1 even though two other messages remain outstanding. Fix one owner for in-flight state.

Error labels use arbitrary truncated exception text (`runner.ts:121`, metrics-server.ts:114) without Prometheus label escaping. Quotes/newlines can break exposition, and arbitrary text increases cardinality. Phase labels omit stable one-hot zeroing, dashboards lack experiment selectors, and MB/s data is already divided by 1e6 but uses a bytes formatter (`decbytes`), risking incorrect display units. Port-bind failure silently disables monitoring, which should become an explicit quality flag.

### F07 — High: report summaries and provenance overstate evidence

`run-ingress-comparison.mjs:158–170` selects the median-throughput trial; its p50/p95/p99 are not independently computed medians of trial percentiles. This is a definite labeling/statistical defect. With two clean trials it also selects the upper throughput value rather than the mathematical median. The script accepts fewer than three clean trials and does not validate manifest compatibility.

`report.ts` saves only manifest and final summary, not mergeable histograms, time-series windows, partial failure evidence, or detailed retry/confirmation accounting. Output base depends on current working directory. Summary is overwritten and does not retain source trial IDs. Manifests omit exact SDK/image digests, effective Docker resources, git revision, explicit seed, phase timestamps, actual topology, and runtime resource samples.

`workload.ts` hashes the template before overwriting its first eight bytes with ordinal. Deterministic generation is reusable, but payloadSha256 is not the hash of the actual corpus. Name it template hash or add a canonical corpus hash with generator version, seed, range and payload length.

### F08 — High: requested settings are presented as effective settings

Kafka initialization swallows every topic-creation error, allows auto-creation, and does not validate an existing topic. getEffectiveSettings reports RF1/minISR1 constants without reading actual state. RabbitMQ swallows all stream creation errors too. Redis INFO collection does not establish appendfsync policy: query actual CONFIG settings where permitted and fail a scored preflight if they cannot be verified.

Mutable image tags (`redis:7-alpine`, `rabbitmq:3.13-management-alpine`) and omitted runtime image digests weaken reproducibility. Broad catch blocks must distinguish already-exists from permissions, reachability, and invalid configuration.

### F09 — High: environmental controls and retained-data management are incomplete

Dedicated Compose limits each broker to 2 CPU/2048M, but the historical manifest never confirms enforcement. Native Windows runner has no corresponding recorded CPU/memory budget; Docker network/VM overhead is part of the measurement. Persistent destinations/volumes are reused across trials without reset policy. Redis retains entries in RAM; Kafka/RabbitMQ retain on disk. All accumulate history and potentially trigger different maintenance thresholds.

Fixed broker order, increasing concurrency order, two-second startup delay, fresh Node process per trial and no cooldown/resource stability criteria allow startup, JIT, cache, thermal, host contention and AOF/segment-maintenance bias. Current broker version settings and cache state are not enough to explain its magnitude.

## 5. What the suspicious results establish

Source: `benchmark/ingress/comparison-summary.json` and selected result artifacts below. Each selected run has payload 1024 bytes, concurrency 64, 500 warmup records, 5,000 measured confirmed messages, and reported zero errors/timeouts/ambiguity.

| Broker | Selected trial | msg/s | Decimal MB/s | Duration | Mean ACK ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Kafka | trial-1793510830159-kafka | 2487.31 | 2.5470 | 2.0102 s | 25.430 | 67.230 |
| Redis | trial-1793510877436-redis-streams | 8818.34 | 9.0300 | about 0.567 s | 4.191 | 10.105 |
| RabbitMQ | trial-1793510954701-rabbitmq-streams | 4285.22 | 4.3881 | about 1.167 s | 8.693 | 18.694 |

These are observed finite-cohort client/broker configurations. Kafka c64 trials span 1976.36–2950.90 msg/s; Redis 7916.40–8987.96; RabbitMQ 4100.71–4390.97. Three repetitions are too few for strong population/tail claims, especially with startup variance and subsecond runs.

ACK audit: Kafka awaits acks=-1 send completion. Redis awaits the server XADD response. RabbitMQ resolves from publish_confirm, not basicSend completion; installed SDK publisher.js:153 maps native confirm and publish_error to that callback. Therefore the allegation that RabbitMQ merely counts socket writes is not supported. However, no packet traces or durable readback artifacts independently prove all historical writes; ACK success must not be equated with identical persistence or exactly-once storage.

Hypotheses requiring experiments:

1. Single-record framing and request overhead suppress Kafka/RabbitMQ throughput. Strong configuration evidence, magnitude unmeasured. Test batch sweep while holding resources/payload/in-flight byte budget fixed.
2. Scheduler, encoding and Node event-loop limitations contribute disproportionately at high concurrency. Test null adapter, CPU profile, generator timing, event-loop delay, then worker-process scaling.
3. Windows-to-Docker networking contributes small-message overhead. Repeat matched host vs container runner profiles without mixing results.
4. Broker CPU, disk or configured caps may be saturated. Collect resource time series and actual native request/chunk rates before attributing a limit.
5. Short-run warmup/cache differences affect variance. Repeat longer randomized blocked trials with stable warmup.

Diagnostic consistency check only: throughput × mean ACK seconds is approximately 63.3, 37.0 and 37.3 outstanding operations for Kafka/Redis/RabbitMQ. This is not a valid direct application of Little's law to prove occupancy because the run is short, nonstationary, and scheduling sits outside latency. It motivates recording actual time-weighted occupancy and generation gaps. It does not prove a Kafka engine bottleneck or a Redis advantage.

## 6. Target architecture: evolve the existing package

Keep `apps/ingress-benchmark` and its publisher port. Extend only the necessary capabilities: typed publish outcome, per-record identity/timing, effective settings, controlled batching/flush, lifecycle cancellation, and diagnostics. Do not introduce a new service, benchmark database, general plugin framework or business imports.

Separate responsibilities inside existing files and a few cohesive additions:

- Config/profile chooses scenario, limits, SDK options, topology and durability class.
- Workload generates a bounded immutable buffer pool or precomputed count corpus outside the timed admission loop. Payloads remain deterministic and logically unique; no mutation while a client retains a buffer.
- Runner owns admission clocks, bounded in-flight records/bytes, terminal accounting, phase state, deadlines and result validity.
- Publisher adapters own native batching, confirms, error/retry evidence and setup/readback. Preserve record identity through batching.
- Histogram/metrics modules observe incremental bounded state; report owns exact artifacts and cross-trial aggregation.
- Existing orchestration starts only the selected dedicated broker, conducts preflight, runs randomized profiles, saves partial results, and closes owned resources in finally.

For scheduler throughput, replace repeated whole-set Promise.race with a completion notification/refill queue or fixed asynchronous worker slots. No unbounded Promise.all. Separate logical outstanding records, bytes, queued batch records, native requests and socket backpressure. A global bound must apply across all producer processes.

Start single-process. Add optional 2/4 producer processes only after calibration shows client saturation. Each process has independent client/histogram; coordinator aligns windows and merges counts/histograms, not per-process p99. Keep it inside the same executable/package with IPC, not a new distributed service. Broker parallelism remains a separate axis.

## 7. Scenarios and fairness contract

### A — Single-message acknowledgement latency

One producer process, one connection/session, one destination shard, one outstanding message. No intentional batching; Kafka sends one message, Redis auto-pipeline off, RabbitMQ chunk one/immediate flush. Reuse the persistent connection throughout warmup and measurement. Report p50/p95/p99, sample count, successful fraction and timeout rate. The primary metric is invocation-to-confirm using a monotonic clock. This is closed-loop request-response latency, not latency under externally imposed load or capacity.

### B — Maximum sustainable confirmed ingress

Allow broker-appropriate batching with an equal tuning/search budget and identical resource/durability class, canonical bytes, external latency/error criteria and measurement duration. Kafka: explicit messages arrays accumulated by records/bytes/max age; do not add unsupported Java producer linger/batch settings to KafkaJS. Redis: explicit pipeline or tested auto-pipelining, not MULTI/EXEC; inspect every command response. RabbitMQ: multi-message Stream frames with per-publishing-ID confirmation; ordinary chunks first, sub-entry batching a separately identified mode because confirmation granularity differs.

Measure actual records/request or chunk, bytes/batch, batch wait and partial-batch flush. Include intentional batching wait in admission-to-confirm latency. Also expose dispatch-to-confirm separately. Do not divide a batch ACK duration by message count. Redis pipeline exec can resolve after all responses: per-command timing is preferable; if only pipeline completion is measured, label it conservatively rather than mixing it with per-command arrival latency.

Use a bounded closed-loop capacity search to find a candidate range, then open-loop offered-rate trials to validate sustainability. Proposed staircase: coarse doubling until throughput plateaus or latency/error/backpressure gate fails, then refine around the knee. Admission queue and maximum bytes stay bounded; scheduled-but-not-admitted work and generator schedule lag are explicit, not hidden coordinated omission. Do not call a generator-limited trial broker saturation.

Define sustainability before running: repeated steady windows with confirmed rate near offered rate (proposed >=99% after accounting for boundary work), no growing backlog, no unexplained failures/ambiguity, latency within the declared scenario budget, and no disqualifying resource pressure. Report the throughput/latency frontier rather than one universal maximum. A default 100 ms p99 sensitivity target may be used as an experiment parameter, not a user/business SLO.

### C — Scaling and saturation

Sweep producer concurrency, batch records/bytes/age, payload size and then producer processes. Separately sweep destination shards: Kafka partitions, RabbitMQ multiple Streams/Super Stream, Redis independent stream keys on one instance. Multiple Redis keys on one instance do not imply multiple execution cores; multi-instance Redis is a different resource/topology profile. Compare within-system scaling curves and disclose these differences rather than forcing a fictitious equal partition abstraction.

Never mix application E2E, consumer throughput, or business DB work into these scenarios. Report performance of the pinned broker + SDK + configuration, not an intrinsic broker ranking unless client non-limitation is demonstrated.

## 8. Experimental matrix and statistical protocol

Avoid a full Cartesian explosion. Use a staged screen then independent final trials:

| Stage | Dimensions | Proposed initial values |
| --- | --- | --- |
| A latency | Payload, repeats | 256 B / 1 KiB / 4 KiB; concurrency1; batches1; 5 repetitions |
| B screen | Outstanding records | 16 / 64 / 256 / 1024, bounded by bytes and SDK request limits |
| B batching | Records, bytes, max age | 1 / 16 / 64 / 256; 16 / 64 / 256 KiB ceilings; 0 / 1 / 5 ms where supported |
| B final | Offered rate | 50 / 75 / 90 / 100 / 110% of pilot capacity, independently measured |
| C client | Producer processes/connections | 1 / 2 / 4, fixed total resource budget and explicit per-process load |
| C broker | Shards/payload | 1 / 3 / 6 destinations; 256 B / 1 KiB / 4 KiB / optional16 KiB |
| Durability | Separate families | Single-node async persistence first; replicated profiles later; stronger fsync tests separately labeled |

Batch values are tuning candidates, not a requirement that every broker choose the same internals. Fix the tuning budget and selection criterion; do not choose Kafka's best run while reporting Redis defaults. Compression off in the main matrix; compression becomes its own profile with a realistic and identical entropy corpus (the current repeated template can become highly compressible across records).

Use pilot feasibility runs, followed by proposed warmup >=15 seconds and measurement >=30–60 seconds with stable interval diagnostics, then bounded drain and cooldown. These durations are conditional on retained-data capacity; do not blindly run them into Redis OOM. If the machine cannot retain the same workload for that duration, use a smaller common feasible profile and label it a short burst; maximum sustainable ingress remains unestablished. Warmup stabilization must have a maximum deadline and documented criterion, not an arbitrary wait forever.

Use at least five independent final repetitions, preferably ten for noisy cells; three is screening only. Randomize broker/profile order in recorded blocks so startup order/thermal state do not always favor one broker. Use matched seed/corpus within each block. Keep failed/censored trials visible, require the minimum valid count, and report per-trial values plus median, IQR and range. If confidence intervals are used, resample independent trials, not millions of correlated messages; state method and small-N limits.

Compute median of trial p99s explicitly and label it; pooled-event p99 requires merging histograms and weights high-throughput trials more. Neither is interchangeable with p99 from the median-throughput trial. Preserve the latter only under that exact label.

## 9. Durability families

Single-node asynchronous persistence is a pragmatic initial common class, not identical failure semantics: Kafka acks=all with RF1/minISR1 and normal OS flush, RabbitMQ Stream server confirms on one node, Redis AOF enabled with appendfsync=no. Verify effective configuration rather than labeling constants. Do not include Redis memory-only in the same chart without an explicit weaker-guarantee badge.

RabbitMQ Streams write persistent logs but do not explicitly fsync every confirmed publish; Quorum Queue guarantees cannot be substituted. Redis everysec, always and WAITAOF change the completion cost/guarantee and must be separate profiles. Kafka acks=all is not a per-message fsync guarantee. Sources: [RabbitMQ Streams](https://www.rabbitmq.com/docs/streams), [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/), [Kafka design](https://kafka.apache.org/41/design/design/).

Replicated profiles: Kafka RF3/minISR2/acksall, RabbitMQ three-node Stream replication/confirmed quorum behavior, Redis primary/replicas with an explicitly declared WAIT/WAITAOF policy. These are not equivalent consensus/failover guarantees. Measure any waiting operation inside the publish completion timer. Do not run all replicated stacks simultaneously on this machine. A strict equivalent fsync-before-ACK three-way mode is not promised; report a capability table and refuse a misleading combined ranking.

## 10. Measurement and telemetry redesign

Use fixed monotonic phase boundaries. Throughput counts first observed successful confirmations in [t0,t1); bytes are canonical successful payload bytes / seconds / 1,000,000. Preserve a separately declared admitted-in-window cohort with final outcome after bounded drain. Record boundary confirmations and late outcomes; throughput-window and latency-cohort statistics must state their populations. Warmup completion/failure is independently accounted and must meet validity conditions.

Each record terminates once as confirmed, definitively rejected, or ambiguous/timed out. Timeout does not prove non-acceptance. Track attempt count/retry count separately from logical records; ACK callback duplicates are not proven duplicate storage. Native idempotency/retry modes are explicit. Controlled readback of benchmark-owned resources after timing can validate persisted counts/identities in tests, but no standing consumer belongs in the benchmark hot path.

Time fields: scheduled, admitted, adapter accepted/enqueued, batch flushed if observable, confirmation observed. Report schedule-to-confirm (offered-load service experience), admitted-to-confirm (primary publish latency including batching), and optional wire-dispatch-to-confirm (SDK visibility permitting). Clock timestamps are per process; coordinate worker start without subtracting unsynchronized monotonic epochs. Use UTC for artifact/Grafana correlation only.

Replace raw arrays with a validated bounded mergeable histogram for exact reports and incremental Prometheus histogram buckets. Record sample count, invalid/overflow samples, min/max and histogram precision. Empty sample populations are null/unavailable, not zero latency. Avoid per-message logging.

Reuse existing Prometheus instance, datasource UID and ingress dashboard. Use a local prom-client registry in the standalone package; reuse the dependency convention, not the Nest-specific business MetricsService. Do not refactor shared service telemetry as a prerequisite.

Metrics additions: offered/admitted/confirmed record counters, canonical confirmed bytes, typed errors/timeouts/ambiguous/retries, duplicate callbacks, queue/in-flight records and bytes, native request/batch count and size, queue/batch/ACK histograms, actual rate and schedule-lag, one-hot phase and validity gauges. Bounded labels: broker/scenario/profile/worker slot; arbitrary error strings/event IDs are excluded. A bounded active experiment slot plus precise artifacts/annotations identifies runs without unbounded runId histograms.

Resource evidence: runner process CPU/RSS/heap, event-loop utilization/delay, GC time, generator/scheduler time; Docker per-container CPU/memory/throttling/OOM, network and block I/O deltas; host/VM memory/swap/disk pressure where accessible. Collect through a bounded read-only sampler or existing collectors into the same Prometheus/artifacts. No second monitoring backend. Distinguish disk block I/O from page-cache writes and Docker VM resources from native Windows runner resources; unavailable values remain unavailable.

Dashboard additions: scenario/profile filters; offered/admitted/confirmed curves; bytes/s with consistent decimal units; latency cohorts/sample count; effective batch size; scheduler lag and queue pressure; resource saturation; run phase/quality flags and manifest links. Avoid smooth interpolation suggesting measurements between sparse samples. Keep the endpoint alive for baseline and final scrapes or use a session-wide runner serving multiple trials with correct phase/slot semantics. Exact artifacts remain authoritative; historical comparison does not rely on Prometheus extrapolated increases.

## 11. Resource-constrained experiment protocol

1. Read actual physical and Docker memory, current containers and host workload; validate headroom. Do not change Docker allocation or stop unrelated application processes automatically.
2. Sequentially start one dedicated benchmark broker with equal outer CPU/RAM allocation; verify inspect/cgroup settings and record image digest. Native client resource allocation is recorded separately. JVM heap must leave native/page-cache room; Rabbit memory watermark and Redis maxmemory must stay below cgroup ceiling.
3. Start shared monitoring once; keep monitoring/healthcheck conditions identical. Measure monitoring-on/off overhead during calibration. Docker healthcheck CLI/JVM processes can themselves affect two-CPU trials; use a consistent lightweight policy and annotate their cadence.
4. Verify effective ACK/persistence/topology settings. Use dedicated per-trial destinations. Select a clean logical resource/cache protocol and document that it is not a cold OS cache. No machine-wide cache purge. Fresh names alone do not erase disk/cache state.
5. Estimate retained memory/disk with measured amplification. At 100k msg/s ×1 KiB ×60 s, payload alone is about 6.14 GB, incompatible with the current Redis 1 GB maxmemory even before overhead. Include warmup, AOF rewrite/fork, allocator and log buffers. No Redis-only MAXLEN/eviction during the test to hide this constraint.
6. Reserve approximately 2 GiB active broker, 0.5–1 GiB runner, measured existing monitoring allowance, and >=2 GiB OS/cache reserve as a starting budget—not a guarantee. With Docker only 7.44 GiB, actual concurrent workloads may invalidate even this envelope. Run a lower footprint calibration first.
7. Use comparable dedicated Docker volumes on the same storage class, not Windows bind-mounted data for one broker and Linux volumes for another. Record filesystem, disk free space, page-cache policy, AOF rewrite and segment maintenance. Pin versions; distinguish a version-upgrade experiment from a batching change.
8. Run calibration/warmup, drain, measured window, final drain, artifact persistence, then cooldown until resources stabilize within a bounded interval. Save partial results in finally and stop only benchmark-owned resources.
9. Cleanup only allowlisted destinations/volumes with explicit scope. Never FLUSHALL, application topic reset, business group reset or broad volume prune. Preserve raw evidence before cleanup.

Long sustainable runs may be infeasible at peak ingress on a 16 GB physical host with memory-retained Redis history. This is a material result limitation, not permission to bias retention settings. Run shorter comparable bursts, lower offered load, or use a larger machine for the capacity claim. Separate a rolling-retention sensitivity if desired, with retention work explicitly in scope for all brokers.

## 12. Exact file change map

All entries below are proposed changes, not edits made during this audit.

| File | Required work |
| --- | --- |
| `apps/ingress-benchmark/src/contracts.ts` | Versioned scenario/profile/outcome/timing/result contracts; cancellation and batch capabilities |
| `apps/ingress-benchmark/src/config.ts` | Strict CLI options; reject unknown keys; scenario/duration/batch/bytes/request/deadline/resource validation; complete URL/credentials handling |
| `apps/ingress-benchmark/src/main.ts` | Lifecycle cancellation, partial artifacts, metrics availability quality policy, session/final scrape lifetime |
| `apps/ingress-benchmark/src/runner.ts` | Bounded efficient scheduler, fixed windows, offered-load mode, deadlines, terminal reconciliation, warmup gate |
| `apps/ingress-benchmark/src/workload.ts` | Bounded generation outside timed admission, canonical corpus identity, configurable entropy without mutation reuse bugs |
| `apps/ingress-benchmark/src/histogram.ts` | Bounded mergeable representation, precision/overflow/empty handling |
| `apps/ingress-benchmark/src/metrics-server.ts` | Incremental registry, accurate in-flight ownership, finite labels, lifecycle, diagnostics |
| `apps/ingress-benchmark/src/report.ts` | Versioned immutable JSON/histograms/timeseries/partial errors; deterministic output root; independent statistical summaries |
| `apps/ingress-benchmark/src/adapters/kafka.publisher.ts` | Real multi-message sends, configurable request limits, per-record settlement, fail-closed setup/effective settings |
| `apps/ingress-benchmark/src/adapters/redis.publisher.ts` | Pipeline modes, per-command errors/timestamps, explicit reconnect/resend behavior, CONFIG verification |
| `apps/ingress-benchmark/src/adapters/rabbitmq.publisher.ts` | Typed publisher, chunks/flush age, native confirmation lifetime/error/close handling and publisher-ID scoping |
| New `apps/ingress-benchmark/src/resource-sampler.ts` | Bounded process/container samples and missing-data reasons |
| Optional new `apps/ingress-benchmark/src/worker.ts` | Same-package producer process scaling only after calibration proves need |
| `apps/ingress-benchmark/test/*.spec.ts` and new adapter/integration tests | Coverage described below |
| `apps/ingress-benchmark/package.json`, lockfile, existing Vitest discovery as needed | Local metrics/histogram dependency and tests; no application runtime dependency changes |
| `scripts/run-ingress-comparison.mjs` | Profile-driven randomized blocks, bounded child processes, preflight, cleanup/finally, manifest comparison, exact statistics |
| `docker-compose.ingress-benchmark.yml` | Pinned images, parameterized dedicated settings, effective limits, opt-in runner placement and separate topology profiles |
| `docker/ingress-benchmark/rabbitmq.conf`, `enabled_plugins` | Benchmark-only settings for native Stream visibility and controlled environment |
| New `benchmark/ingress/profiles/*.json` | Versioned scenario matrices and compatibility rules |
| `docker/monitoring/prometheus/prometheus.yml.template`, renderer | Additive benchmark scrape/rules only; regenerate derived prometheus.yml |
| `docker/monitoring/grafana/dashboards/ingress-benchmark.json` | Correct units, filters, quality/resource/batching panels and annotations |
| `README.md`, backlog/ADR benchmark sections | New methodology, reproducibility, limits and verified completion states |

Existing shared Prometheus/Grafana Compose services are reused. No new monitoring stack and no required edits to business workers or their MetricsService. New functionality must not silently change existing application targets or dashboards.

## 13. Test plan

Unit: never-settling publish deadlines in prepare/warmup/measure/drain; cancellation; late confirmations after finalization; duplicate callbacks; exact outcome reconciliation; negative/invalid timing rejection; boundaries at t0/t1; pipeline partial errors; batch flush smaller than threshold; byte limits; histogram known distribution/merge/overflow; independent median calculations; failure sample sizes; corpus hashes; safe resource names and URLs; no arbitrary metric labels; accurate in-flight gauge.

Adapter integration on disposable benchmark resources: Kafka multi-record request evidence and acks failures; Redis pipeline per-command success/OOM/error/reconnect; Rabbit native confirm before success, publish_error, disconnect and tail flush. Verify selected SDK publisher callback scoping before multiple publishers share a connection (current implementation only has one). Capture effective settings and controlled readback outside measurement for exact identities where feasible. No business consumers involved.

Load-generator calibration: null/controlled-delay sink substantially above expected broker rate (proposed >=3x throughput headroom with matching payload/accounting path), repeat across concurrency, metrics on/off, buffer-generation on/off, and 1/2 processes. A null sink alone cannot prove SDK/network non-limitation; add profiling and producer scaling/native client sanity checks under matched settings. If a different client is used for diagnosis, label its language/runtime and resource costs separately.

Monitoring: promtool exposition/config/rule checks, escaped errors, two-scrape baseline/final state, restart/reset semantics, concurrent slot isolation, units, sample counts, unavailable target display, and exact artifact reconciliation independent of scraping.

Experimental validation: a held-out final matrix after tuning, randomized blocks, resource-pressure invalidation, no hidden retention loss, reproducible summaries from saved artifacts, and proof no protected application files/resources changed. Run required repository gates after implementation; passing the current ten tests does not establish these properties.

## 14. Implementation sequence and gates

1. **P0 — Freeze evidence and contract.** Preserve current artifacts as legacy/unbatched finite-cohort data, record hashes/source revision and missing provenance; define A/B/C profiles and result schema. Do not rename old numbers as sustainable capacity. Gate: agreed metric populations and compatibility policy, no application changes.
2. **P0 — Correct lifecycle/accounting.** Refactor runner/main/contracts, real deadlines/cancellation, warmup success, terminal invariants, partial reports, gauge/error fixes. Gate: hung/late/duplicate/error tests pass and every record reconciles exactly once.
3. **P1 — Measurement/provenance/monitoring.** Bounded histograms, effective settings, immutable artifacts, shared monitoring lifetime and resource sampler. Gate: one trial is fully reconstructable, metrics overhead quantified, no false configured-as-effective values.
4. **P1 — Scenario A baseline.** Preserve one-message mode; prove confirmation correctness and stable warmup. Gate: five valid comparable trials per initial cell with precise latency population and resources.
5. **P1 — Native throughput optimization.** Add Kafka multi-record batches, Redis pipelines, Rabbit chunks and bounded tail flushing. Gate: actual batch effectiveness observed, no lost per-record ACK/error mapping, equal byte/admission budgets.
6. **P1 — Saturation and client attribution.** Efficient scheduler, open-loop staircase, capacity/latency frontier and client profiling; optional process scaling if justified. Gate: saturation attributed or explicitly client/resource-limited; no unbounded queue and no unsupported sustainable claim.
7. **P2 — Statistical orchestration and scenario C.** Randomized blocked matrices, independent medians/ranges, controlled resources/cache, separate shard/client axes. Gate: summaries regenerate solely from compatible immutable artifacts and keep invalid runs visible.
8. **P2 — Replicated/stronger durability studies.** Only after single-node methodology passes and resources suffice. Gate: capabilities/ACK differences explicit; no forced equivalent-guarantee ranking.

Stop after each gate with a concrete report if evidence is insufficient. No phase has a Kafka-wins criterion. A sound result can show Redis or RabbitMQ ahead, a latency/throughput trade-off, or an inconclusive client/memory-limited test.

## 15. Remaining uncertainties and decisions

- Historical runs cannot be retroactively attributed to broker CPU/disk or generator bottlenecks without missing telemetry. Retain them as exploratory measurements.
- Installed SDK source confirms current call behavior, but exact wire batching under load and retransmission behavior require targeted integration evidence.
- RabbitMQ 3.13 image behavior must be checked against that actual pinned release; current official documentation may describe newer versions. Do not silently upgrade during a tuning comparison.
- Retained-history growth may prevent a long saturation run on this machine. Capacity/memory calibration determines feasible duration; shortening is disclosed, not concealed.
- Worker-process mode and alternative native clients should be introduced only with measured justification. They are not prerequisites to fixing correctness and batching.
- No user preference is needed to perform the proposed correctness refactor after approval. Larger hardware, a strict durability requirement, or changing the target from broker+Node-client to best-of-any-language engine performance would require a separate decision.

The requested deliverable is this audit and plan. No implementation is authorized or performed until the user approves it.

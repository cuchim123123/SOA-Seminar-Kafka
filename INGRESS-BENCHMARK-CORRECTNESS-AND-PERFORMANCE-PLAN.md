# Ingress benchmark correctness and performance implementation plan

Status: proposed implementation plan only. Date: 2026-11-01.

Basis: `INGRESS-BENCHMARK-CODE-CONFIGURATION-AUDIT.md`, current benchmark contracts and implementation, installed SDK behavior, and saved trial artifacts. No fixes or infrastructure changes are implemented by this document.

## 1. Objective and completion standard

Build a benchmark whose results are correct, reproducible, explainable and bounded by available resources. “Perfect” cannot mean noise-free measurements, identical broker internals, or guaranteed Kafka superiority. It means known measurement semantics, tested accounting, explicit limitations, and no valid-looking score when the experiment is invalid.

Required outputs:

1. Successful broker-acknowledged messages/second.
2. Successful payload decimal MB/second, explicitly excluding protocol overhead.
3. Comparable publish-to-ACK p50/p95/p99 and separate batch-completion latency.
4. Requested, normalized, effective and observed configuration.
5. Per-destination distribution and terminal outcome reconciliation.
6. Resource/time-series evidence sufficient to investigate a plateau or regression.
7. Independent repeated trials with retained raw results and reproducible provenance.

The implementation must preserve producer-api, outbox-publisher, loyalty-worker, analytics-worker, replay-worker, CDC, Purchase topics, application consumer groups and business logic. All events remain synthetic and all benchmark destinations remain isolated. No changes to application partition keys.

Reuse `apps/ingress-benchmark`, the existing scripts, Prometheus and Grafana. Do not introduce another benchmark service, database, message router, consumer application or general-purpose orchestration framework. A disposable container for the existing runner is acceptable for controlled Linux placement; it is an execution option, not a new persistent service.

## 2. Recommended design decisions

| Area | Decision | Why / trade-off |
|---|---|---|
| Main throughput mode | Fixed-duration, bounded-concurrency, unpaced saturation workload | Measures delivered capacity under a stated outstanding-work limit. Count mode remains a smoke/burst test. |
| Rate-limited mode | One global bounded pacer, with offered/admitted/confirmed rates distinguished | Prevents concurrency multiplying the rate. Do not call it open-loop if scheduling waits for capacity. |
| Partition policy | Explicit batch-round-robin for throughput; record-round-robin for single-record latency | Visits all destinations without fragmenting every application batch. Separate fixed-partition diagnostic. |
| Application batching | Record and payload-byte caps; no artificial runner linger for synthetic preavailable records | A sleep after filling a batch is not batching. Native transport linger is a separate setting. |
| Native Kafka controls | Explicit native linger, native batch limits, ACK, retry/idempotence and queue bounds | Avoid relying on SDK defaults or confusing payload caps with wire batch sizes. |
| Success semantics | Per-record broker acknowledgement, with unresolved outcomes explicit | Never count enqueue completion as success or timeout as proof of non-delivery. |
| Latency | Per-record adapter submission to observed ACK callback; separately batch submission to all-terminal completion | Distinguishes ACK timing from batching barrier time. Document timestamp resolution and client callback delay. |
| Baseline ACK profile | Single-node broker-acknowledged asynchronous persistence, no intentional application retry | Comparable stated scope, without claiming identical crash durability. Kafka retries explicitly zero in the strict baseline. |
| Reliability profile | Separate retry/idempotence experiment, never mixed into baseline ranking | Measures a different delivery contract and client behavior. |
| Resource profile | Retain current 2 CPU / 2 GiB profile; add an explicitly selected larger-memory profile only if sustained preflight needs it | Preserves historical configuration while avoiding Redis memory exhaustion. No silent limit changes. |
| Retention | No trimming during the primary append-only comparison | Trimming adds work and changes retention semantics. A retention-enabled benchmark is separately named. |
| Historical results | Preserve as legacy artifacts; do not rewrite or silently merge them | Current source cannot prove old native settings or routing. |
| Implementation order | Correctness → observability → small diagnostics → tuning → repeated comparison | Prevents optimizing an incorrect score. |

These are recommended design choices for the future implementation, not assertions about existing behavior. If SDK capability discovery makes a choice infeasible, document the evidence and revise that specific decision before implementing a substitute.

## 3. Work packages and dependencies

| Package | Scope | Depends on | Completion evidence |
|---|---|---|---|
| WP0 | Freeze evidence and define schema v2 | None | Baseline manifest, audit-to-test checklist, untouched legacy results |
| WP1 | Strict configuration and profiles | WP0 | Parser/mapping tests and generated normalized configuration |
| WP2 | Outcome contract and bounded lifecycle | WP1 | Deterministic boundary/failure tests; partial artifact on failure |
| WP3 | Correct routing and topology validation | WP1, WP2 | Capture tests and small broker distribution proof |
| WP4 | Adapter ACK, batching and native configuration | WP2, WP3 | SDK capability proof and per-record settlement tests |
| WP5 | Pacing, workload and memory bounds | WP2, WP4 | Rate and memory tests; consistent warmup |
| WP6 | Metrics, artifacts and resource telemetry | WP2–WP5 | Artifact/metrics reconciliation and sampling validation |
| WP7 | Safe reproducible campaign orchestration | WP1–WP6 | Small complete campaign, ownership and budget checks |
| WP8 | Kafka attribution and tuning experiments | WP7 | Controlled diagnosis with evidence and bounded search |
| WP9 | Final cross-broker validation and docs | WP8 | Repeated valid trials, qualified results, all quality gates |

Do not start a large matrix before WP7 passes. Each package should be a reviewable commit or small group of commits; avoid a single rewrite of the entire suite.

## 4. WP0 — freeze evidence and define the new result contract

Affected files: `src/contracts.ts`, `src/report.ts`, benchmark documentation and future test fixtures. Paths under `src` refer to `apps/ingress-benchmark/src`.

Tasks:

1. Record current source revision, dirty-state fingerprint, lockfile hash, Node/platform, SDK version, image IDs and hashes of benchmark source and executed JavaScript.
2. Preserve all historical JSON and the audit. Mark their schema as legacy when reading them; do not overwrite their client/config labels.
3. Define `schemaVersion: 2`, `measurementVersion`, `campaignId`, `trialId` and stable `cellId`.
4. Split `requestedConfig`, `normalizedConfig`, `clientConfig`, `observedBrokerConfig`, and `verification` fields. Every value should identify whether it is configured, read from an API, derived from a pinned SDK default, or unavailable.
5. Replace a single clean flag as the primary decision with independent fields: execution status, accounting validity, workload completion, configuration verification, resource-budget status and comparison eligibility. Keep a deprecated derived `cleanTrial` only if needed for old tooling.
6. Establish a typed finite list of invalidity reasons, such as `target_not_completed`, `unresolved_outcomes`, `unexpected_destination`, `topology_mismatch`, `resource_budget_exceeded`, `scrape_gap` and `configuration_unverified`.

Acceptance:

- Failed or incomplete runs still produce an artifact with reason codes and observed work.
- A parser never treats a legacy result as schema-v2 verified evidence.
- No single boolean implies guarantees that were not checked.

## 5. WP1 — strict configuration and honest profiles

Affected files: `src/config.ts`, `src/main.ts`, `src/contracts.ts`, `.env.example`, scripts, new config tests.

### CLI contract

Use a strict supported-option map. Reject unknown flags, missing values, duplicated conflicting flags and unsupported broker-specific options. Accept documented aliases only. Support ordinary `--flag value`; support `--flag=value` only if deliberately tested. `--help` lists units and defaults. Offer a dry-run/config command that exits before connecting to any broker.

Normalize configuration once at bootstrap; adapters receive typed values. Parameterize destinations, endpoints, credentials and relevant topic/stream settings through the benchmark configuration rather than adding environment reads inside adapters. Never print secrets in manifests.

Recommended field separation:

| Group | Fields |
|---|---|
| Workload | seed, payloadBytes, mode, count or measurementDuration, warmup/ramp durations, concurrency, globalRate, maximumOutstandingRecords/Bytes |
| Application batch | maxRecords, maxPayloadBytes |
| Routing | policy, destinationCount, optional fixedDestination |
| Lifecycle | prepareTimeout, warmupTimeout, drainTimeout, cleanupTimeout, overallTimeout |
| Kafka native | lingerMs, batchBytes, batchMaxMessages, maxInFlightRequests, acks, retries, idempotence, compression, deliveryTimeout, queue memory/messages |
| Trial operations | destinationPrefix, ownership token, resource profile, retention profile, metrics port, artifact directory, runner placement |

Validation rules:

- Count and duration modes have mutually exclusive required fields; duration must be positive.
- Reject a requested duration incompatible with overall timeout after reserving prepare, ramp, drain and cleanup budgets.
- Fixed destination must be in `[0, destinationCount)` and only supported in the intended routing policy.
- Payload size must fit the application payload-byte cap. Reject oversized payloads instead of silently bypassing the cap.
- Validate broker message/frame limits separately from payload cap; include record metadata overhead margin.
- Check maximum outstanding bytes against the runner memory budget; do not permit C × batch × payload to grow unconstrained.
- Validate Kafka idempotence/retry/max-in-flight combinations against the installed SDK. Do not apply Java producer property assumptions blindly to librdkafka.
- Rate is global records/sec; zero explicitly means saturation. Unit names distinguish bytes, decimal MB and MiB.
- Scenario A resolves to single-record/c1 settings unless an explicitly different diagnostic is selected; never label c16 as serial latency.

Remove `batchLingerMs` from the new application batching contract. Legacy nonzero usage should fail with an actionable migration message pointing to Kafka native linger; Redis/Rabbit do not silently inherit it.

Acceptance tests include every parser case reproduced in the audit, invalid partition bounds, endpoint/IPv6 parsing, zero duration, byte caps, excessive memory, option overrides and profile expansion. A mapping test captures the exact options passed to each adapter and the SDK constructor.

## 6. WP2 — correct lifecycle, accounting and windows

Affected files: `src/runner.ts`, `src/contracts.ts`, `src/main.ts`, `src/report.ts`; new lifecycle/accounting tests.

### Lifecycle

Implement explicit phases:

`prepare → warmup → warmup-drain → ramp → measure → stop-admission → drain → verify → report → close`

For sustained trials, ramp and measure share a continuous workload to avoid restarting from an empty queue at the measurement boundary. Warmup validates readiness; ramp establishes the configured outstanding-work level. Each submitted record carries an admission cohort. For count smoke tests, ramp may be disabled and exactly N measured records are admitted.

Use one monotonic clock for every local measurement. Wall time is metadata only. Inject the clock and sleeper into tests. Phase deadlines are separate from an overall watchdog. Stop admitting on the first budget breach, but continue bounded settlement collection. A supervising existing script may terminate an unresponsive child after its deadline and write a supervisor failure artifact; Promise.race alone is not process cancellation.

### Record state model

Track outstanding records by immutable identity and generation, not an ever-growing set of all completed records. Only these terminal categories are mutually exclusive:

- confirmed;
- definitively rejected/not accepted;
- unknown at finalization.

Timeout is a reason/deadline event, not automatically a definitive rejection. A timed-out record may become confirmed during the permitted drain; after final artifact closure it remains unknown and any observed later callback is logged as a post-finalization anomaly. Never double-count it.

For each admission cohort:

`admitted = confirmed + definitivelyRejected + unknown`

Confirmed records additionally split into observed-before-window, observed-in-window and observed-after-window. Keep these temporal classifications separate from outcomes. Record callback timestamps immediately when the ACK notification reaches application code.

Validate receipt identity, original payload length, destination, uniqueness and membership in outstanding work. Unknown IDs, duplicate settlement, impossible timestamps or receipt-size mismatch invalidate the trial. Bound duplicate detection using active batch/generation state rather than retaining millions of completed ordinals. Late callbacks from an old generation must never match a reused slot.

### Throughput semantics

For duration mode, fix `[t0,t1)` before measurement. Count every successful ACK observed in that interval from the continuous ramp/measurement workload; keep source cohort counters so boundary carry-in is explicit. Exclude unrelated warmup work by completing warmup drain before ramp. Denominator is exactly `t1 - t0`, never shortened to the time work stopped. If execution cannot cover the requested interval, label the result incomplete rather than reporting a shorter successful trial.

For count mode, measure first measured admission through the last terminal outcome for the target cohort. Report this as count-to-completion/burst throughput, not sustained ingress. No pacing sleep after the final completed batch.

Latency cohorts must be named. Primary duration latency is the distribution of successful ACKs observed inside the same window; additionally provide latency of the measurement-admitted cohort after drain so boundary effects are visible. Never mix these populations silently.

### Partial-batch failures

Change the existing publisher port minimally to permit per-record settlement as it happens, for example a settlement observer supplied to `publishBatch`. A single batch promise may still signal submission/lifecycle completion, but it cannot be the sole source of record outcomes. A thrown batch exception must distinguish locally unsubmitted records from submitted-but-unresolved ones.

No automatic outer retry of a whole batch after uncertain acceptance. That could duplicate already accepted records. Native retries belong to the declared delivery profile and must be exposed separately.

Acceptance tests:

- One early ACK and one late ACK in the same batch classify independently.
- ACK exactly at t1 is outside `[t0,t1)`.
- Partial success followed by exception preserves successes and marks only unresolved records unknown.
- Timeout followed by ACK during drain settles once; callback after finalization cannot change the frozen score.
- Unknown/duplicate receipt invalidates; count100 completing1 cannot pass.
- Early global deadline cannot pass a requested duration trial.
- Hung init, flush, close and metrics server shutdown each terminate within their configured envelope.
- No warmup callback leaks into measurement after warmup-drain declares success.

## 7. WP3 — routing and topology

Affected files: runner batch context, all three adapters, contracts and topology tests.

Assign a monotonically increasing application batch sequence at assembly time. Reset its phase-local origin deliberately; record the chosen policy in the manifest. Under batch-round-robin:

`destination = batchSequence mod actualDestinationCount`

Never derive batch destination from the first record ordinal. Use actual validated destination count, not an unchecked requested count. A fixed-partition diagnostic is a separate explicit policy.

Centralize the small deterministic routing function to prevent three independent variants. Keep transport-specific destination creation and IDs inside adapters. Synthetic Kafka keys can remain deterministic record IDs; explicit routing is disclosed and does not change business partitioning.

Topology validation:

- Kafka: inspect topic metadata, count, leaders, RF/ISR and relevant topic configuration; fail on mismatches. Create only owned benchmark destinations, with explicit settings.
- Redis: enumerate the intended stream keys and instance identity. State clearly that this is sharding keys in one Redis process, not a Redis Cluster.
- RabbitMQ: inspect the created stream names and availability. Describe them as independent streams. Do not label them Super Streams without implementing and validating an actual Super Stream topology; that is outside the required fix.
- Narrow create-already-exists handling; propagate authentication, permission and invalid-configuration errors.

For each run record submitted, confirmed, rejected and unknown counts by destination. Where possible record broker start/end counts or offsets as a second proof. Offset differences are not universally message counts under transactions/compaction/retries; restrict the proof to fresh owned append-only synthetic destinations with the declared semantics.

Acceptance:

- P1/3/6 with B1/3/64/128/256 all reach every destination when there are at least P batches.
- Full-batch round-robin assignment differs by no more than one batch; calculate tail-adjusted expected record counts exactly.
- Byte-limited batches and varying payload sizes cannot reintroduce skipped destinations.
- Reordering of ACKs does not change routing.
- Existing unexpected topology fails before measurement.
- A small integration trial verifies captured routes against broker evidence; cumulative historical offsets are not accepted as proof.

## 8. WP4 — adapter correctness and native configuration

### Kafka

Affected file: `src/adapters/kafka.publisher.ts` plus config and tests.

1. Pin and record the actual Confluent SDK/librdkafka versions used by the benchmark.
2. Explicitly map native producer settings. Keep application payload-byte cap distinct from native framed `batch.size` and `batch.num.messages`.
3. Baseline: acks-all, RF1, declared minISR, compression none, idempotence false, retries explicitly zero. Record the exact guarantee; this is not a replicated durable-write benchmark.
4. Obtain per-record delivery callbacks through a supported API. First perform a small capability test of the installed wrapper. If it cannot expose per-record settlement safely, use the same package's native producer API inside this adapter; preserve the publisher port and the rest of the runner. Do not patch SDK internals or emulate per-record callbacks using the final batch promise.
5. Keep many records outstanding within bounded limits. Do not await each record before submitting the next in throughput mode. c1/b1 remains intentionally serial.
6. Enable supported background delivery processing; do not introduce a coarse JS polling interval. Verify callback dispatch and shutdown behavior on Windows and Linux.
7. Collect native statistics where publicly supported: queue depth/bytes, retries, requests, records per batch/request, per-broker RTT and transmission counters. If unavailable at an API layer, mark unavailable rather than inventing an effective value.
8. Use flush only for bounded lifecycle drain. A native queue-full response is local backpressure, not a broker rejection; handle according to explicit bounded admission policy and report stall time.

Confluent documents that low-latency awaited sends should use a low native linger and that native buffering is distinct from awaiting individual sends. This supports exposing the control, not choosing a universal fastest value: [Confluent JavaScript documentation](https://docs.confluent.io/kafka-clients/javascript/1.9/overview.html).

### Redis Streams

Affected file: `src/adapters/redis.publisher.ts`.

Keep native pipelining. Bind each command result to its original record. Use supported per-command completion callbacks if they expose response processing timing; verify behavior with a controlled delayed-response mock. Do not assign the final pipeline completion time as an exact timestamp for every reply.

Disable reconnect/replay/retry in the strict baseline and disclose it. Treat connection loss after writing as unknown acceptance. Do not add WAIT/WAITAOF to only one broker's baseline: those change its contract. Record AOF enablement/fsync policy, memory policy, stream-key distribution and client buffer pressure.

### RabbitMQ Streams

Affected file: `src/adapters/rabbitmq.publisher.ts`.

Maintain unique publishing IDs and scope callbacks by connection/publisher where the API supports it. Confirm behavior with multiple publishers sharing a connection; prove no cross-publisher error can settle the wrong record. Never infer success from basicSend or flush alone.

Declare explicit chunk limits and await confirms through the common settlement path. Do not swallow flush errors: record their effect on submission and unknown outcomes. Validate publisher readiness, stream metadata and close behavior. Capture actual connection count; do not equate P publishers with P connections.

### Common latency contract

Timestamp immediately before submission to the SDK for each record; capture acknowledgement when the supported SDK callback runs. This is client-observed publish-to-ACK latency, including native batching, network, broker work and callback dispatch. It is not broker processing latency.

Additionally measure batch submission-to-last-terminal time and optional admission-to-ACK time. If a client can only provide batch-level timestamps, mark record latency unsupported and keep the cell out of a cross-broker record-latency ranking until resolved. Do not trade measurement honesty for API convenience.

## 9. WP5 — pacing, workload and bounded memory

Affected files: `src/runner.ts`, `src/workload.ts`, a small clock/pacer utility only if needed, tests.

Use one global pacer before admission. A token bucket or virtual schedule may be used, with documented maximum burst and bounded capacity. All workers draw from it. If worker capacity is exhausted, do not accumulate an unbounded backlog or later emit an unlimited catch-up burst. Report scheduler lag, admission shortfall and actual offered/admitted rates.

Keep saturation mode free of pacing timers. All waits are cancellation/deadline aware. Warmup, ramp and measurement use the same payload, routing, batch and producer settings; only timing/cohort labeling differs.

Retain deterministic payload generation with unique ordinal identity. First establish a working low-overhead implementation; then profile before optimizing. Prefer bounded reusable slot buffers only when SDK ownership guarantees show a slot can safely be reused after terminal settlement. If an unresolved native send may still reference a buffer, do not reuse it merely because the runner timed out.

Bound outstanding record state by outstanding-work limits, not total messages produced. Histograms must also have bounded memory. Replace unseeded reservoir quantiles with a deterministic bounded histogram implementation with documented relative precision and an explicit overflow count. Prefer a proven library if compatible with the workspace; do not build a complex custom quantile algorithm unnecessarily.

Full payload hashing in the hot path can distort throughput. Record a deterministic workload specification containing algorithm version, seed, payload size and ordinal range; call a sampled digest a sampled digest. Full-corpus hashes may be generated outside timing for bounded runs. Never claim a1000-record hash covers millions of records.

Acceptance:

- Global rate remains within a predeclared tolerance over a sufficiently long fake-clock test for C1/4/16/64; burst never exceeds the configured burst limit.
- Rate-limited tests reveal admission shortfall instead of hiding it.
- No post-final-work sleep affects count throughput.
- Long mocked runs have memory bounded by concurrency/batch/histogram limits.
- Reused buffers cannot corrupt an outstanding or delayed native submission.
- All brokers receive identical payload bytes for identical workload specifications.

## 10. WP6 — artifacts, Prometheus/Grafana and resource evidence

Affected files: `src/metrics-server.ts`, `src/resource-sampler.ts`, `src/histogram.ts`, `src/report.ts`, contracts, existing monitoring configuration/dashboard.

### Artifact requirements

Each trial directory should contain:

- `manifest.json`: schema, source/dist/lockfile hashes, SDK/image versions, host and placement, redacted configuration, workload specification and verification evidence.
- `result.json`: immutable final outcomes, cohorts, rates, quantiles, validity reasons and per-destination distribution.
- `samples.jsonl` or equivalent bounded-stream file: timestamped interval resource/client metrics; do not retain the entire series in process memory.
- `diagnostics.json`: error classifications, retry/queue-full counts and relevant native measurements.

Write atomically using a temporary file and rename. Persist the manifest before measurement and a failure artifact even if init or shutdown fails. Aggregate summaries reference exact trial IDs and include attempted/valid/invalid repetition counts. They must not overwrite older campaigns.

### Metrics

Use record-count units consistently for admitted/confirmed/rejected/unknown/late counters. Keep failed-operation counters separately named. Export payload bytes only for confirmed records. Expose outstanding records and bytes, phase, global admitted rate and resource guard status.

Use finite stable labels such as broker, scenario/profile and a bounded cell ID. Keep arbitrary trial IDs, payload hashes and raw errors in artifacts, not every time-series label. A metadata/info series may expose the current campaign/trial with a defined retention/cardinality policy. Prevent overlapping trials sharing one metrics endpoint.

Keep existing infrastructure. Ensure at least two successful scrape opportunities before measurement and several after completion; validate actual scrape timestamps rather than assuming a sleep guarantees coverage. Measurement duration should cover many scrapes. Grafana live `rate()` charts are interval estimates; show exact completed-trial values as separately named gauges or artifact-derived summaries, not as if a10s rate equals a subsecond JSON result.

Add dashboard panels for configuration identity, validity, destination distribution, queue depth, retry/unknown count, event-loop delay, CPU/throttling, memory and disk/network rates. Preserve existing application dashboards.

Prometheus histogram quantiles are estimates governed by bucket layout. Use a consistent unit and document differences from the offline bounded histogram; never average p99s to claim a pooled p99. A median of trial p99s is valid only when explicitly labelled that way. See [Prometheus histogram guidance](https://prometheus.io/docs/practices/histograms/).

### Resource sampling

Remove synchronous shell sampling from the measured process. Prefer an asynchronous sampler using available Docker/cgroup APIs or existing monitoring, with timestamps and bounded sampling frequency. Snapshot exact process CPU at t0/t1; record container CPU-time and throttled-time deltas over aligned boundaries where available. Report a sampled CPU value as a sample, never an average unless integrated over the interval.

Capture process RSS/heap peaks, event-loop delay with sample count/resolution, container memory, block I/O counters, network bytes and available disk space. Missing data stays null/unavailable; do not replace missing initial counters with zero. Page-cache writes may outlive ACKs; explicitly distinguish measured writeback from logical ingress.

Acceptance:

- JSON and record counters reconcile for success, mixed errors, timeouts and late confirms.
- Metrics remain parseable and have bounded label values.
- Known synthetic distributions produce quantiles within documented histogram error bounds; overflow is visible.
- Sampling-on/off paired trials quantify instrumentation overhead. Predeclare a practical threshold such as3%; if exceeded, reduce sampling or report instrumented performance separately.
- Dashboard never presents invalid trials as successful comparison points.

## 11. WP7 — reproducible orchestration and the16GB constraint

Affected files: `scripts/run-ingress-comparison.mjs`, `scripts/run-partition-comparison.mjs`, benchmark compose only, README/backlog and benchmark documentation.

Reuse common orchestration helpers where the two scripts duplicate behavior; one canonical cell specification prevents drift. Avoid building a general experiment platform.

### Preflight and ownership

1. Build once, record its hash, and execute that exact build throughout a campaign. Abort if source/config changes during the campaign.
2. Inspect endpoints and reject known application endpoints/destinations. Require an explicit benchmark prefix and ownership record for created resources.
3. Verify health, topology, runtime resource limits, image identities, memory/disk headroom and metrics readiness.
4. Record which other containers/services are running. Never stop Purchase services to improve a result.
5. Sequentially activate benchmark brokers where possible. If using an already active shared-host environment, label the campaign accordingly.
6. Fresh trial namespaces isolate evidence. Cleanup may affect only resources created and recorded by that campaign, after artifacts are durable and connections closed. Require an explicit owned-resource cleanup option; never delete historical data or use global volume pruning. If cleanup is disabled, stop when accumulated storage reaches budget.

### Memory strategy

The current Redis1GiB maxmemory cannot hold a30s run at35K/s ×1024B plus warmup and overhead. The plan must not disguise this with silent trimming.

Retain the current2CPU/2GiB profile for bounded correctness and historical comparison. Propose a separate sustained profile of2CPU/4GiB per active benchmark broker, with Redis maxmemory below its container limit and enough headroom for AOF/rewrite/transient allocations. Start with a conservative configurable ceiling, then validate it using observed memory per entry; do not assume payload bytes equal memory use. Kafka heap and Rabbit memory behavior must also be recorded in that profile.

This profile is conditional on the host budget:

`application working set + monitoring + active broker + runner + Docker/OS reserve < usable memory`

Use at least20–25% uncommitted headroom as an initial operational target, then inspect actual pressure. The16GB setting is a ceiling, not all memory available to the benchmark. If preflight cannot satisfy the budget, refuse the large run and produce a smaller explicitly labelled diagnostic.

Before sustained maximum throughput, run a bounded pilot to estimate both throughput and memory growth. Forecast warmup+ramp+measurement storage plus a safety margin. Apply an online hard byte/record/memory budget as well, since a faster tuned Kafka/Redis trial may exceed the forecast. Crossing it invalidates/truncates the trial; never quietly shorten the denominator and call it complete.

Primary objective:30s measured intervals, extending to60s only when budget allows. If the host cannot support a meaningful append-only Redis interval, report that limitation. A separately specified rolling-retention profile using Redis MAXLEN and comparable broker retention intent is possible, but measures ingress with retention maintenance and must not be mixed into append-only results. Redis documents trimming as a way to bound stream growth: [Redis Streams documentation](https://redis.io/docs/latest/develop/data-types/streams/).

### Trial order and summaries

Use randomized balanced blocks of broker/cell order where operationally feasible, recording the randomization seed. Start with three screening repetitions; use five valid repetitions for selected final cells, with a predeclared maximum attempt count. Keep every invalid attempt and reason. Do not retry until a desired result appears.

Separate cold-start and warmed trials. Do not claim independent cold-cache trials merely because a Node process restarted. Do not flush host page cache. Record retained storage/cache state and use consistent readiness/ramp rules.

Report medians, IQR, min/max and raw trials. Include per-second rates to show drift or plateaus. A stability criterion is a declared analysis rule, not a license to discard slow intervals after seeing results.

## 12. WP8 — Kafka diagnosis and measured tuning

No throughput target is promised. The desired result is a measured explanation of unused capacity, if any, and a stable configuration on the latency/throughput trade-off curve.

### Step A: explain single-record latency

Hold P1, c1, batch1, payload/seed, ACK/retry/idempotence, broker resources and placement constant. Alternate native linger0 and5ms. Repeat each condition. Record submission, native delivery/callback times where exposed, JS callback time, event-loop delay and request RTT.

Then repeat fixed-partition versus round-robin P3/P6, without changing another setting. Compare Windows-host runner versus the same built workload in a Linux container. Inspect network traces only if native stats cannot separate broker/network latency from callback dispatch. Packet capture is scoped to benchmark traffic and is diagnostic overhead, not a scored run.

Decision rules:

- If linger0 removes most delay, configuration explains that portion.
- If native delivery is prompt but JS settlement is delayed, investigate dispatch/event-loop scheduling.
- If RTT is high, investigate broker/network timing and host placement.
- If behavior varies without corresponding telemetry, report unexplained variance; do not label it an inherent client limitation.

### Step B: tune batches before high concurrency

Keep P1/c16 initially. Compare application batches64/128/256 with matching application payload caps. Start native linger at explicit0 and5ms, and a documented native batch limit large enough for the intended payload plus framing; inspect **observed** native batches rather than assuming a one-to-one mapping.

Only broaden native batch limits after identifying underfilled batches or excessive request rate. Larger buffers can increase latency/memory without improving throughput. Record records/request, bytes/request, queue delay, outstanding bytes, CPU and p99 alongside rate.

### Step C: find the concurrency knee

For the selected batch settings, test c1/4/16/64, with native max-in-flight held explicit. Plot throughput and latency against concurrency. If c64 adds latency but no throughput, prefer the lower-concurrency point. If throughput regresses, inspect CPU/GC, request size, queue stalls and throttling before changing broker threads.

Only test independent producer instances or multiple runner processes if evidence shows one client execution path is the bottleneck. Such a profile changes client resources and must be reported separately; do not hide it inside the baseline.

### Step D: validate partition benefit

Compare P1/3/6 using corrected routing and the same C/batch/resource settings. Require all destinations to receive the expected share. A single broker's partition count is not a promise of linear scaling. Inspect batch fragmentation and request aggregation as well as broker utilization.

### Step E: official Java reference

Use `kafka-producer-perf-test.sh` only against owned benchmark topics. Match broker, payload size, ACK, compression, retries/idempotence, partition count, resource envelope and network placement as closely as possible. Capture the actual CLI/version/configuration.

The tool's buffering, generated payloads, keys and duration/count methodology may differ. Document those differences rather than asserting byte-identical or scheduler-identical equivalence. Use it to test whether the broker/path can accept more than the Node client under those conditions. Run enough records to cover a meaningful interval within the storage budget; collect its detailed interval statistics.

If Java is faster, the next conclusion is “client/path/configuration headroom exists,” followed by profiling. If both plateau with corresponding broker resource evidence, a broker/resource ceiling becomes supported. Neither outcome establishes a universal Kafka-versus-Redis ranking.

## 13. WP9 — validation matrix and release gates

### Automated deterministic tests

| Category | Required cases |
|---|---|
| Configuration | Unknown/duplicate flags, missing values, mode conflicts, secret redaction, endpoint parsing, broker-specific misuse |
| Routing | P1/3/6; B1/3/64/128/256; tails; byte-limited batches; fixed partition; out-of-order confirms |
| Accounting | Mixed outcomes, duplicate/unknown IDs, partial batch throws, timeout then ACK, finalization freeze |
| Windows | ACK before/at/after t0/t1; ramp carry-in; measured carry-out; count target completion |
| Lifecycle | Hung prepare/drain/close, resource stop, signal cancellation, report-write failure |
| Pacing | Global rate independent of C, bounded burst, scheduler lag, no trailing sleep |
| Memory | Outstanding-state bound, histogram bound, buffer lifetime under timeout |
| Metrics/artifacts | Counter equality, schema validation, units, missing sample handling, label cardinality |

### Small broker integration tests

Use few records and owned destinations. Verify topology mismatch handling, actual per-destination acceptance, ACK success, unavailable destination, client disconnect after submission, and bounded cleanup. Fault injection may affect only dedicated benchmark resources. Never pause or kill the application broker.

### Performance campaign stages

| Stage | Cells | Repetitions | Purpose |
|---|---|---:|---|
| Smoke | Every broker, P1, batch1 and64, small count |1|Transport correctness, no performance ranking |
| Routing | Every broker, P1/3/6, batch256 plus tails |1 small run|Distribution proof |
| Latency attribution | Kafka c1/b1, linger0/5; placement only after first result |3 paired|Explain16–58 msg/s behavior |
| Batch screening | Kafka P1/c16, B64/128/256 |3|Choose useful batch region |
| Concurrency screening | Selected batch, c1/4/16/64 |3|Find knee and regression evidence |
| Partition screening | Selected settings, P1/3/6 |3|Measure corrected scaling |
| Final controlled comparison | All brokers under common application/resource profile |5 valid|Fair reported comparison |
| Best-tuned comparison | Per-broker chosen settings, same resource/ACK scope |5 valid|Separate capability comparison with different tuning disclosed |
| Java diagnostic | Selected Kafka cells, matching placement/resources |3|Client-versus-broker attribution |

Do not run all dimensions as one Cartesian product. Narrow using screening, then confirm the selected configurations. The controlled common-configuration comparison and best-tuned comparison must be different tables.

### Quality gates

For future implementation milestones, run required repository checks: `pnpm format:check`, `pnpm lint`, `pnpm typecheck`, `pnpm test:unit`, benchmark build, and `pnpm kafka:verify` when applicable Kafka infrastructure is active. Investigate failures rather than altering application features to accommodate the benchmark. Update README and the actual repository backlog under `kafka-streaming-agent-spec/docs/11-agent-backlog.md`, recording which validations ran.

Final acceptance checklist:

- Every comparison point completed its declared workload/window and has no unexplained outcome.
- All required destinations were used and actual topology matches the manifest.
- Native Kafka settings are explicit and verified to the extent supported; unavailable observations are labelled.
- Latency semantics are comparable and batch/record metrics are separate.
- No current application feature or business pipeline changed.
- No unrelated resource was deleted or reset.
- All raw trials, invalid attempts, configs, versions and provenance are retained.
- Resource budgets passed, memory is bounded, and duration is long enough to support the stated conclusion.
- Prometheus/Grafana show the same experiment identity and correctly labelled live/final metrics.
- Performance claims identify tested conditions, variability and remaining limitations.

## 14. Audit finding coverage

| Audit finding | Planned resolution |
|---|---|
| F01 skipped destinations | WP3 routing sequence and distribution validation |
| F02 false clean/completion | WP0 status model; WP2 workload/window checks |
| F03 batch-time window classification | WP2 timestamp/cohort accounting; WP4 per-record settlement |
| F04 partial failures/unknown outcomes | WP2 state machine; WP4 callbacks |
| F05 per-worker rate multiplier | WP5 global pacer |
| F06 misleading linger | WP1 remove artificial application linger; WP4 native controls |
| F07 false effective config | WP1 profiles; WP4 explicit options; WP6 provenance |
| F08 byte-cap exception/warmup mismatch | WP1 rejection; WP5 shared workload path |
| F09 ignored CLI arguments | WP1 strict parsing |
| F10 burst methodology | WP2 duration model; WP7 budgets; WP9 repeated trials |
| F11 scrape/label mismatch | WP6 metrics lifecycle and dashboard |
| F12 resource sample mislabelling | WP6 aligned deltas and missing-data semantics |
| F13 allocation/state growth | WP5 bounded memory and evidence-led optimization |
| F14 weak provenance/hash claims | WP0 schema; WP5 workload specification; WP6 artifact evidence |
| F15 metric error-unit mismatch | WP6 record/operation counter separation |
| F16 unbounded setup/cleanup | WP2 lifecycle; WP4 adapter errors; WP7 supervisor |

## 15. Recommended first implementation slice

Start with WP0–WP2 and mocked tests only: strict configuration, new validity/outcome semantics and reliable lifecycle. Follow with WP3 routing and a tiny isolated distribution test. These changes remove the most serious false conclusions without requiring a full producer rewrite or costly matrix.

Then implement the per-record SDK capability proof before committing to adapter changes. Only after accounting and adapter semantics pass should telemetry, sustained orchestration and tuning proceed. This sequence minimizes wasted work and gives each milestone a concrete, reviewable result.

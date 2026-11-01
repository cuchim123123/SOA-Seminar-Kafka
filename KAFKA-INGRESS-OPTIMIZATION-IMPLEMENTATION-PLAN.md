# Kafka ingress optimization: implementation plan

2026-11-01 — proposed work only. No implementation performed.

## Objective

Remove avoidable benchmark/client limits so Kafka can demonstrate its available ingress capacity under explicit CPU, memory, latency and acknowledgement constraints. Do not target a predetermined winner or promise 93K msg/s: the Java diagnostic was not a matched comparison or a sustained-capacity result.

Evidence: `KAFKA-INGRESS-ROOT-CAUSE-INVESTIGATION.md`. Current KafkaJS multi-record batching works, but byte/linger/rate controls are not applied, timing is count-to-completion, and resource attribution is insufficient. Longer and alternative-client diagnostics show the original 21K rate is not a hard broker ceiling.

## Scope protection

Only the standalone ingress benchmark, its tests, dedicated benchmark profiles, reports, and additive shared monitoring panels are eligible. Keep producer-api, outbox publication, loyalty-worker, analytics-worker, replay-worker, Purchase contracts/databases/topics/groups and the application deployment unchanged. Preserve Redis/RabbitMQ benchmark support and apply equal correctness standards to all adapters. Never trade away acknowledgements to inflate Kafka throughput.

Read AGENTS.md before implementation. Preserve existing dirty work. All configuration changes below are proposals for later approval, not changes made by this plan.

## Recommended order

1. Correct measurement and outcome accounting.
2. Implement effective batching and bounded request concurrency.
3. Measure client/broker resources during the window.
4. Tune KafkaJS systematically on one partition.
5. Match placement against the official Java reference.
6. Escalate to process scaling, native client or partitions only if evidence warrants it.

Do not begin by increasing partitions, CPU quotas or disabling ACKs. Those alter the experiment before establishing which current constraint matters.

## Phase 1 — Make results valid

Affected: `apps/ingress-benchmark/src/{config,contracts,runner,main,report}.ts` and tests.

- Add duration-based warmup and measurement as distinct options from legacy count-based burst mode. Preserve old artifacts and identify them as finite-cohort results.
- Define monotonic start/end boundaries. Stop admission at the end, drain pending operations to a separate deadline, then freeze results.
- Publish both fixed-window confirmed throughput and the final outcome of records admitted during measurement. Mark boundary and late confirmations explicitly; do not silently change denominator.
- Validate every batch receipt against expected ordinals: no missing, duplicate or unrelated results. Each admitted record ends once as confirmed, rejected or ambiguous. Unsent work is not a broker timeout.
- Bound connect, metadata verification, warmup, publish, flush and close. Timer expiry must prevent late callbacks changing finalized statistics; close/cancel pending transport work.
- Gate a valid trial on successful warmup, full accounting, supported settings and sufficient samples.
- Reject unknown/unused CLI options. An accepted `rate`, byte cap or linger option must have tested behavior.

Acceptance: deterministic fake-clock tests for boundary ACKs, incomplete receipts, hung operations, late callbacks and partial failures. No result can be labeled sustained throughput merely because all 5,000 messages finished.

## Phase 2 — Give Kafka an effective batching policy

Affected: `src/adapters/kafka.publisher.ts`, runner/config/contracts; add a small batching helper only if needed for cohesive ownership.

KafkaJS does not provide the Java producer's cross-call accumulator here. Implement application-side per-partition accumulation, using the existing KafkaJS `send({ messages })` path.

Flush when any threshold is reached:

1. Maximum records.
2. Maximum canonical payload bytes, with separately reported estimated/actual request bytes and a safety margin under broker request limits.
3. Maximum age of the oldest admitted record.
4. Measurement end, explicit drain or shutdown.

Retain a no-intentional-batching latency profile. For saturation, queues often fill before linger expires; at low rates, linger bounds intentional delay. Include queue/linger time in primary admission-to-confirm latency. Expose dispatch-to-confirm separately. Do not divide batch latency by batch size.

Maintain separate bounds for queued bytes, admitted records, batches awaiting ACK and Kafka native requests. Flush a partial batch even when the admission limit is smaller than the target batch size: otherwise a credit/batch deadlock is possible. Timer precision under event-loop stalls must be measured, not assumed.

Make Kafka `maxInFlightRequests` configurable and record it. A batch slot count of 16 with 256 records is 4,096 potentially outstanding records; it is not equivalent to 16 single-message requests. Avoid a permanently full, enormous queue merely to keep the broker busy.

Initial tuning candidates, not new defaults:

| Parameter | Candidate values |
| --- | --- |
| Payload batch target | 16 / 64 / 256 KiB |
| Record cap | 16 / 64 / 256 / 512 |
| Maximum batch age | 0 / 1 / 5 / 10 ms |
| Outstanding batch/request budget | 4 / 8 / 16 / 32; 64 only as a saturation probe |
| Global outstanding payload budget | 1 / 4 / 8 MiB |
| Payload size | 256 B / 1 KiB / 4 KiB |

Screen a small subset first; do not run the Cartesian product. Start near 64–256 KiB and 8–16 request slots because diagnostics justify investigating that range, not because an optimum is known.

Acceptance: adapter tests and real request instrumentation demonstrate filled multi-record batches, bounded tail flush, effective byte/age limits, no ACK before server confirmation, and stable memory when offered load exceeds capacity. Record actual records/request, bytes/request and request rate.

## Phase 3 — Remove avoidable measurement/client overhead

Affected: `src/{workload,histogram,metrics-server,resource-sampler,runner}.ts`.

- Retain efficient worker/completion scheduling; avoid whole-set Promise.race loops, per-event logging and unnecessary Promise/receipt allocations.
- Replace full-run preallocation with a bounded immutable corpus/pool where long duration requires it. Never reuse a mutable Buffer before the client is done. Keep generation outside the measured admission path where possible and measure remaining generation time.
- Correct histogram merging: bounded reservoirs cannot be concatenated and called population-weighted. Use a tested mergeable representation, document precision, and expose sample/overflow counts.
- Use incremental metrics and finite labels. Benchmark metrics enabled/disabled to quantify overhead.
- Sample CPU, event-loop utilization/delay, GC, RSS and heap throughout measurement. Record event-loop sample count; absent observations are unavailable, not zero delay.
- Sample broker cgroup CPU/throttling, memory/events, block I/O and network asynchronously. Capture timestamps and deltas; stop using a single post-run docker stats snapshot as saturation evidence.
- Profile only the measured phase in diagnostic mode to distinguish KafkaJS CRC32C/encoding from startup and corpus hashing. Keep profiler runs out of headline rankings.

Acceptance: resource traces align with measurement timestamps; calibration shows enough generator headroom or labels client limitation; telemetry overhead is quantified; no growing buffers or invalid histogram merges.

## Phase 4 — Controlled KafkaJS tuning

Affected: `scripts/run-ingress-comparison.mjs`, `benchmark/ingress/profiles/`, report aggregation and ingress Grafana dashboard.

First hold broker resources, one partition, RF1, acksall, compression none, payload and storage class fixed. Do not change the business Kafka broker.

Protocol:

1. Preflight actual Docker allocation, background load, disk space, effective topic/configuration and client versions.
2. Calibrate a safe retained-data budget. Physical RAM is 16 GB but Docker previously exposed only ~7.44 GiB. Redis comparability requires respecting retained in-memory data, not trimming only Redis.
3. Warm up for a bounded time and inspect interval stability; use longer trials only where resource budgets permit. If only bursts fit, report bursts.
4. Conduct coarse bounded closed-loop tuning, then validate candidate capacity with a real offered-rate staircase.
5. Record offered/admitted/confirmed rate, scheduler lag, queued bytes, ACK latency, failures and resources. A growing client queue or missing scheduled work fails sustainability.
6. Randomize matched blocks and run at least five independent final repetitions on configurations selected in earlier screening. Do not choose one best outlier.

Select a throughput/latency frontier: the highest repeatable confirmed rate satisfying the experiment's declared latency/error/memory budget, plus a lower-latency operating point. No single universal concurrency value is expected.

Acceptance: byte/linger/concurrency changes have explainable measured effects; c16/c64 differences are tested with equal outstanding-byte alternatives; the final result is not extrapolated from a 0.2-second burst.

## Phase 5 — Separate broker capability from KafkaJS capability

Keep the official Java performance tool as a reference, not a drop-in score substitution. Match runner network location, CPU/RAM budget, payload size/content class, compression, ACK/idempotency/retries, partition count and outstanding buffering as closely as the clients permit. Record remaining differences explicitly.

Run Node and Java sequentially from the same Docker network where feasible, using benchmark-only run configuration. The earlier Windows Node versus Linux Java comparison was sufficient to disprove a 21K broker ceiling, but insufficient to attribute the entire difference to KafkaJS.

Use observed Java batch-size/request metrics and producer queue time. Compare full publish completion latency with full completion latency; request latency alone excludes producer queueing.

Decision gates:

- If optimized KafkaJS approaches the matched reference under the chosen latency budget, retain KafkaJS.
- If Node process CPU/event-loop saturates while broker capacity remains, test two producer processes under an explicit total resource budget. Merge counts/histograms and bound global bytes; do not average worker percentiles.
- If KafkaJS encoding/CRC remains a material ceiling, evaluate an optional maintained native Kafka client in the standalone benchmark. Pin and validate its Node/platform support and ACK/retry semantics at implementation time. Preserve the KafkaJS mode for reproducibility.
- If both clients saturate the broker similarly, investigate broker-native limits rather than adding client complexity.

Acceptance: any client/process migration is justified by matched evidence, not an assumed Kafka advantage. No native dependency is added to business services.

## Phase 6 — Broker and partition scaling, only after attribution

Investigate one variable at a time on benchmark-only infrastructure:

- CPU quota/throttling: compare profiles with fixed clients and documented quota changes; equalize resources in competitor comparisons.
- Heap/page cache: use actual GC/memory pressure. Larger JVM heap can reduce page-cache space and worsen ingress; do not maximize heap automatically.
- Network/request/I/O threads: change only if request timing/idle metrics identify a constraint. More threads can add scheduling overhead.
- Partitions: retain partition1 baseline, add 3/6 partitions as a distinct sharded profile with actual distribution. Current explicit partition0 must not silently remain in that experiment. Separate per-partition ordering from global order.
- Compression: optional separate profile with representative payload entropy; account for producer CPU and distinguish application bytes from wire bytes. Do not compare compressed Kafka against uncompressed competitors without labeling the profile.

Acceptance: scaling gains survive repeated trials and disclose additional resources/semantic changes. No claim that more partitions fix the present client overhead without evidence.

## Configuration and acknowledgement rules

Keep acksall, no hidden retries in the baseline, and explicit persistence policy. Enabling Kafka idempotence changes retry/in-flight behavior and must be a separate documented reliability profile, not a silent tuning change. Never use acks0 as a substitute for confirmed ingress.

Single-node async persistence and replicated profiles remain separate. Redis and RabbitMQ get their appropriate pipeline/chunk optimizations and the same tuning effort, resource envelope and external success criteria. Internal batch mechanics need not be identical, but differences must be recorded.

## Deliverables and stop conditions

Each implementation milestone delivers exact changed files, tests, effective configuration, raw artifacts and acceptance evidence. Run repository quality gates and scoped broker tests; do not run destructive application reset scripts.

Final outputs:

- Corrected benchmark with effective controls and verified ACK accounting.
- Versioned low-latency and high-throughput Kafka profiles, with measured trade-offs.
- Shared Grafana panels for throughput, latency, actual batches, queueing and bottlenecks.
- Reproducible matched KafkaJS/reference results and fair cross-broker matrix.
- A documented conclusion: broker-limited, client-limited, memory-limited or inconclusive, with evidence.

Do not declare success simply because Kafka exceeds 35K or beats a prior competitor score. Success is exposing available capability reliably under declared constraints. Stop and report limitations if the host cannot sustain a valid experiment.

## First implementation slice

Implement Phase 1 plus tests, then the batching policy in Phase 2. Preserve existing adapters and interfaces where practical. Do not begin native-client replacement, partition scaling or broker reconfiguration until those gates and measurement-time telemetry pass. This is the smallest useful path from the current code to defensible optimization.

# Ingress benchmark: code and configuration audit

Audit date: 2026-11-01. Scope: the standalone synthetic ingress benchmark only. No application, benchmark implementation, or infrastructure configuration was changed. No benchmark matrix was rerun, no broker data was deleted, and no topics were reset. The diagnostics below use in-memory mocks and read-only inspection. This document is the audit deliverable, not an implementation.

## Verdict

**The current results are not sufficient to rank sustained broker capacity or partition scaling.** They are useful records of specific short client-to-broker runs. Several implementation defects invalidate more specific interpretations: six configured destinations do not mean six used destinations, `cleanTrial` does not establish workload completion, and configured application batching is not effective native Kafka batching.

The 34,977 / 21,959 / 24,950 Kafka figures exist in the saved artifacts. Their measurement windows are only **0.2859 / 0.4554 / 0.4008 seconds**. Kafka's current producer is Confluent's KafkaJS-compatible wrapper over librdkafka, not KafkaJS itself. Previous KafkaJS CPU profiles cannot establish the bottleneck in this implementation.

No evidence here establishes that Kafka should beat Redis or RabbitMQ. Correctness and provenance must be repaired before throughput tuning or a broker ranking.

### Evidence labels

- **CONFIRMED BUG:** directly demonstrated in current code, with an isolated reproduction where specified.
- **VERIFIED BEHAVIOR:** established by inspected source, artifacts, or read-only runtime inspection; not necessarily a defect.
- **SUSPECTED ISSUE:** plausible mechanism whose contribution requires a controlled experiment.
- **NOT YET VERIFIED:** missing runtime evidence; no causal conclusion is warranted.

References below use repository-relative paths and exact source lines as inspected. Repository root: `E:\SOA-Seminar-Kafka`. Source is a dirty working tree with substantial pre-existing changes; no commit can currently be asserted to identify historical runs. SDK references are under this repository's installed `node_modules`.

## A. Execution flow

| Stage | Source/function | Actual behavior |
|---|---|---|
| Matrix entry | `scripts/run-ingress-comparison.mjs:124–176`, `runTrial` | Launches compiled `dist/src/main.js`; supplies count, warmup, records/bytes, concurrency and seed. Five repetitions per cell, sequential broker lifecycle. Does not build or fingerprint the executable. |
| Partition entry | `scripts/run-partition-comparison.mjs:34–85,89–160`, `runTrial` and `main` | Launches the same compiled entry; one run per partition/broker/scenario combination. Does not isolate broker lifecycles. The output summary omits trial IDs. |
| Parse | `apps/ingress-benchmark/src/config.ts:36–85`, `parseCliArgs` | Converts flag/value pairs and selected kebab-case aliases, then validates through Zod. Unknown fields disappear. |
| Compose | `apps/ingress-benchmark/src/main.ts:16–114`, `main` | Starts optional metrics HTTP server, constructs exactly one broker adapter and workload generator, passes runner options. Metrics bind failure warns and proceeds. |
| Prepare | `src/runner.ts:100–119`, `BenchmarkRunner.run` | Connects publisher, runs warmup, drains. Initialization is outside the measurement window but has no runner timeout. Warmup errors can be swallowed. |
| Measure | `src/runner.ts:120–129,232–427`, `run` / `executeMeasurement` | Starts resource sampler, starts timing, runs concurrent worker slots. Each assembles a batch synchronously, awaits its publish, accounts receipts, optionally sleeps for rate control, then repeats. |
| Kafka ACK | `src/adapters/kafka.publisher.ts:98–171`, `publish` / `publishBatch` | Awaits `producer.send`. Installed Confluent `_producer.js:970–991` submits each native record and awaits delivery-report promises. Success is not merely enqueue success. All receipts for a multi-record call share its final completion time. |
| Redis ACK | `src/adapters/redis.publisher.ts:74–135` | Single `XADD` reply or entire `pipeline.exec()` completion; per-command errors inspected. Batch receipts share the pipeline completion timestamp. |
| Rabbit ACK | `src/adapters/rabbitmq.publisher.ts:84–114,127–184` | Resolves pending publishing IDs on native publish-confirm callbacks; batch method explicitly flushes and waits for all per-record promises. Receipt timestamps are individual callback times. |
| Finish | `src/runner.ts:129–224` | Stops sampler, performs final drain, computes rates/histogram/reconciliation and manifest. Late confirmations are separate from in-window throughput. |
| Export | `src/report.ts:5–18`, `saveTrialResult`; `main.ts:116–145` | Writes manifest/result JSON, prints report, optionally leaves metrics up, then closes publisher/server. Close is not protected by a global deadline. |

Here and below `src/...` means `apps/ingress-benchmark/src/...` unless another prefix is given.

**Concurrency is a worker-slot limit, not a connection count, thread count, or measured native request count.** A worker waits once per application batch. At c=16/b=256 the nominal outstanding payload limit is 4,096 records, but byte caps, partial batches and SDK buffering affect actual occupancy.

## B. Variable audit matrix

Defaults are schema defaults; historical runs must use their own manifests. “Effective” below distinguishes application values from inferred SDK defaults and observed runtime state.

| Variable / definition | Read and application point | Effective behavior / discrepancy |
|---|---|---|
| `broker`, `profile`, `scenario`, config:4–8 | main:35–69; runner constructor and manifest | Broker chooses adapter. Only `single-node-async` accepted. Scenario labels and batch default change; scenario A does **not** force c=1. Default concurrency is 16. Profile does not validate persistence settings. |
| `partitions=1`, config:20 | main:42,48,63; all adapter constructors | Kafka creates P partitions if topic absent, Redis uses P keys, Rabbit creates P independent streams. Batch routing uses first ordinal modulo P; see D. Kafka metadata is collected, but existing topology is not enforced. |
| `partition`, config:19 | main:41; Kafka publish:101–105,142–147 | Kafka-only explicit override. Overrides all batch routing. Parser accepts partition 9 with P=3; no cross-validation. Redis/Rabbit ignore this Kafka-specific option. |
| `batchRecords=1`, config:15,78–85 | runner:278–305; Rabbit constructor/main:62 | Scenario B defaults to 64 if unspecified. Caps application records, not native Kafka batch records. Rabbit uses it as max chunk length. |
| `batchBytes=65536`, config:16, aliases:64 | runner:293–304 | Caps aggregate **payload** bytes after the first record; excludes keys/protocol overhead. First oversized record passes. Warmup does not enforce this cap. Does not configure librdkafka `batch.size`. |
| `batchLingerMs=0`, config:17,65 | runner:309–311 | Sleeps only after a short batch has already been assembled. Does not accumulate arrivals and does not configure Kafka `linger.ms`. Byte-limited short batches also sleep. Delay is outside adapter latency. `--lingerMs` silently ignored. |
| `maxInflightRequests=64`, config:18,66 | main:40; Kafka constructor:43–49 | Confluent maps `kafkaJS.maxInFlightRequests` to native `max.in.flight` (`_producer.js:239–240`). Only Kafka uses it. It is not equivalent to C or C×batchRecords. |
| `concurrency=16`, config:12 | runner:275–409 | C asynchronous application workers sharing adapter/SDK state. No independent worker threads. No observed native in-flight request telemetry. |
| `payloadSize=1024`, config:11 | main workload construction; workload:23–49 | Buffer payload exactly this length. First eight bytes encode ordinal. Kafka adds record key `bench-rec-N`; Redis adds stream command/field/ID framing; Rabbit adds stream framing. MB/s is payload MB/s, not wire/disk MB/s. |
| `count=5000`, config:13 | runner:266–272 | In count mode target measured admissions; warmup starts measured ordinals at W. Hard deadline can stop before target; clean does not require target completion. |
| `warmup=500`, config:14 | runner:110–118,450 onward | Sequential application batches, capped by a warmup deadline. Excluded from throughput/histogram. Byte cap, rate and linger differ from measurement. Measured ordinal starts at configured W even if fewer warmup messages succeeded. Outstanding timed-out work is not cancelled. |
| `seed=42`, config:31 | main → WorkloadGenerator; workload:7–39 | Deterministic template and ordinal payload generation. General matrix uses 42+repetition; partition script uses 42. Seed itself is absent from manifest. Histogram reservoir uses unseeded `Math.random`, a different source of variability. |
| `mode=count`, `durationSeconds=0`, config:9–10,73–76 | runner constructor:79–86; executeMeasurement:261–272 | Positive duration overrides CLI mode to duration. `--duration` silently ignored; supported alias is `--duration-seconds`. Explicit duration mode with zero duration falls back to count behavior while retaining duration label. |
| `runTimeoutMs=60000`, config:21 | runner:264,324 onward | Admission/operation deadline, not a total process deadline. Doesn't cancel SDK operation; pacing sleep, initialization, final close and some drains can exceed it. Requested duration can exceed run timeout. |
| `drainTimeoutMs=10000`, config:22 | runner warmup/final drain; adapters | Kafka/Redis drain are no-ops. Rabbit loops over flushes until deadline, but an awaited flush can outlive it. Not an end-to-end cancellation guarantee. |
| `rate=0`, config:24 | runner:395–400 | Sleeps N/rate after **each worker's** publish. Aggregate offered rate can approach C×rate; ACK time and timer granularity reduce it. Not a global rate limiter. Final count-mode sleep contributes to elapsed throughput. |
| ACK setting | Kafka constructor:43–49; Redis publish; Rabbit confirm callback | No ACK CLI option. Kafka configured `acks=-1`, idempotence false. Redis uses XADD replies, no WAIT/WAITAOF. Rabbit uses stream confirms. No crash-durability equivalence established by the shared profile name. |
| Retry | Kafka settings:193; installed `_producer.js:259–261`; Redis constructor:31–37 | Manifest claims Kafka retries 0, actual wrapper default 5 because no override supplied. Redis disables retry/resend/reconnect. Rabbit ACK/error path differs; no comparable explicit retry policy in manifest. |
| Native linger / batch size | Kafka constructor:35–50; installed librdkafka `CONFIGURATION.md:166–176` | No override: installed source defaults linger 5 ms, batch size 1,000,000 bytes, max batch messages 10,000. These are source-derived defaults, not a captured live config dump. Explicit partition selection bypasses native sticky destination selection. |
| Native compression/idempotence | Kafka constructor/settings | Idempotence explicitly false, compression not overridden and reported none. No compression CLI. Tuning compression changes CPU/wire work; keep a separate experiment. |
| Native queue limits | librdkafka configuration table | Finite SDK queue/message limits remain at defaults; no queue occupancy/full-error telemetry. Worker limits bound normal application submissions but do not describe native memory/backpressure. |
| Flush | Kafka drain:173; Rabbit batch:180/drain:187; installed Rabbit publisher:132,215–241 | Kafka does not flush per batch; send waits for delivery. Rabbit explicitly flushes inside the measured batch, and SDK auto-flushes chunks / schedules partial chunks. Flush means sending a chunk, not independently establishing ACK. |
| Broker endpoint env | config:25–27 | `BENCHMARK_KAFKA_BROKER`, `BENCHMARK_REDIS_URL`, `BENCHMARK_RABBITMQ_URL`, with CLI override. Kafka string becomes one-element broker array, not comma-list parsing. Rabbit main:51–58 uses simple colon split rather than URL parsing; credentials/IPv6/custom host without port not handled robustly. |
| Metrics port/enabled/drain | config:23,28–29, aliases; main:19–32,127–145 | Defaults 3006/true/5s. Both scripts force drain 0. Metrics failure does not invalidate a trial. No run/partition/batch/concurrency identity in metric labels. |
| Container name | config:30; ResourceSampler | Optional, selects docker stats target. Does not establish benchmark isolation or guarantee sampled container corresponds to endpoint. |
| Destination, credentials, retention | adapter constructors and compose | Defaults are hardcoded rather than broadly CLI/env configurable. Persistent destinations reused; no per-trial retention/topology validation. Rabbit guest/vhost defaults and P stream names are adapter decisions. |

### Native producer and ACK verification

Installed package `@confluentinc/kafka-javascript/package.json:2–6` identifies version **1.11.0**, librdkafka **2.16.0**. Kafka adapter imports `KafkaJS` from that package at line 2. The package named `kafkajs` remaining in dependencies does not mean this adapter uses it.

Installed `_producer.js:433–434` calls `setPollInBackground(true)` and installs the delivery-report callback. Lines 970–991 preserve an explicit partition, call native produce, and `await Promise.all(msgPromises)`. This disproves an assumption that successful `send` only means local queue admission, and does not support blaming an arbitrary 100 ms JavaScript polling interval. The installed low-level examples using `setPollInterval` are not this execution path.

Native batching can merge or split application submissions within native constraints. `avgBatchSize=250` for 10,000 records and forty application calls establishes application batch size only. It does not prove forty Produce requests or 250 records per wire batch.

## C. Confirmed bugs and defects, ranked

### P0 — invalidates affected benchmark claims

**F01 — CONFIRMED BUG: modular batch routing skips destinations.**

Evidence: Kafka `publishBatch`, `src/adapters/kafka.publisher.ts:142–154`, Redis equivalent `redis.publisher.ts:102–105`, Rabbit `rabbitmq.publisher.ts:159–175`: destination comes from `records[0].ordinal % this.numPartitions`. Runner `executeMeasurement`, `runner.ts:287–305`, increments ordinal for every record, so successive full-batch starts advance by B, not one. Six destinations with B=64/128/256 use only three. In-memory Kafka adapter calls reproduce this exactly (D). Invalidates six-way utilization claims, not every measured message/second counter.

**F02 — CONFIRMED BUG: clean does not mean requested workload completed.**

Evidence: `runner.ts:158–168`, `run`: `isReconciled && errorCount === 0 ... && windowConfirmedCount > 0`; no targetCount or requested-duration check. `executeMeasurement:266–272` stops admission at hard deadline. Isolated actual-runner mock: requested 100 records, rate1, timeout10ms admitted/confirmed **1**, elapsed **1.0128s**, `clean=true`. This uses a short internal runner timeout for a fast reproduction; CLI's minimum timeout does not change the logic. Long duration/count runs can be falsely labelled complete. A rate sleep is not bounded by the deadline.

**F03 — CONFIRMED BUG: duration-window ACK classification uses batch-return time.**

Evidence: `runner.ts:344–365`: `now = performance.now()` followed by one `inWindow` decision for all receipts; does not read `receipt.confirmationMonotonicMs`. Rabbit supplies individual callback timestamps (`rabbitmq.publisher.ts:84–110`) but a single late receipt holds up `Promise.all`. Earlier timely receipts are then classified late. In count mode this particular window-boundary issue is inactive. Kafka/Redis already discard earlier individual timings at the adapter boundary, so their receipt contract also limits event-level analysis.

**F04 — CONFIRMED BUG: terminal accounting can hide partial outcomes.**

Evidence: `runner.ts:347–389`: receipts are checked against `settledOrdinals`, but not membership in the currently submitted/admitted set; thrown batch operations classify all original records as error/timeout. Confluent `_producer.js:980–991` uses Promise.all over native produce/delivery promises: one failure can reject while other records have succeeded or remain pending. A timeout races a promise rather than cancelling it (`runner.ts:38–52`). Consequently the error/timeout categories do not establish broker non-acceptance, and late native successes cannot be reconciled after the catch. Existing clean success runs are not thereby proven to contain failed messages; this is a correctness defect in failure handling and duration accounting.

### P1 — significantly distorts configuration or performance interpretation

**F05 — CONFIRMED BUG: rate is per worker, not global.**

`runner.ts:395–400`, `runWorkerSlot`: `expectedMs = recordsSlice.length / targetRate * 1000`; each worker sleeps independently after publishing. Mock actual-runner tests at requested rate100 and count100: c1 **61.97 msg/s**, c4 **240.91 msg/s**. Windows timer scheduling affects absolute values, but c4 exceeding the configured rate proves the cap is not global. ACK time additionally reduces rate; it is not a fixed offered-load generator.

**F06 — CONFIRMED BUG: application linger is not Kafka linger.**

`config.ts:17,65`, `runner.ts:309–311`, Kafka constructor:35–50. Supporting code: `if (maxLingerMs > 0 && recordsSlice.length < maxRecords) await sleep(...)`. Full batches never linger; short batches wait without further accumulation. Native linger remains independently defaulted. A run reporting maxLingerMs0 is not evidence of native linger0. Extra runner delay is excluded from publish latency because adapter timer starts afterward.

**F07 — CONFIRMED BUG: “effective” Kafka retry count is false; topology settings partly assumed.**

Kafka `getEffectiveSettings:183–197` returns `producerRetries: 0`, `minIsr:1` and RF1 constants. Installed `_producer.js:259–261`: `rdKafkaConfig['retries'] = retries ?? 5`. Metadata establishes observed replica/ISR membership, but not `min.insync.replicas` configuration. Existing topics are not required to match requested topology. Compare configured, source-defaulted, and broker-observed fields separately.

**F08 — CONFIRMED BUG: byte budget has an undocumented oversized-record exception; warmup ignores it.**

`runner.ts:293–298` tests the limit only when `recordsSlice.length > 0`. Mock actual-runner test with payload2048, maxBytes1024, maxRecords2 sent ten 2048-byte singles and reported clean. `executeWarmup:450 onward` batches by records only. Ordinary 1KiB/256KiB runs do satisfy the payload budget; don't attribute their ranking to this edge case.

**F09 — CONFIRMED BUG: CLI silently accepts ignored/misleading settings.**

`config.ts:36–85`, `parseCliArgs`, lacks unknown-key rejection and cross-field validation. Direct parser checks: `--lingerMs 25` returns linger0; `--duration 10` returns count/duration0; `--mode duration` returns duration/0; `--partition 9 --partitions 3` succeeds. A typo can produce a plausible artifact for a different experiment.

**F10 — VERIFIED BEHAVIOR / methodology defect: subsecond runs and one-shot scaling cells.**

The artifact evidence in F below establishes durations. `scripts/run-partition-comparison.mjs:95–144`, `main`, loops P/broker/cell once with count10000 and warmup1000 for throughput. No repetitions/randomization or source fingerprint. Forty batches at c16 means only approximately 2.5 waves of work. Queue fill, drain and startup state can materially affect such a short window; sustained capacity is not established. No specific percentage of bias is claimed.

**F11 — CONFIRMED BUG: monitoring cannot faithfully represent the short matrix trials.**

Both scripts set `--metrics-drain-seconds 0` (general script:144–145); Prometheus job `docker/monitoring/prometheus/prometheus.yml:46–50` scrapes every2s. Throughput runs end before a scrape interval. Grafana `ingress-benchmark.json:52,76,107–117` applies 10s rates and 30s histogram windows; these are not the JSON subsecond measurement windows. `metrics-server.ts:116` labels only broker/profile/scenario, so distinct partition/batch/concurrency runs share series. Keeping an endpoint up afterward alone does not reconstruct the original burst throughput.

### P2 — accuracy, observability and reliability gaps

**F12 — CONFIRMED BUG: container CPU report calls an endpoint sample an average.** `resource-sampler.ts:48–60` returns finalSample.cpuPercent; `report.ts:78` prints “Container Avg CPU”. Initial stats occur before t0, final stats after t1. Process CPU collection also starts before initial synchronous stats. This makes CPU/elapsed division unreliable for these short windows. Missing initial block-write sample becomes zero, potentially presenting lifetime writes as a trial delta. Zero writes can also mean deferred page-cache writeback, not no storage work.

**F13 — VERIFIED BEHAVIOR: hot-path allocation and duration memory growth remain.** `workload.ts:42–49` copies buffer and writes BigInt ordinal for every record; `runner.ts:291` calls it inside timing despite unused `generateCorpus` helper at workload:58. `runner.ts:245–246` retains admitted/settled ordinal sets for the entire trial. This is potential client CPU/GC pressure and unbounded-duration memory growth, **not proof** it explains current throughput.

**F14 — CONFIRMED BUG: provenance overstates corpus verification.** `runner.ts:171–175` hashes only `Math.min(admittedCount,1000)` payloads. The name `corpusSha256` and workload comment:67 claim entire corpus. Manifest:177–199 omits seed, source/dist hashes, SDK versions, effective native config and per-destination counts. Summary scripts omit trial IDs, weakening audit linkage. Histogram:55–62 reservoir sampling makes percentiles approximate and non-deterministic above50,000 records; disclose this separately from workload determinism.

**F15 — CONFIRMED BUG: error metric units vary by failure path.** `runner.ts:375–389` increments record error/timeout totals by batch length but calls `metricsServer.recordError` once. Per-receipt failures follow a different path. `metrics-server.ts:78–80` increments by one. Dashboard “errors” can therefore mean failed operations or failed records depending on failure mode. Late confirmations are excluded from confirmed metrics with no equivalent per-record late counter in the exposed set.

**F16 — VERIFIED BEHAVIOR: setup/cleanup safety guarantees are incomplete.** `runner.ts:108` awaits unbounded init; `main.ts:140–145` awaits close; Kafka/Redis drains are no-ops, Rabbit awaits flush internally. `rabbitmq.publisher.ts:71–75` suppresses every createStream error, not only already-exists. Configuration/connection problems can surface later or hang outside operation timers. These are failure-path risks, not established causes of successful-run throughput.

## D. Partition distribution analysis

For full application batches B, measured starting ordinal W and partition count P:

`destination(k) = (W + k*B) mod P`

The sequence visits `P / gcd(P,B)` distinct destinations. This is batch stickiness, but not fair round-robin stickiness. Concurrency does not repair it: batch assembly is synchronous on the JS thread and consumes a contiguous ordinal slice before awaiting the publish.

For the current throughput cell W=1000, N=10000, B=256, there are39 full batches and a16-record tail. The desired balanced record counts for a six-way comparison are approximately1666–1667 per destination. A round-robin batch policy would instead permit up to one batch's imbalance, but still visit all six.

| P | Actual first destinations | Predicted records by destination | Observed at Kafka adapter `producer.send` boundary |
|---|---|---|---|
| 1 | 0,0,0… | [10000] | [10000] |
| 3 | 1,2,0,1,2,0… | [3328,3344,3328] | [3328,3344,3328] |
| 6 | 4,2,0,4,2,0… | [3328,0,3328,0,3344,0] | [3328,0,3328,0,3344,0] |

Observed means **the actual current TypeScript Kafka adapter was loaded through an in-memory TypeScript transpile hook**, its producer was replaced with a send-capture mock, and `publishBatch` received actual WorkloadGenerator batches. No file/build modification or broker publish was involved. There were40 captured calls and average250 records/call. Redis/Rabbit have the same verified formula; their row is a source-level prediction, not independently broker-observed counts.

At B128/W800/N8000: P3 counts [2624,2688,2688]; P6 counts [2624,0,2688,0,2688,0]. At B64/P6 the gcd is also2. At B3/P6 only two destinations would be used; at B6/P6 only one. If byte cap makes B smaller, use the **effective** B. E.g. configured256 records with64KiB and1KiB payload produces64-record full batches. Odd payload/byte-cap combinations and varying tail lengths can change routing.

Single-record publishing selects `ordinal % P`, so it visits all P destinations. Warmup changes the starting phase; cumulative topic offsets can show all partitions populated by warmup, other batch shapes or earlier trials even when one measured run used only half.

**Broker-observed per-trial distribution: NOT YET VERIFIED.** Historical artifacts include Kafka metadata showing six partitions/leader1/ISR[1], but no before/after per-partition offsets or record-count deltas. Current offsets cannot reconstruct past trial distribution in shared persistent topics. Installed Confluent `_producer.js:970–984` preserves supplied explicit partition; no inspected code overrides it. A future bounded run must capture per-partition ACK destinations and isolated offset deltas.

Rabbit creates ordinary streams independently, not a Super Stream topology: `rabbitmq.publisher.ts:69–82` calls `createStream` and `declarePublisher` per name. Redis stream keys remain on a single instance/client; creating six keys does not create six independent CPU execution engines.

## E. Fairness assessment

| Dimension | Established | Missing / consequence |
|---|---|---|
| Resource limits | Compose sets2 CPUs/2048MiB per broker. Read-only `docker inspect` confirmed NanoCpus2000000000 and Memory2147483648 on all three current containers. | These are ceilings, not reservations. Host Node process has no matching quota. Historical host contention is not recorded. |
| Isolation | Dedicated benchmark endpoints/containers; application brokers are separate. | At audit time benchmark Kafka, Redis, Rabbit and real application Kafka/Postgres/Debezium were all up. This does not prove historical interference, but partition script does not isolate them. |
| Nodes | One broker process/node per technology in compose. Kafka metadata artifacts show broker1 replicas/ISR. | P partitions/streams are not P nodes. Redis single-instance keys and independent Rabbit streams have different execution models. |
| RAM/durability | Redis command enables AOF, appendfsync no, maxmemory1gb/noeviction. Kafka heap configured768MiB; all container caps2GiB. Volumes persist. | Equal caps don't imply equal memory/cache architecture. Redis may hit memory rejection during sustained tests. No common synchronous power-loss durability guarantee. |
| ACK | All success paths wait for broker responses/confirms. Kafka acks-all with RF1 has only one in-sync replica. | Redis XADD reply, Kafka RF1 ACK and Rabbit Stream confirm have different persistence paths. Don't label them equivalent fsync durability. |
| Payload | Same size/seed can produce same payload bytes; decimal MB/s calculation is correct for payload. | Kafka keys, protocol frames and storage metadata differ. No wire throughput measured. Some historical template hashes differ. |
| Batch | Same application maximum records/bytes can be requested. | Kafka native queue aggregation, Redis pipeline and Rabbit chunks differ. Application batch counts are not measured wire request counts. |
| Connections | One Kafka producer object, one Redis client, Rabbit client plus P publisher objects. | Objects are not TCP connections. Rabbit SDK connection management and Kafka broker connections require runtime enumeration; equal connection counts unverified. |
| Latency | All include client-to-ACK work from adapter invocation. | Kafka/Redis use final batch completion for every record; Rabbit individual callback times. Pre-adapter assembly/linger excluded. Reported p99s are not exactly equivalent record-level events. |
| Repetitions | General script calculates independent metric medians over clean trials. | Can report fewer than5 clean repetitions; fixed broker/cell order and retained data. Partition script is one-shot. Repetitions are separate processes but not clean storage/cache resets. |

RabbitMQ's documentation states Streams rely on OS flushing rather than fsync before every confirmation; Redis's configured asynchronous AOF likewise does not imply durable-on-reply writes. The baseline should be described as **single-node broker-acknowledged ingress with explicit per-broker persistence settings**, not identical durability. Source: [RabbitMQ Streams documentation](https://www.rabbitmq.com/docs/streams).

The existing Prometheus/Grafana infrastructure is reusable, but neither its current windows nor current labels establish trial-aligned throughput. Keep final JSON results and time-series views distinct.

## F. Root cause assessment and contradictory results

### Artifact provenance: all seven quoted throughput values located

Files below are `benchmark/ingress/results/<trial>/result.json`. Numeric values come from result fields and their manifests, not the prior AI report.

| Trial suffix (`trial-…-kafka`) | Manifest client | P | B / byte cap | Count / warmup | C | msg/s | Seconds |
|---|---|---:|---|---|---:|---:|---:|
| 1793525628548 | kafkajs | 1 | 256 /262144 | 10000/1000 |16|25673.94|0.3895|
| 1793529381799 | kafkajs | 6 | 256 /262144 | 10000/1000 |16|54614.96|0.1831|
| 1793538286805 | confluent-kafka-javascript |3|128 /131072|8000/800|16|15426.15|0.5186|
| 1793538325578 | confluent-kafka-javascript |6|128 /131072|8000/800|16|32746.62|0.2443|
| 1793539079621 | confluent-kafka-javascript |1|256 /262144|10000/1000|16|34977.26|0.2859|
| 1793539118197 | confluent-kafka-javascript |3|256 /262144|10000/1000|16|21958.72|0.4554|
| 1793539180928 | confluent-kafka-javascript |6|256 /262144|10000/1000|16|24950.10|0.4008|

All seven report clean, zero errors and zero timeouts. Payload is1024 bytes. B256 averages250 records over40 application calls; B128 averages127 after rounding (8000/63). No evidence here of those runs accidentally retaining the64KiB cap.

The first KafkaJS trial's template SHA begins `8bc1b97f`; the remaining six begin `76746b36`. Thus even the nominally comparable KafkaJS p1/p6 pair does not use the same template. No source revision/dist fingerprint establishes their historical routing implementation. Applying today's routing formula retroactively as proven history would be incorrect.

Verified differences: client labels, P, B, byte cap, N, W, template, time, and resource snapshots. Actual native batching, native config, active competing workload and historical source changes are **NOT YET VERIFIED**. More partitions need not improve a one-broker short test, and today's p6 route uses only three destinations; neither fact quantitatively explains all seven values.

### Single-record Confluent slowdown

Latest saved trials:

| Trial | P | count | msg/s | window | p50 |
|---|---:|---:|---:|---:|---:|
|1793539084737|1|500|57.94|8.6297s|15.845ms|
|1793539123206|3|500|16.57|30.1679s|60.393ms|
|1793539186089|6|500|16.75|29.8589s|59.838ms|

**VERIFIED BEHAVIOR:** c1/b1 waits for each delivery; average loop time is approximately17.3ms or60ms. That explains the rate arithmetically, not the underlying latency cause. Source-default native linger5ms is present despite application linger0; ACK waiting is correct; no explicit hot-loop Kafka flush exists.

**SUSPECTED ISSUE:** native linger plus Windows scheduling, native-to-JS delivery callback wakeups, broker/network scheduling or idle-partition behavior could contribute. Default5ms alone does not prove a60ms delay. Background polling is enabled, so importing a low-level example's polling interval would be an unsupported explanation.

Crucial counterevidence: p3 single-record trial1793538291970 achieved59 msg/s, while later p3 achieved16.57. p6 trial1793537321537 achieved59.69, later p6 achieved16.75. Partition count alone cannot explain these outcomes. Older stored KafkaJS trials also include very slow4.49–12.24 msg/s runs; the premise of a universally stable600 msg/s KafkaJS baseline is unsupported by the full saved history.

Required measurement: timestamp native enqueue, broker Produce response/native delivery event, and JS receipt; capture native linger config, per-request RTT/queue delay, event-loop delay and packet timing; compare host Windows versus colocated Linux runner under the same broker/settings. No current trace separates these stages.

### Resource evidence and c16 → c64

The p1 native34,977 trial recorded endpoint containerCPU197.65%, loop p99 14.59ms and12MB block writes. The p3 native21,959 trial recorded2% CPU, loop p99 18.645ms and0 block writes; p6 native24,950 recorded1.41%, loop p99 34.603ms and0 writes. Endpoint CPU fluctuating from near2 cores to almost idle is consistent with sampling after a short burst; it is not evidence the p1 run saturated the broker while p3/p6 did not.

**NOT YET VERIFIED:** CPU throttling, disk saturation, network saturation, native queue exhaustion, or a single-partition broker lock causing c64 regression. The earlier claim “single partition hits TCP produce queue contention” lacks a request/queue/CPU trace. Higher concurrency can add queueing and allocation without increasing bottleneck service rate, but that is a hypothesis to test. The historical c16/c64 KafkaJS results are not a native-client concurrency sweep.

### Ranked causal assessment

1. **Confirmed, high impact on conclusions:** routing skips half the six-way destinations; short measurements and inconsistent provenance prevent sustained/scaling claims.
2. **Confirmed configuration mismatch:** native linger/batch controls are not the reported application controls; retries falsely reported zero. Exact throughput impact unmeasured.
3. **Confirmed interpretation limit:** c1/b1 is serial ACK latency, not capacity; batch completion and record completion latencies differ between adapters.
4. **Confirmed failure/duration correctness defects:** clean validation, pacing, window classification, partial outcome handling. Their impact on successful rate0 count10000 runs must not be assumed.
5. **Suspected performance contributors:** native callback/scheduling delay, host/Docker contention, allocation/GC and per-destination native batch fill. Need controlled instrumentation.
6. **Unproven:** inherent Kafka capacity ceiling at21K, universal superiority of another broker, or a specific disk/network/CPU bottleneck.

## G. Proposed fix plan — not implemented

| Priority | Change and affected files | Trade-off / risk | Required proof |
|---|---|---|---|
|1|Fix completion/window/terminal contracts in `runner.ts`, `contracts.ts`, adapter receipt paths.|Failure ambiguity must remain explicit; don't convert timeouts into known rejections.|Partial success, early deadline, late ACK and duplicate/unknown receipt tests. Count target/duration validity independently reported.|
|2|Make routing policy explicit in all three adapters. For batch-stickiness use a batch sequence independent of ordinal; preserve separate single-record route policy.|Record-level round robin fragments batches; batch round robin has bounded tail imbalance. Don't change Purchase partition keys.|P1/3/6, B1/3/64/128/256, byte-limited and tail tests; captured ACK destinations plus broker deltas.|
|3|Reject unknown CLI fields; validate duration/timeout and partition bounds in `config.ts`; document all aliases.|Stricter parsing breaks previously ignored flags, intentionally.|Parser negative tests and config→adapter mapping tests.|
|4|Separate application batching from native Kafka config in config/main/Kafka adapter/manifests. Expose explicit native linger, bytes/messages, retry/idempotence settings.|Native merging makes app batch and wire batch differ; no forced Kafka advantage. Retry semantics must be disclosed.|Captured constructor config plus native statistics/config snapshot; zero/5ms latency A/B.|
|5|Implement one global admission pacer; enforce or explicitly document oversized record policy and consistent warmup settings in runner.|Open-loop offered load requires bounded queues; closed-loop rate is still ACK constrained.|C1/4/16 rate ceilings, burst bounds, no final pacing sleep, warmup parity.|
|6|Normalize latency contracts. Report record ACK latency where observable and separate batch-completion latency.|High-level Kafka/Redis APIs may need instrumentation for individual ACK timing; avoid pretending batch time is exact record time.|Delayed member test; timestamps and histogram cohort counts match.|
|7|Add effective topology/config/provenance and per-destination counters to manifests/results; retain trial IDs in summaries.|Some introspection may require permissions; missing verification must mark trial unverified.|Source/dist hashes, SDK/image versions, seed/full-corpus specification, actual partition count and settings recorded.|
|8|Align resource collection and Prometheus/Grafana with sustained runs; fix metric units and CPU label.|Sampling has overhead; measure it. Avoid unbounded metric label cardinality.|In-window CPU time/throttle deltas, I/O/network/native stats, scrape coverage and trial configuration mapping.|
|9|Bound workload/state memory and remove measured allocation only if profiling justifies it.|Full corpus preallocation can exhaust16GB; use bounded deterministic reusable blocks with ownership guarantees.|Duration memory plateau, unchanged payload hash/content and measured profiler improvement.|
|10|Revise matrix scripts to build/verify artifacts, randomize/block repetitions, preflight topology/resources, retain raw results.|Longer trials use storage/RAM; no automatic data deletion. Failed cells cannot silently become five-clean claims.|Reproducible repeated matrix, explicit invalid trials, confidence/variation summaries.|

Only benchmark/monitoring files are in scope for a future implementation. No producer-api, outbox, loyalty, analytics, replay, CDC or Purchase changes are needed. No production topic partitioning changes are proposed.

## H. Verification plan

### Gate 1: correctness before broker performance

Add deterministic mocked-adapter tests for all P0/P1 cases. Existing `pnpm --filter @lab/ingress-benchmark test:unit` passed **4 files /18 tests** during this audit. Current tests cover ordinary byte caps, simple hangs, batching and one duration case, but not the reproduced defects. Passing these tests is not validation of benchmark correctness.

Verify source and executed dist have identical build provenance. Store requested config separately from normalized config, native effective config and observed broker topology. Keep native max-in-flight distinct from application concurrency and measured occupancy.

### Gate 2: bounded diagnostics, independent destinations

Use synthetic benchmark-only destinations with a unique run prefix. Record initial/end offsets or stream lengths; do not reset shared topics. First verify distribution at P1/3/6 with B1/64/128/256 including tails. Reconcile admitted records, broker-confirmed records, known rejections, unknown outcomes and late confirmations. A cumulative topic offset count is not a substitute for run-scoped counts.

For single-record latency, run repeated c1/b1 trials at native linger0 and5ms with identical ACK/retry/idempotence settings. Alternate conditions to reduce order bias. Compare fixed destination versus balanced routing. Obtain actual native queue/RTT/delivery timings before selecting a cause. Do not change request limits and linger simultaneously.

### Gate 3: sustained comparison with memory/storage budgets

Start with a small screening matrix, not a full Cartesian product:

| Purpose | Cells | Measurement |
|---|---|---|
| Latency | P1, c1, b1; Kafka linger0/5 diagnostic | Warm connection,10s warmup,30s measurement; ACK latency and callbacks. |
| Batching | P1,c16; b64 and256 with matching payload byte caps | Native/request sizes and ACK throughput; no rate limit. |
| Concurrency | Best verified batch; P1; c1/4/16/64 | Throughput versus native queue depth, RTT, loop delay, CPU. |
| Partitioning | Fixed C/batch; P1/3/6; verified all-destination routing | Count/ACK distribution and actual batch fill per partition. |
| Cross-broker | Common payload/seed, application C/batch and explicit single-node ACK profile | Same host placement/resources, five randomized valid repetitions for selected cells. |
| Client attribution | Native Node versus Kafka official Java perf producer | Matched broker/topic topology, payload size, ACK/retry/compression/idempotence; disclose differing key/workload generation and buffering. |

Use30–60s measurement windows only after capacity preflight. At35K/s ×1024B,30s already adds about1.075GB **payload alone**, beyond Redis's1GB maxmemory before overhead. Thus a single unbounded30s maximum-throughput Redis run is unsafe under current settings. This is a concrete constraint, not a reason to reuse subsecond comparisons. Before long tests, choose an explicitly budgeted retention/storage strategy or a feasible lower-rate duration, and state how retention work affects the benchmark. Do not silently add trimming to Redis alone or raise limits during an audit. Pause between experiments on memory/storage thresholds; don't delete existing data automatically.

For16GB Docker RAM, run one benchmark broker at a time when operationally possible, preserve the application, and reserve capacity for its existing services, monitoring and Docker overhead. If the demo must remain active, record its load and treat the resulting benchmark as a shared-host condition. Equal2CPU/2GiB broker caps are a valid controlled resource envelope, not an assertion of equal internal resource use.

Use Kafka's official `kafka-producer-perf-test.sh` as a **diagnostic reference**, not a directly interchangeable latency score. First run Java and Node in the same network namespace/location and comparable CPU envelope; then separately measure Windows-host versus Docker networking. Match explicit configuration and distribute to all partitions. Collect native records/request, request rate, queue delay, retry count, broker Produce request timings, CPU time/throttling, process/event-loop metrics, block I/O and network bytes. A larger Java result would demonstrate client/path headroom under those conditions, not automatically identify the responsible Node code.

Success criteria: requested experiment completes, zero unexplained outcomes, topology/distribution verified, config provenance complete, enough temporal coverage to establish steady behavior, bounded memory/storage, and repeated results with variability reported. Broker ranking is permitted only for the explicitly tested configuration and ACK profile.

## Audit actions and limitations

- Read current benchmark config, runner, workload, histogram, metrics, report, adapters, orchestration, compose, monitoring settings, installed SDK paths, and saved results.
- Verified Confluent1.11.0/librdkafka2.16.0 source defaults and delivery-report waiting. Official documentation corroborates distinct native batching/linger behavior: [Confluent JavaScript overview](https://docs.confluent.io/kafka-clients/javascript/1.9/overview.html), [librdkafka configuration reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md). Installed version source is authoritative for this audit's defaults.
- Executed mocked Kafka routing capture, actual config parser cases, actual runner pacing/deadline/oversized-record checks; no network publishes.
- Read current Docker status and resource limits; did not stop/start/change any container.
- Ran existing18 benchmark unit tests successfully. Full workspace quality gates/infrastructure verification were not run because no implementation changed and this was a scoped read-only audit.
- No new Kafka perf run, packet capture or runtime native stats was collected. Historical per-partition distribution, exact native batch sizes, TCP connection counts, and the cause of the roughly60ms single-record cycle remain unverified.

The next action should be correctness fixes and their narrow validation, followed by diagnostic tuning—not another headline throughput matrix using the current runner.

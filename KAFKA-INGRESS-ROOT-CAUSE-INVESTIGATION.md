# Kafka ingress throughput: evidence-based investigation

Date: 2026-11-01. Investigation and planning only. No application, benchmark source, or infrastructure configuration was changed.

## 1. Conclusion

**The reported ~21K Kafka / ~29K Redis / ~35K RabbitMQ rates do not establish broker capacity or an intrinsic Kafka disadvantage.** They are genuine short-run confirmed-publish measurements of specific Node clients, but the benchmark remains dominated by short finite workloads, startup/fill/drain behavior, client costs, and incompletely enforced controls.

Kafka batching now works: the adapter sends arrays of 64 records. The old explanation that Kafka still sends one record per request is obsolete. However, advertised batch-byte, linger and offered-rate controls are not operational. The latest rewrite also did not actually implement a fixed-duration steady-state window.

Isolated diagnostics show the same one-partition, two-CPU Kafka broker accepting substantially more than 21K messages/s. The official Java performance tool reported 55–69K steady-state messages/s with a 64 KiB batch target, and 93K in a 256 KiB diagnostic. These are capability checks, not directly matched Java-versus-Node scores. Longer Node diagnostics ranged from 24–42K with 64-record batches and 48–49K with 256-record batches. The concurrency-64 decline did not reproduce consistently.

**Strongest supported explanation:** a client/configuration/measurement ceiling, with material KafkaJS encoding/allocation costs and short-run sensitivity. Single-partition broker CPU pressure is plausible at higher rates, and CPU throttling was observed, but a hard broker TCP queue bottleneck at 21K is not established.

## 2. Isolation, actions and evidence limits

- Inspected current source, installed KafkaJS behavior, latest saved trial results, configuration and resource sampler. Prior agent claims were treated as hypotheses.
- Started only the previously stopped `kafka-bench` container, without recreating it or changing its configuration. Verified actual limits: 2,147,483,648 bytes RAM and 2 CPU via Docker inspect.
- Created a dedicated one-partition RF1 topic, `benchmark-diag-20261101`, solely for diagnostics. Queried metadata confirmed one leader/replica/ISR and min.insync.replicas=1.
- Ran the existing compiled Node adapter/runner via temporary command-line harnesses with diagnostic topic overrides; no benchmark source edits or rebuilds.
- Ran official Kafka 4.3.1 performance tools in temporary auto-removed client containers, separate from the broker cgroup, with 2 CPU and 768 MiB limits. Broker configuration remained unchanged.
- Deleted the diagnostic topic, stopped `kafka-bench`, and verified no containers remained running. Existing Purchase services, database, Kafka topic/groups and application broker remained stopped and untouched.
- Saved diagnostic JSON, profiler data, cgroup snapshots and tool output under `benchmark/ingress/`. These are investigation artifacts, not source/config changes.

One preliminary admin command used the host-facing listener from inside Docker and failed because metadata advertised localhost:9094. It was corrected to the internal listener kafka-bench:29092. That failed process briefly overlapped early Node diagnostics, adding a confound; early values are exploratory, not clean comparative trials. It had exited before the Java and profiled diagnostics. This error demonstrates why placement/listener settings must be recorded.

No Redis/RabbitMQ performance reruns were made: this investigation targeted the claimed Kafka limit. Their latest numbers were inspected from saved results. All new runs are short diagnostic interventions; they do not certify long-duration sustainable capacity or predict a winning broker.

## 3. Ranked causes and issues

| Rank | Finding | Evidence strength | Impact / interpretation |
| --- | --- | --- | --- |
| 1 | Measurement is still a tiny count-to-completion burst | Confirmed source and artifacts | Invalidates steady-state capacity and saturation claims; 5,000 records means only 79 batches at batch64 |
| 2 | KafkaJS/client-side encoding and allocation cost is material | CPU profile, process CPU and event-loop delays; alternative-client diagnostic | Strong contributor to observed client ceiling; exact percentage of measured loop requires phase-scoped profiling |
| 3 | Tuning controls are incomplete; larger actual batches increase diagnostic throughput | Confirmed unused options and batch64/256 interventions | Kafka tuning search has not established an optimum; arbitrary fixed batch/concurrency cannot represent maximum ingress |
| 4 | High concurrency primarily enlarges outstanding batches and queueing in a tiny run | Confirmed scheduler; lower throughput not consistently reproduced | Plausible latency/overhead cause; does not prove single-partition broker TCP contention |
| 5 | Resource telemetry cannot attribute historical saturation | Confirmed sampler implementation | Post-run CPU snapshots are not load-window CPU; no original disk/network/throttling traces |
| 6 | Broker CPU/cgroup/cache pressure exists during longer tests | Actual cgroup deltas and memory events | Potential limiting factor at higher rates; no evidence of OOM or a universal 21K ceiling |
| 7 | Protocol, ACK timing and durability details differ | Confirmed adapters/settings | Restricts interpretation of cross-broker comparisons, especially latency; no proof of fraudulent ACK counts |

## 4. Kafka producer and batching audit

`apps/ingress-benchmark/src/adapters/kafka.publisher.ts:106` implements publishBatch using one send with an array of records, `acks=-1`, compression none, and explicit partition zero. This is effective multi-record submission. The runner's average batch size measures logical records passed, not observed wire request bytes. At 5,000 records, batches are 78×64 plus one×8, average approximately 63.3.

`maxInFlightRequests` is 64 by default (`kafka.publisher.ts:32`). Runner concurrency is worker slots, each holding a whole batch, not individual messages. Thus c16/b64 allows up to 1,024 records outstanding and c64/b64 up to 4,096. Broker request limits and logical message limits must be separately labeled and controlled.

`config.ts:14–19` parses batchBytes, batchLingerMs and rate. `runner.ts:235` slices solely by maxRecords. maxBytes/maxLingerMs are stored and reported but never applied; rate is not wired into scheduling. Changing those CLI options cannot support a claim about tested batch bytes, linger or offered rate.

KafkaJS 2.2.4 does not expose Java producer `linger.ms` and `batch.size` through this send path. For this implementation, accumulation/flush policies must be implemented by the benchmark, or another client used deliberately. Do not add Java property names to a KafkaJS object and claim tuning is effective. [KafkaJS producer API](https://kafka.js.org/docs/producing).

The official Java producer has its own per-partition accumulator, byte batch target, linger and buffering. Its observed records/request and batch-size metrics are the evidence of effective batching; nominal configuration alone is insufficient. [Kafka 4.3 producer settings](https://kafka.apache.org/43/configuration/producer-configs/).

Request overhead remains relevant: multiple 64-record requests incur framing, checksums, encoding, JS promise handling and request/response work. Larger batches amortize these costs but can add waiting latency and memory usage. The diagnostic batch256 improvement supports amortization as useful; it does not isolate exactly which stage improved.

## 5. The latest reported runs are not steady state

The matrix still sends 5,000 measured records after only 500 warmup records in scenario B. Typical reported durations derived from artifacts are about 0.23 seconds Kafka c16, 0.27 seconds Kafka c64, and 0.10–0.25 seconds for competitor throughput trials.

`runner.ts:220` keeps isWindowActive true; `:322` awaits every worker; `:325` closes the window only afterward. There is no independent t1 fixed by measurement duration. Consequently, nearly all successful confirmations count in the window and the late-confirmation branch is normally unreachable. This measures whole-cohort elapsed throughput, including filling and draining the pipeline.

At c64, 64 of only 79 batches can be dispatched immediately. That is mostly a large initial wave and a short tail, not a mature steady queue. At c16 there are roughly five waves. Increasing concurrency changes a large fraction of total experiment structure. The expected 4× window-size increase can increase allocation bursts and event-loop stalls without producing more steady-state capacity.

Five repetitions improve description of this short-run experiment, but do not remove systematic warmup, finite-cohort, cache, ordering or client-runtime bias. Little's law requires stable populations and appropriate time averages. Multiplying independently computed median throughput by median latency from potentially different trials cannot establish exact slot occupancy or locate a bottleneck. The prior report's precise occupancy percentages and broker TCP contention explanation should be withdrawn as unproven.

## 6. Diagnostic results

### 6.1 Existing Node runner, longer workload

Same existing compiled runner and Kafka adapter; host Windows Node v22; same benchmark broker/topic partition; 1 KiB payload; 100,000 measured records, 10,000 warmup; compression none, acksall, retries0; no metrics listener/container sampling in this harness. Each cell executed twice in the listed sequence within one Node process. Therefore JIT/process reuse and order differ from historical fresh-process trials.

| Sequence | Batch slots | Records/batch | msg/s | Duration s | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 16 | 64 | 24,246.54 | 4.1243 | 87.124 |
| 2 | 64 | 64 | 41,837.50 | 2.3902 | 190.628 |
| 3 | 16 | 256 | 49,492.70 | 2.0205 | 159.240 |
| 4 | 64 | 64 | 31,973.40 | 3.1276 | 192.306 |
| 5 | 16 | 64 | 35,747.48 | 2.7974 | 66.778 |
| 6 | 16 | 256 | 48,132.46 | 2.0776 | 157.251 |

Artifact: `benchmark/ingress/kafka-diagnostic-node-20261101.json`.

Interpretation: c64 does not reliably produce less throughput than c16; it does produce substantially higher latency here. Batch256 at c16 gives more throughput than batch64 in this exploratory set. These are sensitivity observations, not statistically controlled optimal settings. The first runs have the preliminary-admin confound described above.

### 6.2 Official kafka-producer-perf-test.sh

Separate Linux Docker client container, same one-partition broker, same ACK/replication/compression class, 1,024-byte generated record bodies, 300,000 total records, 30,000 excluded warmup. Explicit enable.idempotence=false, retries0, max.in.flight.requests.per.connection=16; throughput=-1. Native Java producer buffering is not bounded to the Node runner's identical logical record window.

| Batch target | Linger | Overall msg/s | Tool-reported steady msg/s | Actual avg batch bytes | Records/request avg | Request latency avg ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 65,536 B | 0 ms | 45,045 | 55,430 | 58,467 | 56.54 | 7.58 |
| 65,536 B | 5 ms | 49,116 | 63,664 | 65,070 | 62.93 | 6.54 |
| 262,144 B | 5 ms | 68,666 | 93,426 | 254,051 | 245.70 | 4.89 |
| 65,536 B repeat | 0 ms | 53,725 | 68,966 | 57,965 | 56.05 | 6.01 |

Raw outputs: `benchmark/ingress/kafka-diagnostic-java-b64k-l0.txt`, `...-b64k-l5.txt`, `...-b256k-l5.txt`, `...-b64k-l0-repeat.txt`.

The repeated linger0 run outperformed the single linger5 run. Therefore these results do **not** establish a causal benefit of 5 ms linger. They demonstrate different batch fill and large run-order/warmup variation. Likewise the 256 KiB result is one diagnostic, not a stable maximum.

Critical caveats: Java runs on Docker's bridge, Node on Windows through published ports; Java's generated content/key framing differs; Java has its own queueing and buffer budget; warmup/duration/lifecycle differ. Compression is disabled, limiting content-compressibility effects but not all encoding differences. The performance tool's printed MB/s uses binary units; compare msg/s or recompute decimal MB/s, not its MB label against the app's decimal field.

At 64 KiB the Java producer reported hundreds of milliseconds of record queue time and bufferpool waiting despite single-digit broker request latency. Producer ACK latency is not merely broker processing time. The 256 KiB run showed much lower queue time. Do not compare its request-latency metric directly with the Node adapter's complete send-to-confirm duration.

Reproduction template (only on isolated benchmark resources):

```text
docker run --rm --name ingress-diag-java --network ingress-benchmark_default
  --memory 768m --cpus 2 --entrypoint /opt/kafka/bin/kafka-producer-perf-test.sh
  apache/kafka:4.3.1 --topic <diagnostic-topic> --num-records 300000
  --warmup-records 30000 --record-size 1024 --throughput -1 --print-metrics
  --command-property bootstrap.servers=kafka-bench:29092 acks=all
  enable.idempotence=false retries=0 compression.type=none
  batch.size=65536 linger.ms=0 max.in.flight.requests.per.connection=16
```

This proves the tested partition is capable of more than 21K with another client/path/configuration. It does not prove Java is intrinsically superior or Kafka outperforms the other brokers.

### 6.3 Node CPU profile and resource observations

A separate existing-runner trial: c16/b64, 200,000 measured records, 10,000 warmup, same topic/ACK settings, CPU profiler enabled. Result 36,737.69 msg/s over 5.444 s, process CPU 4.922 user +2.109 system seconds, event-loop p99 277.348 ms, RSS about 412 MB. Process CPU may exceed wall time through runtime threads; do not interpret it as a percentage of the entire 16-CPU host.

Whole-process sampled self-time leaders:

- FastBuffer: about 2,158 ms.
- KafkaJS crc32C: about 825 ms.
- writeBuffer: about 402 ms.
- GC: about 373 ms.
- KafkaJS groupMessagesPerPartition: about 308 ms.
- Buffer fromArrayLike: about 303 ms.

Profile includes module startup, warmup, corpus construction/hashing and reporting as well as measurement; these are not measured-loop percentages. Nevertheless CRC32C/encoding frames and substantial in-window process CPU/event-loop delays provide direct evidence of client-side work, not just a speculative explanation. Further phase-scoped profiling should determine the addressable fraction.

Artifacts: `kafka-diagnostic.cpuprofile`, `kafka-diagnostic-profile-top.json`, `kafka-diagnostic-profile-result.json` under `benchmark/ingress/`.

Before/after broker cgroup snapshots bracketed the profiled process, including setup/warmup, not only its 5.444-second window. Broker CPU usage rose about 8.316 seconds, throttled_usec by about 0.682 seconds, and throttled periods by 15. Block writes rose 71,553,024 bytes, reads did not rise. These disk counters reflect actual block flush timing/cache, not total application accepted bytes. Memory events showed max-limit encounters but no OOM kill. This establishes resource pressure, not disk saturation or an OOM cause for the old ranking.

No synchronized network utilization or disk queue/latency time series was collected. Network bandwidth saturation and disk saturation remain unproven. Windows/Docker networking overhead remains a plausible confound. Do not infer disk health from a small block-write delta or label asynchronous page-cache writes durable per record.

## 7. Assessment of the other agent's claims

| Claim | Verdict |
| --- | --- |
| Real multi-record Kafka batching added | Confirmed |
| Redis pipelines and RabbitMQ chunks added | Confirmed; ACK boundaries still differ in granularity |
| Fixed independent measurement window implemented | Incorrect: window ends after worker completion |
| Byte/linger controls effective | Incorrect: parsed/reported but unused in batching |
| Offered-load control available | Incorrect: parsed rate is not used by runner |
| Every record reconciles exactly once | Not enforced: receipts are not checked against expected unique ordinals/completeness; timeouts do not cancel operations |
| A hard deadline covers the entire lifecycle | Overstated: publish waits are raced; init/drain/close not uniformly bounded, outstanding work can continue |
| Resource sampling proves bottleneck | Incorrect: container stats fetched once after measurement; no disk/network/pressure time series |
| Histogram merge is reliable | Incorrect for percentile reservoir: merge appends until capacity, biasing toward the first input and not weighting by population |
| Batching unlocked true broker capacity | Unsupported: improved configured client throughput only |
| c64 decrease proves broker TCP produce queue contention | Unsupported, and not consistent with longer diagnostic outcomes |

Histogram defect does not invalidate exact-sample percentiles for 5,000-event individual trials (below reservoir cap). It does invalidate future claims of unbiased merged large-run percentiles. The code's count-to-completion arithmetic is not fabricated; its capacity interpretation is the problem.

## 8. Trustworthiness and fairness

Trustworthy within limits: successful returns use real server ACK paths; Kafka confirms whole send batches, Redis checks pipeline command results, RabbitMQ resolves native confirmation IDs. Count×1,024/elapsed/1e6 is correctly decimal payload MB/s. The existing numbers can be retained as exploratory finite-cohort results of those exact clients and configuration.

Not trustworthy as: maximum sustainable broker rates, broker-only latency, definitive saturation causes, equal durability ranking, or an optimized cross-broker comparison. Warmup is too short, workload is tiny, batch/byte/rate policies are incomplete, and resource attribution is inadequate.

Kafka maps one send completion timestamp to all records in a batch. Redis maps pipeline completion to every command (later than an early response may arrive). RabbitMQ tracks native confirmation timing per publishing ID, although worker completion waits for all promises. These timing populations should be exposed rather than presented as identical individual ACK boundaries.

All current profiles are single-node asynchronous persistence, not replicated durable quorum comparisons. Kafka RF1/acksall does not impose replicated ACK work here. Redis AOF/no fsync and RabbitMQ disk-backed Stream confirmations are not identical crash guarantees. None of these differences proves unfair intent; they require precise labels and separate durability studies. No need to weaken Kafka ACKs to manufacture a higher score.

## 9. Recommended fixes, expected effect and trade-offs

1. **Correct measurement before tuning.** Implement real duration/admission windows, warmup stabilization, complete outcome validation and cancellation. Expect more credible numbers, not necessarily higher numbers. Longer tests cost time and retained memory/disk.
2. **Make tuning knobs effective.** Enforce max records AND bytes and flush age; wire rate or reject it. Record actual requests/batches and bytes. Larger batches can raise throughput at the cost of latency, memory and failure granularity. Do not promise the diagnostic 2× gains will persist under controlled tests.
3. **Separate record, batch and request concurrency.** Tune batch slots and total outstanding bytes together; expose Kafka request limit in CLI/manifest. Prefer a throughput/latency frontier over setting c64 everywhere. Reducing queueing may lower p99 without reducing sustainable throughput.
4. **Profile and compare clients fairly.** Retain KafkaJS as the existing baseline. Add an optional diagnostic Java/native-client reference only after equivalent placement, resource limits, payload, batching and buffer budget are defined. A native client may reduce CRC/allocation costs, but brings packaging and semantic differences; do not replace KafkaJS based solely on these confounded runs.
5. **Use measurement-time resource collection.** Async periodic process/container samples, cgroup counter deltas, disk/network and event-loop availability. Avoid post-run docker stats as an attribution tool. Collection has overhead; measure it and preserve missing-data flags.
6. **Keep one-partition baseline; test partitions separately.** One partition is a legitimate ordered-ingress profile. Add 3/6 partitions only as a separate scaling experiment with actual producer distribution; current hardcoded partition0 defeats that test. More partitions are not a proven fix for this client bottleneck and change semantics/resources.
7. **Fix statistics and provenance.** Mergeable weighted histograms, sample counts, raw phase/corpus identity, effective settings, image digests, compatibility gates, randomized blocks and held-out final trials. Five identical systematic mistakes do not become scientific evidence through repetition.

## 10. Prioritized implementation plan

### P0 — Validity and truthfulness

Files: `apps/ingress-benchmark/src/runner.ts`, `contracts.ts`, `config.ts`, `main.ts`, `report.ts`, tests.

- Label existing saved results finite-cohort/legacy; preserve them unchanged.
- Implement separate warmup, fixed measurement, bounded final drain, and immutable finalized results.
- Track scheduled/admitted/confirmed/rejected/ambiguous/not-admitted identities; reject duplicate/missing receipts and incomplete warmup for scored runs.
- Bound init/close/flush and cancel pending operations; distinguish timeout from known rejection.
- Reject unsupported/unused configuration until implemented.

Acceptance: deterministic boundary/deadline/partial-batch/late/duplicate tests, exact reconciliation, no hidden infinite operations. No business application changes.

### P1 — Effective batching and diagnostics

Files: three existing adapter files, runner/config/contracts, new focused adapter tests.

- Kafka records/byte accumulation with maximum wait; explicit request limit; actual request metrics.
- Redis pipeline policy and per-command outcome; timestamp granularity documented.
- Rabbit chunks/flush timer with native ID correlation and tail/error handling.
- Bounded logical record/byte/request queues across all adapters.

Acceptance: observed batch/request sizes match declared policy, maxBytes is honored including appropriate payload/protocol-size definition, partial batches flush, linger tests prove behavior, and no ACK before server confirmation.

### P1 — Attribution and monitoring

Files: `resource-sampler.ts`, `histogram.ts`, `metrics-server.ts`, ingress Grafana dashboard, additive shared Prometheus rules/config only.

- Record time-window CPU/GC/event-loop/heap and broker cgroup/network/I/O samples asynchronously.
- Fix weighted histogram merging and reproducible quantiles; preserve overflow/empty flags.
- Separate live rates from exact run artifacts; show sample counts, queue wait, actual batches, throttling and unavailable data.

Acceptance: observer overhead measured, no post-run CPU presented as active CPU, no zero event-loop delay treated as proof of a healthy loop when sample count is insufficient.

### P2 — Controlled experimental matrix

Files: `scripts/run-ingress-comparison.mjs`, benchmark-only profiles, report aggregation.

- Scenario A: c1/b1, adequate warmup, 5–10 independent repetitions.
- Scenario B: matched payload/resource/persistence class; batch64/256 and bytes16/64/256KiB; slots4/16/64; fixed total outstanding-byte controls; repeat in randomized blocks.
- Offered-rate staircase around observed capacity; explicit latency/error/backlog gates.
- Repeat Java reference with matched runner placement and explicit buffer budget; include actual batch/request metrics, not just nominal linger.
- Only after client attribution, add partition1/3/6 and producer-process1/2 scaling separately.

Acceptance: held-out trials reproduce conclusions with uncertainty bounds, no unsupported claim of engine limits, failed/resource-limited trials visible. Larger workload must fit Redis retained-data memory; if 30–60 seconds at capacity is infeasible on this host, report burst performance or use a larger test host rather than silently trim only Redis.

### P2 — Optional client change or broker tuning

Make this decision from matched evidence. Candidate native Kafka client/Java producer is an additional benchmark mode, not a Purchase pipeline dependency. Broker thread/heap/partition tuning remains benchmark-only and separately profiled; do not modify current infrastructure until approved. Expected trade-offs and maintenance cost must be recorded.

## 11. Final answer to the root-cause question

Kafka is slower in the latest saved **specific short Node benchmark**, but the observed ranking is not evidence that Kafka's single partition tops out at 21K. Confirmed factors are ineffective tuning controls, a transient count-limited test, material KafkaJS/client work, and insufficient attribution telemetry. Diagnostics disprove the proposed hard ceiling and do not consistently reproduce c64 throughput regression. Real broker CPU/cache pressure exists at higher loads, while disk/network saturation and precise historical queue contention remain hypotheses.

Improve correctness and attribution first, then tune and compare under a declared workload/latency/durability contract. Any broker may legitimately win that comparison.

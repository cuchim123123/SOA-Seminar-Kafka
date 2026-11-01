# Official benchmark configuration feature

Date: 2026-11-02. Scope: existing external tools, not the removed custom Node ingress runner.

## Live implementation progress / handoff

User confirmed: configuration file + pnpm commands, no web UI; reusable execution logic, results preserved, disk protection and automatic owned-data cleanup.

- [x] Repository/tool analysis and installed-image help verification.
- [x] Initial strict Zod schema, sample config and pure command builder added in `scripts/official-benchmark/config.mjs` and `config/official-benchmark.json`.
- [x] Initial reusable execution module added: disk preflight/monitor, benchmark-container identity check, unique destinations, raw output/result files, scoped cleanup.
- [x] CLI preview/run entry and `benchmark:preview`, `benchmark:run`, `benchmark:test` commands added; `preview all` passed without broker access.
- [x] Execution review added image-ID pinning, installed-help validation, one-run lock, client-stop verification and same-broker identity check before cleanup. Live failure testing remains.
- [x] Seven automated config/argv/disk-parser/broker-identity tests pass.
- [x] All three tools passed bounded smoke runs. Kafka10000 records; Rabbit/Redis10s at configured1000/s. Cleanup requested on all, Redis EXISTS=0 and Rabbit queue list empty verified. Failed early attempts remain preserved.
- [x] User guide `docs/official-benchmarks.md`, README quick commands and password `.env.example` documentation added. Backlog update pending final validation.
- [ ] Run format/lint/typecheck/unit/infrastructure checks; report unrelated baseline failures separately.

Current edits are uncommitted; substantial unrelated changes predate this task. Do not revert them. Starting dedicated benchmark brokers for bounded smoke verification; they were initially stopped and should be stopped again when validation completes. Installed tool images are already present; help commands ran with network disabled. Continue with unfinished boxes and update this section at each milestone.

Validation checkpoint: typecheck passed; corrected lint and full Vitest passed (49 files/212 tests). Latest small safety changes still need final format/lint/test rerun. Installed memtier exits2 for valid help; capability check now accepts only its verified usage signature. One Rabbit preflight failed during a Docker daemon interruption (all broker exits255), then passed after restoring only benchmark containers. Remaining: negative disk/timeout tests, final checks/docs/backlog, stop test brokers. Results are in ignored `benchmark/official/`; failed attempts are intentionally retained.

## Verified architecture

- README section2.1 contains direct Docker commands for Apache Kafka ProducerPerformance, RabbitMQ Stream PerfTest and Redis memtier XADD.
- `package.json` starts/stops benchmark infrastructure, but has no official-tool runner.
- `docker-compose.ingress-benchmark.yml` isolates three brokers with persistent volumes and2CPU/2GiB limits. Redis currently uses1800MB maxmemory/noeviction.
- Existing Grafana dashboard observes broker-side traffic independently of the client. `perf-` topic names match its Kafka filter.
- `apps/demo-ui/src/components/BenchmarkStudio.tsx` drives Purchase/API workloads. It is not an official-tool launcher and must remain unchanged.
- The earlier `apps/ingress-benchmark` and comparison scripts are absent. Historical artifacts remain ignored under `benchmark/`.
- Repository search found no reusable benchmark disk-space guard or owned-data cleanup implementation. Do not claim those mechanisms already exist.

## Tool capabilities and sources

Checked upstream documentation and actual installed image help, without connecting to brokers:

- [Kafka4.3.1 source](https://github.com/apache/kafka/blob/4.3.1/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java): count-based workload, payload bytes, throughput, warmup subset, native producer properties; no universal thread/concurrency or duration control.
- [RabbitMQ Stream PerfTest](https://rabbitmq.github.io/rabbitmq-stream-perf-test/stable/htmlsingle/): producers, consumers, duration, body size, batch size, confirms; publish rate is per producer. Ingress mode fixes consumers to zero. Actual installed help also exposes dynamic batching and optional delete-streams.
- [Redis memtier](https://github.com/RedisLabs/memtier_benchmark): arbitrary XADD command with `__data__`, threads, clients per thread, pipeline, request count or duration, per-connection rate and JSON output. Preserve these meanings.

Record image IDs with results because current README uses mutable latest tags. Never assume upstream main has exactly the flags of a local image; validate required options against its help before running.

## Recommended feature

One checked-in JSON configuration, strict schema validation, a command preview and a short launcher command. Common fields: payload size, client resource limits, network and safety settings. Separate Kafka/RabbitMQ/Redis sections for their actual controls. No arbitrary shell-command field. No new service or broker adapter.

The user selected the file/CLI interface. Keep schema/command generation and execution reusable; a future UI can call the same logic through a separately designed backend.

## Implementation sequence

1. Add strict validated configuration and pure Docker argument builders. Test every exposed field changes the relevant invocation, and reject unknown or contradictory settings.
2. Add preview/run interface agreed with the user. Execute argv directly with `shell:false`; keep secrets out of preview and saved manifests.
3. Validate existing benchmark containers and network identity. Never start/stop application brokers or auto-pull unexpected images.
4. Perform host and broker-volume disk preflight; poll volume headroom during execution. Reject unknown safety state, timeout jobs, and stop only the owned client container on cancellation/guard failure.
5. Use unique owned `perf-...` destinations. Explicit Kafka topic creation controls partitions. Preserve broker persistence/replication. Add narrowly scoped post-run cleanup of owned resources only; no flushall, volume removal or pruning.
6. Stream stdout/stderr to console and per-run files. Save redacted config, image ID, argv, timestamps, exit status, stop reason and cleanup result. Raw official output is authoritative; no fabricated common latency/result parser.
7. Provide conservative smoke defaults, plus documentation showing how to change workload settings. Leave direct README Docker commands usable.
8. Verify pure mapping/safety tests, no-Docker preview, installed tool help, then bounded smoke runs if benchmark infrastructure is active. Run repository quality gates and report unavailable infrastructure honestly.

## Safety and limitations

- A disk monitor is best-effort, not a hard byte quota: periodic samples and delayed writeback permit overshoot. Reserve headroom and cap runtime/workload. Do not interpret cleanup as immediate filesystem reclamation, especially Redis AOF.
- Duration and concurrency are tool-specific. Kafka count-to-completion is not made duration-based by killing it on a timer; watchdog exits are failures/interruption.
- Broker-side Grafana curves are approximate live views; no producer or monitoring rewrite required.
- Results remain tool-specific. Comparable configuration does not imply identical ACK/durability or latency semantics.
- Default workloads must be small enough for the current Redis memory limit. Do not silently add XADD trimming or alter benchmark broker resources.

## Acceptance

Users can edit one configuration, preview exact redacted commands and execute an official benchmark with a short command. Every supported field maps to a tested flag. Invalid input fails before Docker execution. Safety/cancellation failures are visible and do not leave an intentionally running owned client. Existing app features and broker data are untouched; cleanup is restricted to resources created for that invocation. Artifacts identify the settings/tool image and preserve output for comparison.

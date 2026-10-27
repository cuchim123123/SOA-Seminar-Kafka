# 16 — Benchmark CLI, Result Contracts and Cross-Broker Demo Runbook

This document is an **implementation target**. The listed commands and HTTP endpoints do **not** exist until the coding agent implements their tickets. Do not describe them as functioning beforehand.

## Benchmark CLI: stable interface

```bash
# Transport-only track, first Kafka baseline
pnpm benchmark:run --transport kafka --mode transport \
  --profile c1 --messages 50000 --payload-bytes 1024 \
  --customers 10000 --publishers 8 --consumers 3 \
  --target-rate 0 --seed 42 --duration-sec 30 --warmup-sec 10

# Redis and RabbitMQ runs: same workload seed and frozen config
pnpm benchmark:run --transport redis-streams --mode transport --config infra/benchmark/profiles/c1-1k.yaml
pnpm benchmark:run --transport rabbitmq-quorum --mode transport --config infra/benchmark/profiles/c1-1k.yaml

# Aggregate at least three completed comparable runs per transport
pnpm benchmark:report --input infra/benchmark/results/ --output infra/benchmark/reports/
```

All CLI options must be documented in `--help` and validated (sensible positive bounds, one clear meaning of `--target-rate 0` = closed-loop saturation). Prefer one canonical YAML file that freezes an entire matrix cell; CLI overrides must be echoed in the resulting manifest. `pnpm` is a suggested workspace tool; select exactly one package manager in Phase 0 and use it consistently.

## Explicit lifecycle state machine

`CREATED → PREPARING → WARMING_UP → RUNNING → DRAINING → COMPLETED` or `FAILED|CANCELLED|TIMED_OUT`.

- PREPARING asserts clean topology/unique run ID, available service ports, empty queue/stream, valid workloads and version/host/resource manifest.
- RUNNING uses a **bounded** in-flight queue and a monotonic measurement clock.
- DRAINING waits for all published events to reach success or a configured deadline, recording the remainder if it times out. Do not let an unfinished run appear `COMPLETED`.
- Always `close()` subscribers/publishers and export partial results on error in `finally`.
- Each run is immutable and unique; destructive cleanup only targets resources labeled by its `runId`.

## File layout for reproducible evidence

```text
infra/benchmark/results/
  <timestamp>-<transport>-<profile>-<runId>/
    manifest.json           # pinned versions, settings, topology, host, commit SHA
    events-summary.json     # input/confirmed/delivered/unique/dupes/errors
    timeseries.ndjson       # per-interval counters/rates/resources/backlog
    latency-histograms.json # mergeable producer / consumer / e2e distributions
    result.json             # typed summary + quality flags
    execution.log           # sanitized warnings/errors and relevant fault events
infra/benchmark/reports/
  comparison.csv
  comparison.json
  throughput.png
  producer-p99-latency.png
  consumer-p99-latency.png
  cpu-versus-throughput.png
```

Use consistent CSV columns plus machine-readable JSON for the dashboard. The repository should not commit huge raw runs by default; include `.gitignore` and small synthetic **sample** output clearly marked `EXAMPLE_ONLY`.

## Proposed TypeScript reporting contracts

```ts
export type Transport = 'kafka'|'redis-streams'|'rabbitmq-quorum'|'rabbitmq-stream';
export type TrialStatus = 'COMPLETED'|'FAILED'|'CANCELLED'|'TIMED_OUT';

export interface BenchmarkManifestV1 {
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly transport: Transport;
  readonly brokerMode: 'single-node'|'replicated';
  readonly benchmarkMode: 'transport'|'end-to-end';
  readonly profileId: string;             // e.g. C1, C2, C3, C4
  readonly startUtc: string;
  readonly gitRevision: string | null;
  readonly seed: number;
  readonly eventCount: number;
  readonly payloadBytes: number;
  readonly keyCardinality: number;
  readonly publisherConcurrency: number;
  readonly consumerConcurrency: number;
  readonly topology: Record<string, unknown>; // typed discriminated config in real code
  readonly durability: Record<string, unknown>; // native ACK, sync, replicas, persistence
  readonly softwareVersions: Record<string, string>;
  readonly host: { readonly os: string; readonly cpus: number; readonly memoryBytes: string };
  readonly warmupSeconds: number;
  readonly measurementSeconds: number;
  readonly drainTimeoutSeconds: number;
}

export interface LatencyPercentilesMs {
  readonly count: number;
  readonly p50: number | null;
  readonly p95: number | null;
  readonly p99: number | null;
  readonly max: number | null;
}

export interface BenchmarkResultV1 {
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly status: TrialStatus;
  readonly offeredEvents: number;
  readonly attemptedPublishCalls: number;
  readonly confirmedUniqueEvents: number;
  readonly receivedDeliveries: number;
  readonly processedUniqueEvents: number;
  readonly duplicateDeliveries: number;
  readonly failedEvents: number;
  readonly ambiguousEvents: number;
  readonly steadyStateConfirmedEventsPerSecond: number | null;
  readonly steadyStateProcessedUniqueEventsPerSecond: number | null;
  readonly confirmedPayloadMiBPerSecond: number | null;
  readonly producerAckLatencyMs: LatencyPercentilesMs;
  readonly consumerDeliveryToAckLatencyMs: LatencyPercentilesMs;
  readonly endToEndLatencyMs: LatencyPercentilesMs | null;
  readonly backlog: { readonly name: string; readonly value: string | null; readonly unit: string }[];
  readonly resourceSamplesAvailable: boolean;
  readonly warnings: string[];
  readonly validationPassed: boolean;
}
```

JSON cannot represent `bigint`, so export large counters or offsets as decimal strings where precision could be lost; keep histogram serialization valid across Node processes. The reporting schema above is intentionally minimal—extend with *versioned optional properties*, not silent semantic changes. For unknown measurements, use `null` and explicit warning flags.

## Control/observation API (read model, optional once dashboard is built)

- `POST /api/v1/benchmarks` validates an allowlisted configuration and schedules **local, bounded** run; does not accept arbitrary shell commands.
- `GET /api/v1/benchmarks/:runId` returns status, counters and progress from durable run metadata.
- `GET /api/v1/benchmarks/:runId/timeseries` returns real bounded samples.
- `GET /api/v1/benchmarks/compare?runIds=...` compares only compatible modes/profiles; if incompatible, return an explanation.
- `POST /api/v1/benchmarks/:runId/cancel` safely terminates known local run.

Run creation and cancellation are **application commands**. Results/status/timeseries are **queries**. Monitoring controllers validate inputs and dispatch CQRS handlers; they never launch Docker/shell directly. Local fault controls are disabled without explicit opt-in.

## Cross-broker demonstration (after Kafka phases are stable)

1. **Kafka baseline:** single broker, fixed topic/partition count, fixed workload. Capture manifest, 3+ trials and charts.
2. **Redis Streams:** same exact workload bytes and metrics pipeline; implement `XADD`, `XREADGROUP`, `XACK`, pending recovery and retention. Record AOF policy.
3. **RabbitMQ Quorum:** same workload bytes; publisher confirms, manual ack, explicit queue topology; record replica membership, prefetch.
4. **RabbitMQ Streams/Super Streams (optional):** add native offset-based log comparison, clarify that Quorum Queue is not the same product.
5. **Compare:** side-by-side median confirmed publish events/s, unique processed/s, producer/consumer p95/p99, resources, errors and availability caveats. Filter to compatible profiles and label the rest.
6. **Kafka replicated profile (separate):** RF3/minISR2/acks=all leader failover test with its own manifest. Do not merge it into one-node comparison charts without labeling.

## Benchmark acceptance tests

- Same seed and workload config produce byte-identical messages and event IDs across transports.
- No test changes broker-specific transport's measurement formulas or summary schema.
- Failure/timeout results are not included as successful runs in aggregate charts.
- Test catch-up/backlog metrics compare broker **native** concepts; no fake Kafka offset for Redis/RabbitMQ.
- Test percentile merger against a known distribution; ensure p99 of merged workers is calculated from merged observations.
- Integration fixture with deliberate duplicate confirms `receivedDeliveries > processedUniqueEvents` when appropriate, without silently incrementing processed count.
- Chart generator must work from saved result files offline, not from network calls or a hard-coded in-memory sample.

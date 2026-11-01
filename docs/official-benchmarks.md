# Configure and run the official benchmarks

Edit [`config/official-benchmark.json`](../config/official-benchmark.json) instead of reconstructing Docker commands. This launcher uses Apache Kafka ProducerPerformance, RabbitMQ Stream PerfTest and Redis memtier XADD. It does not use the old custom Node ingress runner or the web application's Purchase benchmark.

```powershell
pnpm benchmark:preview all
pnpm benchmark:up
pnpm benchmark:run kafka
pnpm benchmark:run redis
# Set BENCH_RABBITMQ_PASSWORD in .env or this terminal first:
$env:BENCH_RABBITMQ_PASSWORD = 'guest'
pnpm benchmark:run rabbitmq
pnpm benchmark:run all
```

`all` runs sequentially and stops on a failed process/safety check/cleanup. Preview validates configuration without Docker or passwords. An alternative configuration is supported:

```powershell
pnpm benchmark:preview kafka --config config/my-benchmark.json
pnpm benchmark:run kafka --config config/my-benchmark.json
```

Unknown options/fields fail rather than silently doing nothing. All settings are explicit in the file. Root-relative default paths work regardless of the terminal's working directory; an explicit `--config` path is relative to that directory.

## What the controls mean

| Section | Setting | Effect |
|---|---|---|
| Common | `payloadBytes` | Kafka record size, Rabbit body size, Redis generated XADD field value size. Protocol overhead differs. |
| Common | `client.cpus`, `memoryMiB` | Limits the temporary tool container, not the broker. |
| Common | `network` | Existing Docker network shared with the benchmark brokers. |
| Kafka | `partitions` | Creates a fresh owned topic with this many partitions and RF1. |
| Kafka | `records`, `warmupRecords` | Total record count; warmup is a subset, not additional records. |
| Kafka | `recordsPerSecond` | Tool's global publish rate; -1 means unthrottled. |
| Kafka | `batchBytes`, `lingerMs`, `bufferBytes` | Java Kafka producer properties; ACKs remain all, matching the existing README command. |
| Rabbit | `durationSeconds`, `producers` | Tool duration and producer count; consumers fixed to zero. |
| Rabbit | `batchRecords`, `dynamicBatch`, `outstandingConfirms` | Native batching and outstanding-confirm limit. With dynamic batching enabled, requested batch size is not proof of effective batch size. |
| Rabbit | `recordsPerSecondPerProducer` | Rate for each producer; aggregate requested rate is multiplied by producer count. -1 is unthrottled. |
| Rabbit | `username`, `passwordEnv` | Password is resolved at bootstrap from that environment variable; not stored in configuration/manifest. |
| Redis | `threads`, `clientsPerThread` | Total connections are approximately their product. |
| Redis | `pipeline` | Outstanding pipelined requests per connection; not equivalent to Kafka batch bytes. |
| Redis | `durationSeconds`, `requestsPerSecondPerConnection` | Duration and per-connection rate; null omits the rate limit. |
| Each tool | `image`, `container`, `host`, `port`, `dataPath` | Existing benchmark endpoint/image and mounted broker data location for disk checks. |

Kafka's tool is count-based: there is no fake common duration or concurrency flag. The watchdog is a safety timeout, not a valid duration benchmark. Rabbit/Redis duration controls remain native. Broker persistence, replication and resource limits are unchanged.

Defaults are deliberately small smoke workloads (~10 seconds, ~1000 messages/sec), not recommended maximum-performance settings. Edit the file to tune, taking disk and Redis memory limits into account. The launcher does not add trimming, consumers, retries, or a different measurement methodology. The direct README Docker commands still work independently.

## Results and monitoring

Every invocation saves `benchmark/official/perf-<broker>-<uuid>/`:

- `manifest.json`: normalized configuration, redacted preview argv, run identity/time, actual resolved Docker image ID.
- `tool-help.txt`: capability evidence from the installed image.
- `stdout.log`, `stderr.log`: official tool output, streamed directly to files without accumulating it in memory.
- `result.json`: process exit, safety/cancellation reason and cleanup status.

The console prints the output folder at startup. Follow a log with PowerShell `Get-Content <path>/stdout.log -Wait` if desired. Raw tool logs are local diagnostic artifacts; review them before sharing because the tools control their contents. Docker users can inspect client process arguments, including connection credentials.

`tool-exited-zero` means exactly that; it does not certify zero message errors or equivalent benchmark semantics. Inspect the official output. No synthetic combined p99/throughput summary is fabricated. Image tags are resolved once and the immutable image ID is executed; images must already be local (`--pull=never`). A changed tag can therefore be identified across runs.

Start the existing monitoring stack with `pnpm monitoring:up`, then open Grafana at the configured port (default3005). Generated Kafka topics begin `perf-` and match the current dashboard filter. Redis/Rabbit monitoring remains broker-side. Cleanup may remove topic series after a run; use historical time ranges and preserved CLI output, not a live graph as the final result.

## Disk protection, cancellation and cleanup

`safety.minFreeDiskMiB` is a free-space floor for the result filesystem, the Windows user-profile drive and the broker data filesystem. Docker's virtual filesystem can report more free space than its physical Windows drive; both checks matter. If Docker's VHD has been moved to another physical drive, that location is not automatically discovered: monitor its headroom too before a large run.

Use `safety.additionalHostPaths` to include any other physical drive holding a relocated Docker VHD, for example `["D:/"]`. The launcher checks before publishing and every `pollSeconds` during the tool run. Failed checks stop only the uniquely named client. `timeoutSeconds` bounds the workload process; setup/cleanup commands have separate20-second deadlines. These are best-effort guards, not hard byte quotas; polling, in-flight requests and delayed writeback can overshoot the floor. They do not guard Redis memory exhaustion. Keep headroom and conservative workloads.

Ctrl+C attempts to remove only the owned client container, then performs owned-data cleanup and writes an interrupted result. A local `.active-run` lock prevents overlapping launcher executions. After a forced process/host crash, inspect Docker for `perf-...-client` containers before removing the empty stale lock directory; no automatic recovery deletes unknown resources.

With `cleanupOwnedData: true` (default), cleanup runs after successful or failed execution, only after the client is confirmed absent and the original broker identity is rechecked:

- Kafka: deletes the exact freshly created topic.
- Redis: unlinks the exact freshly used stream key.
- RabbitMQ: deletes the exact freshly used stream queue.

Existing destinations, application topics, shared volumes and unrelated containers are never targeted. `cleanupStatus: requested` means the broker accepted the delete request, not that disk bytes have already been reclaimed. Redis AOF and Kafka/Rabbit storage can reclaim asynchronously; the launcher never compacts AOF, prunes Docker, deletes volumes or modifies Windows/WSL disk files. Failed cleanup is visible and returns a failing launcher exit status.

Set `cleanupOwnedData: false` deliberately to inspect data after a run; this accumulates storage. Per-run IDs and the manifest identify retained resources. No generic wildcard cleanup command is provided.

## Architecture and verification

`scripts/official-benchmark/config.mjs` contains schema and pure argv generation; `execution.mjs` contains reusable execution/safety; `scripts/official-benchmark.mjs` is a thin CLI. A future UI can reuse the first two without editing the Purchase API, after separately designing access to Docker execution.

Run `pnpm benchmark:test` for focused configuration/safety tests. Follow the tracked implementation and live-validation status in `OFFICIAL-BENCHMARK-CONFIGURATION-PLAN.md`.

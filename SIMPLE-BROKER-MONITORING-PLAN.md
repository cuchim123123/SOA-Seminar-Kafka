# Simple broker ingress monitoring plan

Date: 2026-11-02. Status: plan only; no services started or configuration changed.

## Recommendation

Reuse the existing Prometheus and Grafana stack. Add a Kafka exporter, a Redis exporter, and RabbitMQ's built-in Prometheus plugin. Provision one small dashboard named **Broker Ingress Benchmark**.

The purpose is to watch traffic while running existing benchmark CLI commands. Official tool output remains the final benchmark result. This work does not depend on implementing the earlier benchmark redesign and does not require rewriting a producer.

## 1. What exists and what is missing

Inspected repository files:

| File | Finding |
|---|---|
| `docker-compose.monitoring.yml` | Prometheus and Grafana already defined; default browser ports9090 and3005; named persistent volumes; Prometheus retention currently15 days. |
| `docker/monitoring/prometheus/prometheus.yml` | Application scrape jobs and a2s scrape of the custom runner at `host.docker.internal:3006`. No three-broker exporter jobs. |
| `scripts/render-monitoring-config.mjs` | Generates Prometheus configuration from a template. Changes must go into the template too, or regeneration will overwrite them. |
| `docker/monitoring/grafana/provisioning/` | Prometheus datasource and file-based dashboard provisioning already exist. Datasource interval currently15s. |
| `docker/monitoring/grafana/dashboards/ingress-benchmark.json` | Existing dashboard depends on `lab_ingress_*` metrics emitted by the custom Node runner. Independent CLI tools do not automatically emit these metrics. |
| `docker/ingress-benchmark/enabled_plugins` | Enables Streams and management, but not `rabbitmq_prometheus`. |
| `docker-compose.ingress-benchmark.yml` | Separate benchmark brokers; Kafka internal advertised endpoint `kafka-bench:29092`; Redis container port6379; RabbitMQ3.13. |

The attachment describes temporary containers running Kafka perf-test, RabbitMQ stream-perf-test and memtier. Their exact invocation/version is not recorded in the scripts searched. During implementation, inspect the actual commands, particularly whether memtier really executes XADD rather than its default workload. This is a metric-selection check, not another fairness audit.

Live Docker inventory did not return promptly during planning, so current container health, network membership and exposed metrics are not verified. Do not treat the following candidate queries as tested queries.

## 2. Minimal architecture

```text
Kafka benchmark broker → Kafka exporter ──────┐
RabbitMQ benchmark broker → built-in metrics ├→ existing Prometheus → existing Grafana
Redis benchmark broker → Redis exporter ────┘
```

Only two additional long-lived containers. No JMX agent, Node Exporter, cAdvisor, OpenTelemetry, Pushgateway, Loki or custom collection service for the first version.

Reuse `docker-compose.monitoring.yml` as the separate monitoring Compose file. Attach Prometheus and exporters to the existing benchmark network through a parameterized external network name, expected to be `ingress-benchmark_default`. Keep the monitoring default network so Grafana can continue resolving `prometheus:9090`. Grafana itself does not need broker-network access.

The external network must already exist. Fail clearly if it does not; do not silently create a second isolated network with a similar name.

Use these internal destinations after runtime verification:

- Kafka exporter → `kafka-bench:29092`, not the localhost-advertised host listener.
- Redis exporter → `redis://redis-bench:6379`.
- Prometheus → Kafka exporter port9308, Redis exporter port9121, RabbitMQ port15692.

Do not publish exporter ports to Windows unless troubleshooting requires it. Preserve application scrape jobs, application dashboards and the old custom-runner dashboard. Add a new dashboard with a unique UID.

## 3. Metric choices

### Kafka: approximate ingress from partition offsets

Use the Kafka exporter's current partition offset gauge, restricted to benchmark topic names. The exporter documents `kafka_topic_partition_current_offset` as a **gauge**, not a counter. Therefore do not apply `rate()` as though it were a message counter. [Kafka exporter documentation](https://github.com/danielqsj/kafka_exporter).

Candidate query after verifying names and labels:

```promql
sum(
  clamp_min(
    deriv(kafka_topic_partition_current_offset{
      job="benchmark-kafka", topic=~"$topic"
    }[10s]),
    0
  )
)
```

Panel label: **Kafka — offset advance/s (approximate ingress)**.

Verify the exporter returns fresh offset values often enough; a2s Prometheus interval does not override exporter caching. Check compatibility with the installed Kafka version, collection time and selected topic filters. Disable unnecessary consumer-group collection if the selected exporter supports it.

Offset movement is not producer-confirmed throughput or ACK latency. Transactions/control records, topic recreation and truncation can invalidate a one-offset-equals-one-message interpretation. Keep the seminar workload append-only and topic identity stable during a chart interval. Suppress or annotate reset/recreation intervals; clamping a negative slope alone does not make those intervals valid.

Do not add JMX just to make this chart more precise. If a producer already exposes a verified metrics endpoint, it can be considered later without replacing the simple broker view.

### RabbitMQ: verified native Stream publish/confirm metric

Enable `rabbitmq_prometheus` in the benchmark broker's persistent plugin list. Keep existing plugins. This may require recreation/restart of only `rabbitmq-bench`; schedule it between benchmark runs. Do not alter persistence, replication or resource limits.

Inspect the actual3.13 metrics endpoint while a native Stream publisher is active. Select a Stream-specific published or confirmed counter only after checking its HELP/TYPE, labels and observed movement. Generic classic-queue/channel metrics are not sufficient proof. RabbitMQ documents the built-in endpoint and metric families: [RabbitMQ3.13 monitoring documentation](https://www.rabbitmq.com/docs/3.13/prometheus).

Query shape, deliberately not an invented metric name:

```promql
sum(rate(<verified_stream_counter>{<verified_benchmark_labels>}[10s]))
```

Prefer a stable broker-level cumulative counter. If only per-publisher counters are available, apply `rate()` to each series before summing, and test publisher disconnect/reconnect. An aggregate of currently connected publishers can decrease when clients disappear and cannot safely be treated as an ordinary monotonic counter.

Panel label states the observed event, for example **RabbitMQ Streams — messages confirmed/s**. If stable metrics cannot be obtained in this version, stop and document that specific limitation. The fallback is the smallest supported Streams metric endpoint/family; do not substitute unrelated queue traffic. Stream-perf-test metrics are a secondary option only if broker metrics fail, with the short-lived-endpoint limitation disclosed.

### Redis: XADD command calls

Use the Redis exporter's command statistics filtered to XADD. Candidate query:

```promql
sum(rate(redis_commands_total{job="benchmark-redis",cmd="xadd"}[10s]))
```

Verify the exact label case and whether the metric appears only after the first XADD. Exporter source defines per-command counters with a `cmd` label: [Redis exporter source](https://github.com/oliver006/redis_exporter/blob/master/exporter/exporter.go).

Panel label: **Redis Streams — XADD calls/s**. This is not total Redis commands/sec, durable writes/sec or necessarily successful insertions/sec. Inspect failure/rejection command counters during verification; do not silently claim all calls succeeded.

Command statistics are instance-wide, not per stream key. The dedicated Redis benchmark instance makes this useful, but concurrent XADD workloads on that instance will be combined. Do not add expensive key scans or derive ingress from stream length, which retention can change.

## 4. One readable dashboard

Name: **Broker Ingress Benchmark**. Default time range: last5 minutes.

Layout:

1. Kafka offset advance/s.
2. RabbitMQ verified Stream published/confirmed messages/s.
3. Redis XADD calls/s.
4. Full-width comparison chart with all three series and their actual meanings in the legend.

Add a small status row for the three monitored brokers/exporters and one short description: “Live broker activity; metric semantics differ. Use CLI output for final benchmark results.”

For exporter-backed targets, distinguish scrape success from broker availability. Prometheus `up=1` alone can mean the exporter is reachable while its broker is not. Use supported backend-health metrics or show collection errors too.

Use2s scrapes for these jobs only, and2s dashboard refresh. Configure Grafana's minimum refresh interval to2s, since its default is5s. [Grafana configuration documentation](https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/).

Keep application datasource defaults unchanged; set a2s minimum interval/query step on the new dashboard panels. Start with10s rate/slope windows for readable lines, increasing to20s if noisy. The chart is a smoothed view, not instantaneous per-second measurement. After traffic stops, the displayed rate may take roughly one window to return toward zero.

Use gaps or a visible unavailable state for missing metrics. Do not use unconditional `or vector(0)` to make broken targets look idle. A new Redis instance may legitimately have no XADD series until its first call; explain that state.

Do not add latency, payload MB/s or CPU dashboards in this first version. The chosen broker metrics cannot accurately produce uniform publish latency or payload throughput without more instrumentation.

## 5. Storage and overhead

Keep collection narrow: benchmark topic/stream filters where supported, command-level Redis statistics, no per-key scans, and no high-cardinality labels for every message or transient publisher unless required by the verified Rabbit metric.

Proposed configurable seminar settings:

- Prometheus retention time24h and retention size512MB.
- Bound Docker logs, for example10MB ×3 files per monitoring container.
- Grafana/Prometheus/exporter memory and CPU usage checked during smoke tests; avoid arbitrarily tight limits before measuring normal usage.
- Initial combined monitoring memory target below approximately1GB; treat this as a budget to verify, not a guarantee.

Prometheus retention-size is not a strict whole-volume limit: WAL/head data and overhead need additional room. Verify Docker disk space before starting; don't promise that monitoring uses exactly512MB.

Retention applies to all series in the reused Prometheus instance. Make the24h/512MB settings explicit environment options and document that they expire older monitoring history. Keep existing volumes; no volume removal, Docker pruning or WSL/VHD manipulation. Do not delete broker data to make monitoring fit.

## 6. Small implementation sequence

| Step | Work | Completion check |
|---|---|---|
|1|Inspect active Compose project/network, ports, images, free disk and actual benchmark CLI commands.|Confirmed endpoints and no duplicate monitoring services.|
|2|Extend existing monitoring Compose with two pinned exporter images, external network, parameterized retention and bounded logging.|`docker compose config` validates; exporters resolve brokers internally.|
|3|Add RabbitMQ Prometheus plugin and only required metric-family settings.|Streams-specific samples observed; existing broker settings preserved.|
|4|Update Prometheus template and renderer variables, then regenerate the output file.|New jobs survive regeneration; existing jobs remain.|
|5|Inspect real samples and finalize the three queries.|Each query returns the intended metric with verified type/labels.|
|6|Provision the four-chart dashboard and2s refresh support.|Dashboard appears automatically using the existing datasource.|
|7|Run bounded smoke checks and write a short usage guide.|Lines rise, settle after traffic, and report unavailable when a target fails.|

Expected touched files:

- `docker-compose.monitoring.yml`
- `docker/monitoring/prometheus/prometheus.yml.template` and generated `prometheus.yml`
- `scripts/render-monitoring-config.mjs`, only as needed for new substitutions
- `docker/ingress-benchmark/enabled_plugins`
- `docker/ingress-benchmark/rabbitmq.conf`, only if verified collection needs settings
- new `docker/monitoring/grafana/dashboards/broker-ingress-benchmark.json`
- `.env.example` and a short README section

Existing datasource/dashboard provisioning can remain intact. No application or benchmark producer source changes are planned.

## 7. Verification without a large benchmark

First validate Compose, Prometheus configuration with promtool, and dashboard JSON. Then inspect all targets and query actual Prometheus samples.

For each broker, use an isolated test destination and a bounded low-rate workload lasting20–30s, with a strict message/byte cap. This tests visualization without accumulating large data volumes. It is a smoke test, not a performance result. Choose the exact command after inspecting installed tool capabilities, including whether memtier's workload is XADD.

Observe10s idle before and15–20s after each test. Verify:

- Correct panel rises during matching traffic; unrelated broker panels remain idle.
- Labels and topic filters match the test destination.
- CLI exit does not take the exporter endpoint down.
- RabbitMQ publisher disappearance does not create a false rate spike.
- Rates settle after the query window; missing series are not disguised as zeros.
- Topic creation/recreation or first XADD does not produce a misleading giant spike.
- Queries return real samples rather than merely passing syntax validation.
- Monitoring CPU, RAM, scrape duration and disk growth are reasonable.

Do not reset counters or delete existing topics/streams. Any later cleanup is limited to explicitly identified smoke-test resources; retaining a small bounded test dataset is acceptable and avoids adding destructive cleanup to this task.

For real benchmark runs shorter than several scrape intervals, the chart may miss or smooth the burst. Document this limitation instead of changing their commands or claiming the graph matches the CLI's exact throughput.

## 8. Planned user workflow

After implementation, verify and document the existing Compose project identity so these commands operate on the same stack:

```powershell
# Brokers/network must already be available.
node scripts/render-monitoring-config.mjs
docker compose -f docker-compose.monitoring.yml up -d
```

Open Grafana at `http://localhost:3005` unless `GRAFANA_PORT` overrides it, then select **Broker Ingress Benchmark**. Existing credentials/anonymous viewer settings apply; environment changes do not automatically reset credentials in an initialized Grafana volume.

Run existing benchmark commands unchanged and watch the appropriate chart.

```powershell
# Stop monitoring without deleting data or stopping benchmark brokers.
docker compose -f docker-compose.monitoring.yml stop
```

The final README should show the verified local URL, login behavior, start/stop commands, expected chart delay and three metric definitions. No claim of working runtime queries should be made until the implementation smoke checks pass.

## Done criteria

One existing monitoring stack, two new exporters, RabbitMQ's native plugin, one provisioned dashboard, four understandable charts, bounded retention/logging, and no changes to the Purchase pipeline or benchmark methodology. The only unresolved technical selection is the exact reliable RabbitMQ3.13 Stream metric; resolve that through real samples before finalizing its panel.

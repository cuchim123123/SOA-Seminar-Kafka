# 07 — Live Demo Runbook and Observable Proof

The seminar is about **Kafka mechanics**, not loyalty features. Each demo has a starting state, an operator action, evidence to capture, an expected result, and a limit on what can legitimately be claimed. Create `infra/scripts` commands for these scenarios in their corresponding phases.

## Shared demo rules

- Reset the **disposable** dataset deliberately before scripted trials. Do not reset unrelated topics/groups or persistent volumes without operator confirmation.
- Give each event a stable ID and correlation ID; log them with actual Kafka `{topic,partition,offset}` values.
- Show at least one Kafka UI/CLI observation and one application-level observation when possible.
- Observability must report **real** offsets/assignments/ISR; wireframes and screenshots must be labelled if simulated.
- Do not benchmark while a UI animation or manual producer timer artificially limits throughput without saying so.
- Use stable synthetic customers; no production or personal data.

## D1 — Basic publish and consume (Phase 1)

**Arrange:** KRaft single broker, one partition; producer-api, loyalty worker, Kafka UI.  
**Act:** `POST /api/v1/demo/purchases` with U001 and 12000 cents.  
**Observe:** HTTP 202, Kafka topic record key `U001`, envelope eventId, broker-assigned partition and offset, worker receipt and +12 synthetic result.  
**Pass:** values correlate by `eventId`.  
**Proves:** producer, broker append, consumer flow. **Does not prove:** database durability or replication.

## D2 — Key-based partitioning and ordering (Phase 2)

**Arrange:** reset disposable topic to 6 partitions, stable partitioner.  
**Act:** publish U001 seq1/2/3 and simultaneous records for at least 20 other customers.  
**Observe:** U001 records all in one partition and ascending offsets; workers calculate 82 points when all three are applied. Different customers spread across partitions, though collisions are expected.  
**Pass:** no U001 sequence gap or reversal; partition metadata from Kafka.  
**Proves:** per-partition order. **Does not prove:** globally ordered Kafka stream or automatic correct order across concurrent DB writes.

## D3 — Independent consumer groups and lag (Phase 3)

**Arrange:** Loyalty and Analytics on `purchase.events`, different group IDs.  
**Act:** pause **Analytics** consuming its partitions; publish 100 new events; leave Loyalty running; resume Analytics.  
**Observe:** Loyalty committed offsets advance; Analytics committed offsets remain stationary while log-end moves, so Analytics lag rises; resume leads to lag reduction.  
**Pass:** groups process same records independently; analytics recovers from its own committed offsets.  
**Proves:** group independence, offsets, lag/catch-up. Paused consumer **remains** a group member.

## D4 — At-least-once and durable consumer idempotency (Phase 4)

**Arrange:** Postgres + outbox, consumer manual offset, per-consumer processed-event table. Fault switch injects process crash **after** DB transaction commits and **before** Kafka offset commits.  
**Act:** send one known purchase event to Loyalty, trigger crash, restart worker.  
**Observe:** DB contains one processed-event row and one applied points update. Kafka committed offset initially not advanced, record redelivered, application reports `duplicate_skipped`, then offset advances.  
**Pass:** attempts >= 2, applied mutation count == 1.  
**Proves:** at-least-once/redelivery and application idempotency. **Does not prove:** Kafka automatically makes Postgres exactly-once.

## D5 — Outbox producer reliability (Phase 4)

**Arrange:** durable purchase/outbox.  
**Act A:** stop Kafka broker temporarily, submit purchase, show committed purchase and pending outbox, restart broker and publisher retries successfully.  
**Act B:** inject publisher crash just after ack but before `published_at`, restart publisher.  
**Observe:** event may appear twice in Kafka with same `eventId`; consumer state remains exactly once.  
**Pass:** accepted source purchase never lost and duplicates do not double-update.  
**Proves:** why outbox + idempotency are needed. Outbox is a DB/application pattern, **not** a native Kafka guarantee.

## D6 — Retained log and intentional replay (Phase 5)

**Arrange:** seed U001 P001=$120/P002=$200/P003=$100; current projected points=82.  
**Act:** invalidate P001 (fourth event), freeze new input, drain outbox, wait for groups to reach recorded end offsets, run new replay group from earliest **retained** offsets through a fixed high watermark.  
**Observe:** group ID `replay-<runId>` with its own offset progress; two-pass recomputation into isolated staging; corrected U001 points=40; original Loyalty group offsets unchanged.  
**Pass:** output equals 40 and not 70; new group read real Kafka records, no duplicate external side effect.  
**Proves:** log retention, offsets, consumer replay. **Does not require:** adopting Event Sourcing or a permanent event store.

**Retention edge test:** configure an isolated short-retention topic/dataset, wait for segment cleanup and verify old history is no longer available. Because cleanup is asynchronous, capture actual log-start offsets; do not promise precise wall-clock deletion.

## D7 — Horizontal worker scaling (Phase 6)

**Arrange:** 6-partition topic, reproducible high-cardinality customer load, Loyalty group with one worker, separate worker processes/containers.  
**Act:** measure 1 worker, then scale 3, 6, optionally 7, using same group ID.  
**Observe:** real group assignments, active/idle members, total events/s, latency, lag over time, CPU and Postgres resource constraints.  
**Pass:** valid Kafka partition ownership and eventual state equality across runs; report measured performance.  
**Proves:** per-group partition parallelism and rebalancing. **Does not promise:** 6× speedup or multi-host performance on a shared laptop.

## D8 — Worker failure and group rebalance (Phase 6)

**Arrange:** 3 workers, 6 partitions, nonzero backlog.  
**Act:** kill one Loyalty worker process/container, then replace it.  
**Observe:** membership changes, partition ownership reassigns after failure detection, lag/throughput transient, processing resumes.  
**Pass:** no lost state updates or duplicate effects; assignments match broker/client truth.  
**Proves:** consumer-side fault recovery. **Not** partition replica leader election.

## D9 — Replicas, ISR and broker leader election (Phase 7)

**Arrange:** **fresh** KRaft combined-mode 3-node profile; six-partition topic RF3 minISR2; producer `acks=all`, workload running. Record leaders and ISR.  
**Act:** identify broker leading a chosen partition; stop that broker.  
**Observe:** KRaft controller quorum remains 2/3; affected partitions elect eligible new leaders; ISR shrinks from 3 to 2, producer retries metadata and continues after election; resume broker and observe follower catch-up.  
**Pass:** topic metadata proves leader movement, RF=3 and ISR changes; consumer/producer resumes without manual topic recreation.  
**Proves:** broker-side fault tolerance in a single-host lab.

## D10 — Insufficient ISR / write availability (Phase 7)

**Arrange:** RF3/minISR2.  
**Act:** use a documented controlled fault to make a partition have fewer than 2 in-sync replicas.  
**Observe:** `acks=all` produce returns an appropriate error such as insufficient replicas; no false success.  
**Caveat:** stopping two combined broker/controller nodes also loses the three-voter controller quorum. The outcome is **not purely an ISR demonstration**. For a clean minISR experiment, isolate a follower/replication issue while keeping controller quorum alive, or accurately explain the coupled failures.  
**Pass:** report exactly what failed and why; never confuse lost quorum with minISR threshold alone.

## D11 — Throughput and lag experiment (Phase 8)

**Arrange:** deterministic load dataset; record hardware and Docker limits.  
**Act:** run each worker scale with the same event count, rate and DB state; warm-up and steady-state periods; repeat trials.  
**Observe:** produced records/s, committed processed records/s, p50/p95 end-to-end processing latency, peak and drain time of lag, CPU/RAM.  
**Pass:** reproducible measurements with graphs; explain bottlenecks, skew and Kafka partition cap.  
**Do not claim:** theoretical throughput without measurement.

## Suggested 12–15 minute seminar flow

1. D1 + D2: basic pipeline and ordering (2–3 minutes).
2. D3: pause group, lag, catch-up (2 minutes).
3. D7 + D8: scale/rebalance worker, compare offsets (3 minutes).
4. D4: forced duplicate, idempotency (2 minutes).
5. D6: replay after invalidation (2 minutes).
6. D9: broker leader election / ISR (3 minutes).

Prepare a short fallback recording **only** for demo continuity; distinguish recordings from live measurements.

## D12 — Kafka throughput and latency observability (Phase 8)

**Setup:** a clean six-partition Kafka dataset and fixed deterministic producer workload. Record host hardware, broker/client versions, producer concurrency, payload size and acknowledgment settings.

1. Run a bounded fixed-load trial with one Kafka producer/client adapter; capture confirmed messages/s and payload MiB/s.
2. Display producer acknowledgment latency and consumer delivery-to-commit p50/p95/p99 **from measured histograms**.
3. Capture real per-group lag, resource usage, rejected/failed/ambiguous events and drain time.
4. Repeat at least 3 times, show median and variance, and reconcile expected vs confirmed vs processed event IDs.

**Proof:** a JSON manifest and result file for every trial; graph rendered from saved files. Do not show a fabricated throughput number or claim a universal Kafka maximum. See docs 14 and 16.

## D13 — Kafka vs Redis Streams vs RabbitMQ Quorum (Phase 9)

**Setup:** run the **same exact serialized event payloads** and run profile on each transport, sequentially on the same host with clean broker resources.

1. Kafka transport-only baseline: collect three trials.
2. Redis Streams: `XADD`, `XREADGROUP`, `XACK`, and pending-recovery configuration; collect three trials.
3. RabbitMQ Quorum Queue: publisher confirms and manual ACK with explicit prefetch/topology; collect three trials.
4. (Optional) RabbitMQ Streams/Super Streams using the **native** stream API, not Quorum semantics.
5. Compare broker-confirmed messages/s, unique processed messages/s, producer and consumer latency p50/p95/p99, errors, resource usage and native backlog; show durability/replication/topology caveats.
6. Show that Quorum Queue does **not** offer replay of already-acked records, whereas Kafka, Redis Streams and RabbitMQ Streams have retained-data reads subject to policies.

**Proof:** immutable manifests, identical corpus checksum, raw samples, per-run success/failure accounting and mode-specific, caveated charts. See docs 13–16.

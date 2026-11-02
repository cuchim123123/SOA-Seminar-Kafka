# Simulation Playground

The playground is an in-memory teaching model. It does not send traffic to the real Purchase pipeline or official benchmark brokers.

Choose a scenario, enter a customer key in the message composer, and send an event. Sending while paused queues work. **Run/Resume** advances virtual time; **Step event** executes one queued event and stays paused. Particle positions use that same clock, including speed changes. Hiding the browser tab pauses the model.

For the Minimal scenario, step through append, poll/start, processing completion, database persistence and N+1 commit. Open **Evidence** to inspect the record offset, group, partition and message journey. Only the latest 100 events are retained; the panel also reports the total recorded count. Filters never claim to cover evicted evidence.

Use **Build system** to click or drag new components onto the canvas. Connect supported handles and use the inspector to configure the selected component. Invalid drafts remain editable; execution is blocked until their listed issues are repaired.

- **Add replica** adds a consumer to the same group, sharing its subscribed topic, offsets and database sink. Replicas beyond the partition count are idle. Scaling pauses, applies membership changes, reconciles old deliveries and stays paused.
- **Add independent projection** adds a separate consumer group and its own database. It uses the selected consumer, or the first consumer, as a template. New groups follow the configured initial-offset policy; use a group-specific earliest reset to read older records.
- Moving, selecting, zooming or fitting the canvas does not rebalance or pause. Structural edits and mechanics changes pause the run. Fault actions remain available while running.
- Pausing consumer polling stops new deliveries; already accepted handler work can finish. Crash/recover models membership failure without deleting a component.
- **Message exercises** contains a duplicate retry and targeted poison injection. Duplicate retries the previous event ID through its original producer. Poison without a DLQ blocks that consumer; a DLQ result persists before manual commit.

| Action | Preserved | Cleared | Playback |
|---|---|---|---|
| Navigate away/back | Diagram, settings, viewport and complete in-memory run | Nothing | Paused |
| Refresh | Validated diagram, settings, source/key/burst preferences and viewport | Messages, offsets, queued work, faults, counters, evidence, dedup receipts | Fresh paused run |
| Reset run | Customized diagram and effective settings | Transient execution state | Paused at time zero |
| Restore scenario | Selected scenario's declared defaults | Custom diagram/settings and transient execution | Fresh paused run |
| Clear canvas | Ability to load a scenario | Components, wires and transient execution | Paused |

Diagram autosave is debounced and writes only when its serialized configuration changes. Browser storage key: `kafka-lab.simulation.diagram.v1`. Denied/full storage reports a recoverable error; the session remains usable. Corrupt or unsupported saved documents are preserved with autosave paused. Download the original or explicitly replace it; replacement first saves a recovery copy. An incomplete supported diagram can be restored for repair, but cannot execute while invalid.

Effective controls include topic partitions/replication/minISR, acknowledgments, keyed or round-robin routing, manual/early-auto commit, initial-offset policy, database idempotency, group assignment, subscriptions, membership, consumer processing delay and database write delay. Global mechanics controls are authoritative over scenario/node labels. Acks=all applies minISR; acks=1/0 still require an online leader but do not enforce minISR. This is simplified replication, not real disk/consensus timing.

Database engine/schema labels do not change the storage implementation. Consumer concurrencySlots/failureRate, producer messageRate/clientDelayMs, brokerLatencyMs, outbox retentionCount and CDC pollIntervalMs/batchSize do not control execution. The UI does not expose them as working capacity/timing controls. The current model has one processing slot per consumer and deterministic poison exercises. It has no automatic CDC broker retry or DLQ replay service. Use the independent official benchmark tools for throughput comparisons.

Verification commands: `pnpm format:check`, `pnpm lint`, `pnpm typecheck`, `pnpm test:unit`, `pnpm --filter @lab/demo-ui build`. The frontend typecheck now checks its actual project references with strict mode enabled. Simulation regressions live under `apps/demo-ui/test/simulation`.

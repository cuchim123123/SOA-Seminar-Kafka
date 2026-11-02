# Simulation Playground enhancement — finalized scope plan

Status: product decisions settled; implementation-ready plan awaiting implementation approval. Source review and browser inspection performed2026-11-02. Existing simulation suite:5 files/35 tests passed. No measured FPS or performance attribution is claimed yet.

## Agreed product direction

- Presenter-first, with a fully usable expandable builder. Users must be able to scale consumer replicas, add databases and other supported components, and construct meaningful variations of the supplied blueprints.
- Automatically pause before structural edits, including adding/removing consumer replicas. Apply scaling and remain paused until explicit Resume. Live fault actions remain available.
- Save diagram/settings locally. On navigation away, pause and retain the complete current run in memory; returning resumes viewing that paused run. After a full browser refresh, restore only diagram/settings into a fresh paused runtime. Full persisted runtime snapshots were not selected.
- Support multiple database projections through independent consumer groups, each writing to one database sink. Replicas within a group share that logical sink; they do not create independent copies of the projection. One consumer writing to multiple database sinks and distributed partial-write/retry behavior are deferred.
- Preserve Kafka teaching as the central purpose. Synthetic business operations stay minimal. Simulation must remain isolated from real services and the Purchase pipeline.

All requested product decisions are resolved. The finalized scope below is ready for review; answering these scope questions does not itself authorize implementation.

Approved persistence contract: autosave versioned diagram, component settings, edges, scenario selection and viewport locally. Navigation away pauses the clock and rendering work but retains the current runtime in memory, including messages, logs, offsets, counters, deduplication state and scheduled work; return displays that same paused run. After browser refresh, restore the saved diagram/settings into a fresh paused runtime with an explicit banner. Runtime state is not serialized to persistent storage, and no elapsed wall-clock time is caught up while away. Do not label a fresh runtime as a resumed run. Engine/session ownership must outlive the route component, while animation/subscription lifetimes remain scoped to the visible playground.

## Findings and evidence

| Priority | Finding | Relevant implementation | Required treatment |
|---|---|---|---|
| P0 | Consumer offset committed before scheduled database write | `engine/handlers/sim-event-handlers.ts`, COMPLETE_PROCESSING versus WRITE_DATABASE | Make write/receipt/commit ordering explicit; manual commit follows successful handler persistence. Failure/retry must not produce false committed state. |
| P0 | RF control recreates topic partitions/logs | `store/useSimulationStore.ts`, setTopicReplicationFactor; `domain/kafka-cluster.ts`, createTopic | Separate topic creation from replica reassignment; preserve records/offsets or reject unsupported transitions explicitly. |
| P0 | Reset rebuilds cluster but incompletely resets group members | `engine/sim-engine.ts`, reset | Reconstruct a coherent runtime from the current diagram/configuration; test crash/pause/processing combinations. Source review confirms state-boundary mismatch; exact consequences need focused reproductions. |
| P0 | Node geometry changes invoke group rebalance/status changes | store topology effect and `SimEngine.setTopology` | Separate presentation changes from semantic topology reconciliation. Moving/selection/measurement must not alter assignments or processing. |
| P1 | Repeated engine construction and broad React updates every animation frame | useRef(new SimEngine()), syncFromEngine, animation loop | Lazy engine lifetime; stable snapshots; publish only changed information at appropriate cadence. Profile before setting tuning values. |
| P1 | Particle animations run in wall time, engine runs in virtual time | `ui/canvas/SimFlowEdge.tsx`, SVG animateMotion | Derive particle progress from the same virtual clock; pause/speed/step must agree. |
| P1 | Edges/group wrappers rebuilt frequently; each edge filters all tokens | `ui/SimulationLab.tsx` | Index tokens once, preserve references for unchanged nodes/edges, separate animation updates from inspector/timeline updates. |
| P1 | Duplicate brokers copy broker identity; deletion can leave model brokers | duplicateNode/deleteNode and setTopology | Role-specific cloning and atomic identity/membership reconciliation. |
| P1 | Only first connected database receives normal writes | COMPLETE_PROCESSING uses edges.find | Enforce the agreed sink cardinality; never display accepted wires that are silently ignored. |
| P1 | Topic/group hardcoding and split configuration authority | operations/store/offset menu use purchases or loyalty-v1; global and per-node settings coexist | One authoritative value per setting and explicit scope. Complete supported paths or mark/reject unsupported combinations. |
| P1 | Crowded action hierarchy and duplicated entry points | PlaybackBar, MechanicsToolbar, palette, inspector | One location per action; progressively disclose advanced settings. |
| P2 | Fixed minimum height, open palette and overlaid inspector shrink canvas | SimulationLab layout | Responsive canvas-first layout, optional palette, dedicated inspector area and collapsible evidence panel. |
| P2 | Timeline is bounded to100 events but lacks selection/filtering and semantic disclosure control | SimEventTimeline | Retain bounded memory; accessible disclosure, event filtering and message correlation. Show retained versus total counts accurately. |

Passing engine tests do not cover browser rendering, editing workflows, persistence, or all transition combinations. No claim that every advertised inspector field currently changes engine behavior: field-by-field audit is required.

## Target workspace

```text
Scenario name / scenario picker               Build system / Reset run
Run or Pause | Step event | Speed | Virtual time | Send message / burst
------------------------------------------------------------------------
Optional component drawer | Large canvas | Contextual inspector
                          |             | Overview / Settings / Evidence
------------------------------------------------------------------------
Collapsible evidence panel: event list, selected message and outcome
```

- Keep the existing global application sidebar. Avoid another permanently open navigation rail.
- Default to a readable presenter scenario rather than fitting the densest12-node graph into every viewport. Keep the enterprise blueprint available.
- One scenario picker. One failure-injection entry point. No duplicated Poison Pill buttons.
- Keep common actions visible; place duplicate/poison demonstrations in an explicit advanced action menu.
- Node cards show identity, current state and a few useful counters. Detailed configuration/actions belong in the inspector. Node quick actions, if retained, must invoke the exact same commands.
- Selecting a consumer group exposes replica count, membership, assignments, lag and its database pipeline. Distinguish consumer replicas from Kafka partition replicas.
- Selecting a database explains which group writes to it. A database engine label is a visual simulation choice, not an implementation of PostgreSQL/MongoDB/Redis behavior.
- Make status text precise: no partition assignment is idle; paused is not crashed; pending is not committed; an animated token is not proof of a durable write.
- Fault actions use controlled language such as Crash/Recover; remove ambiguous wording such as Destroy where the action does not delete a node.
- Keyboard-operable menus/dialogs, visible focus, labelled fields, sensible touch targets and reduced-motion support. On narrow screens use a focused drawer/sheet rather than simultaneous palette/canvas/inspector columns.

## Interaction and editing rules

### Commands and state ownership

Preserve React Flow and the existing discrete-event engine/priority queue. Introduce cohesive commands at the simulation boundary, not a new service or generic plugin framework. UI components dispatch intents; the engine owns runtime transitions; the diagram owns geometry and configuration. UI snapshots are read-only projections.

Inventory every visible setting/action against its actual handler. Resolve global/per-node conflicts explicitly. Preserve versioned event identity and stable IDs within a run; tie pending work to the relevant consumer assignment/run generation so stale completions cannot mutate replaced state.

### Structural edits

- Add/remove/rewire components and change group/topic membership through validated commands. Pause before applying them, report what changed, and require explicit Resume.
- Moving, resizing, selecting and fitting nodes are presentation edits: no rebalancing, queue cancellation or clock changes.
- Consumer cloning creates a unique instance in the same group; creating an independent projection creates a new group. Make this distinction visible instead of relying on generic Duplicate.
- Broker cloning allocates a unique broker ID. Adding a broker does not silently change RF or redistribute existing logs.
- Reject duplicate edges, invalid port roles and unsupported cardinalities with inline feedback. Do not rely on blocking browser alerts.
- Preserve compatible runtime state. Define explicit invalidation/recovery for queued work referencing deleted members/sinks. Unsupported destructive changes must explain the need to reset rather than silently erase records.
- Partition decreases and topic replacement require explicit reset/rebuild semantics; do not pretend Kafka supports arbitrary in-place shrinking.
- Topic/group scope must be explicit. If the first version supports only one subscribed topic per group, validate and label that constraint rather than implying multi-topic assignment support.

### Playback/reset

- Run resumes virtual time; Pause freezes processing and particle progress. Sending while paused queues intent and does not secretly resume playback.
- Step executes exactly one discrete event and stays paused; disable or pause coherently when invoked during Run. Explain that Step event is lower-level than processing a complete message.
- Reset run preserves the edited diagram and settings, clears all runtime state and returns to time0 paused.
- Restore scenario replaces diagram/settings and requires confirmation when custom edits would be lost. Clear canvas is a builder action, not adjacent to Run.
- Returning to a hidden/background tab must not create a huge unexplained visual jump. Define bounded catch-up behavior while preserving logical event order.

### Failures and persistence correctness

- Normal manual processing: consume → handler work → successful sink write/dedup state → commit N+1. Do not release partition ordering locks too early.
- Test consumer crash before write, after write/before commit, and during rebalance. Retries must demonstrate the difference between at-least-once delivery and database idempotency.
- A deliberate unsafe auto-commit demonstration must be visibly labelled and actually use different timing; it cannot be just a toggle label.
- Poison handling must distinguish retry/blocking versus a configured DLQ route and its commit policy. Do not imply the real application's unsupported DLQ/redrive behavior exists.
- Restored diagrams are validated and migrated by schema version. Corrupt/unavailable storage must not blank the app. Offer recovery to a safe scenario without silently destroying the stored diagram. Export/import is useful but optional for the first delivery.

## Delivery sequence and acceptance

### 1. Reproduction and baseline

Build a capability matrix for each component/setting/action and every supplied blueprint. Record correctness defects with focused reproductions. Profile idle, Run without messages, single-message flow, bursts, drag/zoom, inspector open and longer sessions on the same machine.

Use representative small/enterprise graphs and a larger builder graph. Record render commits, frame-time distribution, long tasks, token/queue sizes and retained history. Identify event-budget overload behavior (currently20 due events per frame); do not derive simulated throughput from browser FPS.

Exit: reproducible cases, known unsupported features and measured baseline. Agree performance acceptance from these observations; aim for smooth50–60Hz presentation on the chosen baseline without promising arbitrary graph sizes.

### 2. Correctness and editable topology

Fix commit/write ordering, runtime reconstruction/reset, identity allocation, topology/geometry separation, duplicate wires, pending-event invalidation, and supported topic/group scope. Implement paused consumer scaling and independent-group/single-sink projection rules. All members of a group must use the same subscribed topic and logical database sink; incomplete intermediate builder edits may exist as drafts but must not execute until validated. One database may hold multiple separately identified projection domains only if explicitly modelled; the first enhancement should create distinct sinks for distinct independent projection pipelines.

Exit: moving nodes cannot change runtime; RF changes cannot erase records; reset after crash/pause is coherent; adding/duplicating/removing components cannot leave ghost members or writes; offsets remain group/topic/partition scoped.

### 3. Clock and rendering performance

Lazy engine creation; clear engine-to-view update boundary; immutable/stable snapshots for changed slices; virtual-clock particle interpolation; indexed token lookup; memoized group geometry; bounded visual token representation and timeline rendering. Only introduce a Web Worker or an additional state library if profiling demonstrates a remaining need.

Logical events may never be silently discarded to improve FPS. Any aggregation or cap applies to visualization and is visibly counted. Separate simulated completion counts from rendered particle counts.

Exit: pause freezes particles;0.5x/1x/2x change processing and movement consistently; Step remains accurate; idle/paused views stop unnecessary frame work; before/after profiles show actual improvement.

### 4. Presenter workspace and flexible builder

Implement the agreed layout and command grouping. Add discoverable Add replica/Add independent group/Add sink flows, validated connection feedback, readable node summaries, contextual inspector, one scenario picker, one message/fault entry point, and evidence selection/filtering. Preserve manual drag/wire editing for supported layouts.

Exit: a presenter can choose a scenario, send messages, explain lag/assignments, scale a group, crash/recover a component and inspect evidence without hunting through multiple toolbars. Builder edits have real supported simulation effects.

### 5. Diagram persistence and accessible/responsive behavior

Persist the approved diagram/settings contract, retain the paused in-memory session across route navigation, validate fresh-runtime restoration after browser refresh, distinguish Reset run from Restore scenario, and verify both lifecycles. Add keyboard/focus and reduced-motion handling. Test storage failures and old document versions.

Exit: customized topology/settings survive refresh; route navigation retains exactly the same paused run without hidden ticks or catch-up; browser refresh starts a clearly labelled fresh paused run and never claims persisted messages/offsets.

### 6. Integrated acceptance

Automate meaningful engine/command regressions and browser workflows. Add DOM/browser test support only if needed, explaining the dependency first. Retain existing simulation tests and run repository format/lint/typecheck/unit gates plus production UI build. Real Kafka verification only when relevant infrastructure is active; simulation correctness must not require contacting real brokers.

Minimum matrix:

- Every blueprint: reset → run → send → pause → step → resume.
- Clock speeds, background-tab return, paused dispatch and empty-queue step.
- Same group replicas versus independent groups; excess consumers idle.
- Broker crash/recover, minISR/acks combinations and replica configuration without record loss.
- Consumer crash at persistence/commit boundaries; rebalance during pending processing.
- Duplicate events with idempotency on/off; poison with/without DLQ.
- Geometry edits during Run; structural pause/edit/resume; deleted endpoints and duplicate wires.
- New group/topic/DB supported paths, invalid settings and missing routes.
- Navigation, refresh, storage denied/corrupt/version mismatch, scenario replacement and reset.
- Narrow/wide layout, inspector/palette/timeline combinations, keyboard and reduced motion.
- Long/bursty runs: bounded visual/history work, preserved logical events and responsive controls.

Exit: observed defects fixed or explicitly unsupported with clear UI validation; no misleading Kafka guarantees; measured performance improvement and a repeatable presenter walkthrough. Keep runtime/resource bounds explicit rather than claiming an unlimited Kafka emulator.

## Implementation boundaries

No real application, CDC pipeline, official benchmark runner, Prometheus/Grafana or infrastructure change is required. Preserve unrelated working-tree edits. Do not remove freeform design to simplify the presenter UI. Do not promise arbitrary new component types until their execution semantics exist. Keep code/tests and short progress notes aligned with approved decisions; begin implementation only after this plan is approved.

## Concrete control placement proposal

| Location | Contents | Interaction rule |
|---|---|---|
| Scenario header | Scenario picker, custom/modified indicator, Build system, overflow menu | One picker; replacing an edited system requires confirmation. |
| Playback row | Run/Pause, Step event, speed selector, virtual clock | Fixed position, stable labels; Step stays paused. |
| Message composer | Producer selector, editable customer key, Send, burst count | Message settings stay together. Show computed destination for the actual topic/partition count. Multiple producers require an explicit selected source. |
| Advanced message menu | Duplicate previous event, targeted poison exercise | Disabled with an explanation when the prerequisite message/target does not exist. No duplicate toolbar buttons. |
| Builder drawer | Add component, connect, add independent projection | Closed by default during presentation. Supports click-to-add as well as drag/drop. |
| Group inspector | Group/topic identity, replica count, assignments, lag, scaling | Separate adding a member from creating another independent group. Scaling pauses first and remains paused until Resume. |
| Component inspector | Identity, effective settings, current evidence, contextual actions | One authoritative value per setting. Separate editable configuration from read-only runtime values. |
| Fault actions | Crash/Recover, pause/resume consumption | Available in the selected component/group context; actions remain clearly distinct from deleting nodes. |
| Evidence panel | Retained/total event counts, type/group/message filters, selected-message journey | Bounded history and rendering. Explain missing/retained evidence; no false complete audit claim. |
| Canvas tools | Fit, zoom, layout controls | Never change runtime membership/assignments. Do not refit automatically after every message/state update. |
| Overflow/settings | Reset run, Restore scenario, Clear canvas, diagram import/export if included | Distinct destructive scopes; Reset run never discards a custom diagram. |

Preserve capability through better placement and explicit prerequisites rather than deleting controls to make the interface look simpler. Avoid introducing separate Demo/Builder engines: Build system is a UI disclosure over the same validated diagram and runtime commands.

## Lifecycle and edit contract proposal

| Action | Diagram/settings | Runtime | Playback afterward |
|---|---|---|---|
| Navigate away | Retained | Retained in memory; animation/subscriptions detach | Paused |
| Navigate back | Same diagram/viewport | Same messages, offsets, pending work and evidence | Paused; explicit Resume |
| Full browser refresh | Restore validated local document | Fresh runtime; prior transient execution is not restored | Paused with fresh-run notice |
| Reset run | Preserve current custom diagram and effective settings | Reconstruct all runtime state coherently | Paused at time0 |
| Restore scenario | Replace with selected blueprint after confirmation where needed | Fresh runtime | Paused at time0 |
| Move/resize/select/zoom | Presentation changes only | Unchanged, including assignments and in-flight work | Unchanged |
| Add/remove/rewire or change subscription | Validate and apply atomically | Preserve compatible state; explicitly reconcile invalidated work | Automatically paused; no automatic Resume |
| Add/remove consumer replicas | Membership command with unique identities | Assignment transition reconciles pending work | Automatically paused; apply membership and remain paused until explicit Resume |
| Crash/recover or pause a worker | Do not delete the component | Explicit runtime fault transition, recorded in evidence | Run may continue; global Pause remains paused |

Implementation should place the simulation session owner above the route component. It owns the lazily created engine and diagram, while visible canvas rendering owns its own frame loop. Navigating away pauses before detaching; returning starts no wall-time catch-up. A provider/store is acceptable if needed, but no external state library is assumed.

Persistent documents include schema version, scenario/custom identity, semantic component settings, stable IDs, explicit wires, positions and viewport. Exclude derived group wrappers, transient measured bounds where unnecessary, engine objects, partition records, committed offsets, pending events, deduplication sets and runtime status/counters. Autosave on diagram/settings changes, not on animation frames. Restore saved viewport deliberately; unconditional initial fitView must not overwrite it. If validation/version migration fails, preserve the original stored document and offer recovery rather than silently replacing it.

## Additional capability audit findings

- Producer key presets contain hardcoded destination annotations, including P4 while the default model has four partitions (P0–P3). Display destinations derived from the actual selected topic and partitioner; annotations must change after partition-count changes.
- The schema contains messageRate, clientDelayMs, brokerLatencyMs, concurrencySlots, failureRate, writeDelayMs, pollIntervalMs, batchSize and retentionCount. The inspected engine/store search only establishes effective reads for some settings, notably processingDelayMs. Several display/configuration fields have no corresponding engine read in the inspected execution paths. Classify every field as effective, presentation-only, unsupported or needing implementation; do not promise behavior merely because a field exists.
- Database writes currently use a fixed scheduled transit delay rather than the database writeDelayMs field. CDC timing also needs reconciliation with the displayed interval. Separate explanatory transit animation from actual simulated processing/write latency.
- The component union/connection validator still mentions worker-service although it is absent from the component registry and node-type registration. Either implement a complete supported role when justified or reject it during document restoration/connection validation. Do not allow half-supported components through imported diagrams.
- Generic consumer duplication currently copies only the first inbound/outbound wire. Scaling must use group-owned subscription/sink rules and preserve the intended pipeline, not accidental edge iteration order.

These are source-backed findings. They expand the reproduction checklist; they do not claim additional runtime experiments or a measured performance profile.


## Final scope and first implementation milestone

Resolved choices:

1. Presenter-first with a flexible builder, not separate simulation engines.
2. Every structural edit, including replica scaling, automatically pauses; no implicit resume.
3. Multiple database projections use independent consumer groups, each with one logical sink shared by its replicas. Reject execution of a group with conflicting sink/subscription configuration; identify the affected nodes and offer a clear repair path. Never silently choose the first wire.
4. Navigation pauses and retains the run in memory. Refresh restores only the diagram/settings into a fresh paused run.

After implementation approval, begin with Phase1 baseline/reproductions and Phase2 correctness. First coherent change: establish explicit configuration/runtime/geometry boundaries and reproduce/fix database-write-before-offset-commit, reset-after-failure and geometry-triggered rebalance defects. Preserve the existing UI until these contracts are tested; then improve timing/rendering and replace the control layout incrementally. Do not combine an untested engine rewrite with the entire visual redesign.

Deliver implementation commits by coherent verified slice. Keep one concise progress/status section rather than repeatedly appending broad planning reports. Record actual checks and meaningful limitations, preserve unrelated benchmark edits, and do not push without instruction.

# AI Agent Brief — Interactive Kafka Lab UI

**Status:** Discovery-first implementation brief  
**Audience:** Coding agent with access to this repository  
**Project:** Existing Kafka-based Loyalty / Purchase Event Streaming seminar application  
**Primary objective:** Design and implement an interactive, visual control-and-observation UI **on top of the existing backend**, reusing existing capabilities wherever possible.

> **Mandatory agent behavior:** **Do not invent requirements, choose architecture, or start implementation while material decisions remain unresolved.** Inspect the repository first, distinguish *verified facts* from *proposals*, ask the project owner targeted questions, **wait for their answers**, and only then implement the approved scope. Do not ask about facts that can be established by reading code or configuration.

---

## 1. Mission and intended experience

Turn the existing loyalty application into an **Interactive Kafka Experiment Lab**, not merely a dashboard with static charts.

A presenter or viewer should be able to:

1. **See the real system** as a graph of visual components with icons: Kafka brokers, topic partitions and replicas, producers, consumer groups and members, database, outbox relay, replay worker, and any existing notification services.
2. **Interact with the diagram:** select components, inspect current state and metrics, and—where approved and supported—add, remove, stop, restart, or reconfigure real components.
3. **Run experiments:** change traffic load, increase consumer instances, increase topic partition count, inject latency or failures, explore idempotency, and observe the results.
4. **Understand what happened:** live topology changes, consumer reassignment/rebalances, throughput, lag, processing latency, errors, and experiment timelines should explain cause and effect.
5. **Explore replay and correction** without accidentally re-executing irreversible external side effects.

**Core product rule:** An icon appearing on the canvas must never falsely imply that a real broker/worker/partition was created. Show the operation lifecycle (`requested → executing → verifying → succeeded/failed`) and update the *actual-state view* only from observed backend/cluster state. An explicit **simulation/preview mode** may be proposed, but it must be visually distinguishable from live mode and approved by the owner.

### Critical visual requirement

The diagram is the primary interface, not decoration:

- Use distinct, comprehensible icons/illustrations for each component type.
- Adding a consumer or broker should visually introduce the node only when its real state is verified; display transitional states and animate connections/assignments.
- Partitions should be shown as ordered logs, not as generic servers.
- Differentiate partition **leadership / replica placement** from consumer **partition assignment**.
- Node selection opens an inspector with meaningful live properties and supported actions.
- Use visual cues for healthy, joining, rebalancing, lagging, paused, failed, offline, and unknown states.
- Use accessible color plus labels/shapes/icons; do not encode status only by color.
- Animate a **sample or aggregate** of event flow rather than drawing a particle for every real event.

**Possible navigation model to discuss, not assume:**
- **Infrastructure View:** Kafka brokers → partition replicas → leaders, ISR, broker health.
- **Processing View:** producers / outbox → topic partitions → consumer groups → DB / downstream effects.
- **Experiment View:** controls, scenarios, metrics, timeline, comparisons.

Do not choose these exact tabs or a graph library until the existing frontend and project constraints have been inspected and the owner has approved the direction.

---

## 2. Existing context — leads to verify, not assumptions

Prior discussion suggests that the repository may already contain:

- A TypeScript/NestJS, pnpm-workspace Kafka loyalty demonstration.
- Events such as `PurchaseCompleted` and `PurchaseInvalidated`, keyed by `customerId`, and per-customer `customerSequence`.
- Producer/outbox flow; consumer idempotency; loyalty points/tier calculation.
- Kafka deployed locally, potentially with KRaft and Docker Compose.
- Replay worker with a `ReplayCustomerCorrector` implementing **two-pass** invalidation discovery and corrected reconstruction; staging/validation before cutover.
- Metrics/observability and possibly inspector/ops code.

**Do not rely on this list as ground truth.** Inspect actual directories, `package.json`, Compose files, service modules, contracts, event schemas, startup commands, README/specs, tests, and existing frontend (if any). Preserve working backend behavior. If these leads are inaccurate, report that and follow the repository.

The backend is **somewhat complete**; the goal is **not** to rebuild it for the sake of a preferred UI architecture.

---

## 3. Mandatory workflow: inspect → report → ask → plan → implement

### Phase 0 — Repository reconnaissance (READ-ONLY)

Before changing files:

1. Map the monorepo: apps/packages, language/framework versions, package manager, deployment method, commands to launch the app, and tests.
2. Trace the real event path: HTTP purchase command → transaction/outbox → Kafka producer → topic(s) → consumer group(s) → DB projections → replay/correction → any downstream notifications.
3. Inventory existing management/control APIs, Kafka admin client usage, Docker orchestration, runtime configuration mechanisms, logs, metrics, WebSocket/SSE endpoints, and tracing.
4. Identify existing UI, design system, charting or graph libraries, and authentication/authorization provisions.
5. Check the actual Kafka cluster topology: brokers, KRaft/controller setup, topics, partition counts, replication factors, consumer groups and how workers are started/scaled.
6. Inspect current replay boundaries, effect handling, idempotency checks and safety guards before proposing toggles.
7. Find the narrowest reusable abstractions and determine what UI functionality can be built **without changing the backend**.
8. Run existing tests and build only if safe and feasible; report baseline failures instead of quietly fixing unrelated issues.

**Discovery deliverable (in chat, before coding):**

- **Verified architecture map:** concise path/file references, event and data flow.
- **Capability matrix:** `Already supported` / `Requires adapter or endpoint` / `Requires infrastructure change` / `Not safely supported`.
- **Gap analysis:** what must be implemented for the UI, with backend impact clearly marked.
- **Risks and constraints:** ordering, replay safety, resource requirements, container support, access control.
- **Questions for the owner:** only meaningful unresolved decisions (see §4).
- **Recommended MVP:** smallest compelling demo based on repository evidence—not an unapproved commitment.

**STOP HERE and wait for answers.** No code edits, dependency installs, service reorganizations, schema migrations, or Docker changes before the owner answers the blocking questions. Read-only analysis and sketches are fine.

### Phase 1 — Obtain decisions and produce a plan

After receiving answers:

1. Restate confirmed requirements and any explicitly deferred items.
2. Present the proposed UI architecture, state flow, APIs/events, deployment approach, and feature milestones.
3. Separate **existing capabilities reused** from **new backend work**.
4. List proposed changed/new files and dependencies, why each is necessary, and migration/runtime effects.
5. Define acceptance criteria and exact demonstration steps.
6. **Ask for explicit approval of the implementation plan** before making architecture-changing or potentially destructive changes. When a decision is still unclear, ask rather than inventing it.

### Phase 2 — Implement in approved, testable increments

For every increment:

- Reuse existing ports/adapters/contracts first; avoid duplicating domain rules.
- Deliver a working vertical slice (real state → UI → safe control, if applicable → observed feedback).
- Add tests appropriate to the changed logic and run the existing relevant checks.
- Provide an updated changed-file list, rationale, commands run, results, known limitations, and what comes next.
- If new information invalidates a prior decision, **stop, explain the trade-off, and ask**; do not silently redesign.

---

## 4. Question protocol — ASK rather than ASSUME

The agent must **derive questions from verified repository findings**. Ask a small, prioritized group of blocking questions at a time; offer options with implications and a recommendation, but do **not** treat the recommendation as approval. Do not repeatedly ask questions already answered in the repository or conversation.

### Decisions likely to require owner input

1. **UI scope:** Is this a new frontend, an extension of an existing one, or a standalone local seminar console? Who will operate it—only presenter or attendees as well?
2. **Technology choice:** Is an existing frontend stack/design system mandatory? Can a visual flow library (e.g. React Flow) be added if appropriate?
3. **Priority/features:** Which are MVP vs optional—consumer scaling, topic partition increase, broker lifecycle, delay injection, traffic generation, idempotency, replay, notifications, broker failover?
4. **Execution semantics:** Must every visible control act on real infrastructure, or is a separately labeled preview/simulation acceptable for expensive/unsafe features?
5. **Deployment environment:** Local Docker Compose, WSL, remote cluster, cloud, Kubernetes, or other? What are the actual RAM/CPU limits and demo constraints?
6. **Permissions and destructive actions:** Who can start/kill brokers, mutate topics, toggle protections, reset state, or trigger replay/cutover? Are confirmation dialogs and a dedicated experiment environment required?
7. **Data isolation/reset:** Is it acceptable to create per-experiment topics/groups/DB namespaces, or should runs share one cluster? What must a Reset button actually reset?
8. **Configuration toggles:** What exactly does “idempotency OFF” disable (event-ID inbox, sequence checks, HTTP request idempotency, or an intentionally isolated unsafe path)? Which guarantees must remain enforced?
9. **Realtime/metrics:** Which metrics are already recorded and trusted? Expected freshness, polling vs SSE/WebSockets, time-windowing, persistence, and benchmark definitions?
10. **Replay controls:** Is the UI read-only for replay/staging, allowed to trigger correction, or allowed to approve cutover? What is the side-effect policy, and how is live activity reconciled?
11. **Visual style:** Compact operations console, educational animation, or a hybrid? Brand/colors, icon style, accessibility and screen resolution for projector/demo?
12. **Scope/timeline:** What is the delivery deadline, must-have demo flows, acceptable additions to backend, and allowed dependencies?

**Question format (example):**

> I found that consumers currently run inside `<actual service/file>`, so scaling the container would also duplicate `<actual other responsibility>`. Should I (A) extract a dedicated worker runtime, (B) implement an in-process multi-consumer manager for the demo, or (C) defer consumer scaling? I recommend **A** because `<repository-specific evidence>`. **No change will be made until you choose.**

Do not ask generic questions like “Which framework?” if `package.json` clearly answers them. Ask only about the remaining choice.

---

## 5. Feature candidates to evaluate against existing backend

These are **candidates**, not authorized requirements. In Phase 0, mark each as available/partial/missing/unsafe and propose an implementation cost and demo value.

| Candidate control / visualization | Observable result | Key correctness constraint |
|---|---|---|
| Add / remove consumer **instances** | Group membership, partition assignments, rebalance, lag | Must not duplicate unrelated service responsibilities; assignment comes from Kafka |
| Increase **topic partition count** | New partitions, changed maximum parallelism | Kafka does not support shrinking partitions in place; key-to-partition mapping may change and affect per-customer ordering |
| Add / stop / restart broker | Broker health, leader changes, replica placement | Requires real orchestrator and correct KRaft setup; new brokers do not automatically receive existing replicas |
| Show / change replica placement | Leader/follower, ISR, resilience | Replication factor is not “partitions per broker”; reassignment is separate and potentially risky |
| Traffic generator | Target vs achieved event/s, customer distribution, errors | Distinguish HTTP API throughput, outbox publish rate, Kafka ingress, and consumer processing rate |
| Processing delay / pause | Lag growth and recovery | Injection scope and location must be explicit; do not block heartbeat/poll loop incorrectly |
| Crash/restart consumer | At-least-once redelivery, recovery | UI must distinguish hard crash, graceful stop, offset commit and DB commit |
| Idempotency experiment | Duplicate detected vs effects applied | Never silently weaken real production guards; define precisely which guard is disabled |
| Hot-key / partition skew | Uneven partition traffic, uneven lag | Producer key distribution must be measurable and transparent |
| Broker/replica failure experiment | Leader election, ISR, availability | Requires meaningful replication and failure-safe test environment |
| Replay/correction | Live vs staged state, invalidations, corrected tier/points | Replay must not unintentionally dispatch external effects; cutover is separate and controlled |
| Notification side effect (if present) | Live notification vs replay suppression | External effects must be isolated from replay; do not assume email feature exists |
| Experiment presets | Repeatable compare-before/after runs | Presets must declare and validate their prerequisites and cleanup |

### Suggested MVP for discussion

A compelling first milestone *may* be:

- Real cluster/consumer topology with icon nodes and inspector.
- Real consumer scaling **if architecture supports it safely**.
- Traffic generation with adjustable rate and number of customer keys.
- Processing-delay injection with lag/throughput/latency graphs.
- Experiment timeline with actual operation statuses.

Defer broker scaling, topic mutation, and risky toggles if the environment cannot support them safely or their implementation would distract from the seminar. **Ask the owner to approve the actual priority order.**

---

## 6. Visual interaction specification (proposed behavior)

### Component representations

- **Broker node:** icon, broker/node ID, controller vs broker role when relevant, running/unavailable/unknown, replica count, status details.
- **Topic / partition:** topic name, `P0 ... Pn`, ordered log metaphor, leader, replicas, relevant offsets, partitions' rates/lag where available.
- **Consumer group:** group ID, state, member count, total lag, assigned topic partitions; individual consumer nodes show client/member identity and assignments.
- **Producer / outbox:** publish rate, batch/ack indicators when measured, pending outbox count, errors.
- **Database:** projection and outbox state; do not invent round-trip estimates.
- **Replay worker:** discovery → reconstruction → staging → validation → cutover phases, bounded range/watermark, progress and failure states.
- **Notification/external effect:** only if implemented; show staged vs dispatched behavior, not fictional mail sends.

### Interaction lifecycle

1. User selects `Add Consumer` or similar action.
2. UI displays the exact requested change and any preconditions/risks.
3. User confirms if required by the agreed safety policy.
4. UI calls a control-plane command; receives an operation/job ID when asynchronous.
5. UI shows `requested / executing / waiting for verification`.
6. Backend reports completion and the observer independently confirms membership/assignments.
7. Canvas updates actual nodes/edges and metrics; failures show an actionable reason and rollback/reset options where supported.

**Never** equate an HTTP `202 Accepted` or button click with a running broker/consumer. Rebalances can cause temporary lag and reassignment; render them as observable events, not made-up animations.

### View/model requirements

- Clearly label **actual / stale / unknown / simulated** data.
- Distinguish the **physical placement graph** (broker → partition replicas) from the **processing assignment graph** (partition → consumer member).
- Preserve a user's selected node across refresh where possible; do not jitter graph positions every polling tick.
- Use zoom/pan, tooltips, legends, status labels, and a details inspector.
- Avoid overwhelming the browser: aggregate high-frequency activity and throttle/virtualize rendering.
- Make layout usable on a typical laptop and large presentation display; confirm specific target dimensions with the owner.
- Consider keyboard navigation, reduced-motion settings, color-blind-safe states, and loading/error/empty states.

---

## 7. Backend integration and API design principles

**Inspect before designing.** Prefer adapting existing admin/ops/inspector and observability code over introducing new services by default.

Potential boundaries to evaluate:

- **Read model / topology observer:** cluster brokers, topic metadata, replica/leader/ISR, groups/members/assignments, app worker identity and health.
- **Metrics reader:** authoritative time series for producer/consumer/DB/replay; timestamp, source, freshness and definition of each metric.
- **Experiment coordinator:** lifecycle, scenario metadata, scoped cleanup/reset, and audit trail.
- **Control adapters:** Kafka Admin, process/container orchestrator, load generator, configurable test-only worker behavior.
- **Realtime transport:** WebSocket, SSE or polling based on existing infrastructure and measured update needs, not preference alone.

### Candidate API contract shape (illustrative only; do not implement by assumption)

```ts
// Identify operation separately from observed cluster state.
type OperationState =
  | 'REQUESTED'
  | 'RUNNING'
  | 'VERIFYING'
  | 'SUCCEEDED'
  | 'FAILED'
  | 'CANCELLED';

type ExperimentCommandResult = {
  operationId: string;
  state: OperationState;
  submittedAt: string;
};

type ObservedComponent = {
  id: string;                 // stable identity from real infrastructure
  kind: string;               // broker, partition, replica, consumer, ...
  status: string;             // from observation, not optimistic UI
  observedAt: string;
  source: string;
  properties: Record<string, unknown>;
};
```

The agent must inspect existing DTOs/events/schema libraries before proposing endpoints or contract changes; use existing conventions (auth, validation, error handling, logging, versioning).

### Architecture precautions

- **Consumer scaling:** A NestJS HTTP process that also hosts a consumer, outbox relay, or scheduler cannot automatically be replicated without evaluating those responsibilities. Determine actual process boundaries first.
- **Kafka partitioning:** Partitions belong to topics, not brokers. Replicas are placed on brokers. Increasing partitions can change hash mapping for future keyed messages and complicate strict per-customer ordering; flag this before allowing a live mutation.
- **Broker orchestration:** Kafka Admin API does not start Docker containers. Determine who owns broker processes and how controller quorum is configured. Broker joins and replica reassignment are distinct.
- **Delay injection:** Simulate *business processing time* in an approved test-only mode while preserving Kafka consumer liveness; do not inject sleeps arbitrarily into the wrong callback.
- **Idempotency:** `eventId` dedup, `customerSequence` gating, unique constraints, HTTP idempotency, and outbox guarantees are separate. A single `Idempotency OFF` toggle is ambiguous unless explicitly specified.
- **Replay:** Corrected projection reconstruction is not automatic re-delivery of all historical side effects. Reuse shared business logic where appropriate, isolate external effect dispatch, and preserve staging/validation/cutover safety.
- **Metrics:** No fabricated lag, broker health or throughput. Distinguish estimated, sampled, derived and directly measured metrics. If no metric exists, display `not available` until instrumented.

---

## 8. Experiment safety and isolation

The UI is an **experiment console**, but unsafe commands must be gated.

Before implementing mutation controls, ask about an approved **experiment environment** and authorization model. Candidate safeguards:

- Only run in explicit development/demo mode, or an approved sandbox; prohibit accidental production access.
- Role/permission checks for broker lifecycle, partition changes, destructive resets, idempotency bypass and replay cutover.
- Confirmations for irreversible/disruptive actions, with clear warnings about consequences.
- Per-experiment `experimentId`, timestamps, operation log and scoped resources where feasible.
- Idempotent control commands where appropriate and correlation IDs for tracing.
- Cleanup/reset workflow for created topic/group data, traffic, injected failures and process state; no pretend `decrease partitions` control.
- Resource guardrails (consumer/broker max count, max traffic rate, timeout, rate limits), based on actual host capacity.
- Graceful handling if an operation partly succeeds, worker crashes, the UI disconnects, or backend reports stale state.
- Replay should not dispatch external email/payment/third-party effects simply to rely on downstream deduplication.

Do not implement an unsafe toggle by weakening protections throughout the application unless the owner explicitly approves a narrowly scoped experiment design.

---

## 9. Testing and demonstration acceptance criteria

**Agreement required on which items are MVP.** For every feature implemented:

1. **Ground truth:** UI topology/status reflects real Kafka/worker state, with timestamps or clear unavailable indicators.
2. **Operation lifecycle:** Requested actions do not appear complete before verification; failure states are visible and actionable.
3. **Scale demo:** Increasing consumer members updates group assignment and shows whether throughput/lag changes under controlled load; excess consumers can remain idle.
4. **Ordering demo:** Per-customer ordering can be inspected using `customerId`, `customerSequence`, Kafka partition/offset; no assumption that partition increase preserves existing key mapping.
5. **Delay demo:** Changing processing delay changes observable work/lag when load is sufficient; test results remain measured, not scripted.
6. **Failure demo (if approved):** Crash/restart behavior, redelivery and idempotency are shown without corrupting non-experiment data.
7. **Replay demo (if approved):** Invalidated purchase causes corrected tier/points in staging; external effects do not fire again during replay; cutover requires explicit control.
8. **UI robustness:** Loading, errors, disconnections, rebalances, missing metrics, and stale snapshots are handled without claiming success.
9. **Build quality:** Relevant tests, typecheck, lint, and smoke-test documented; no unnecessary changes to core business logic.
10. **Documentation:** Exact commands to start all processes, run an experiment, stop/reset, and reproduce a failure.

Suggested test layers (choose after inspecting tooling): component tests for node renderers/inspector, API contract tests, control-plane adapter tests, and at least one local end-to-end smoke test against Kafka/containers when available.

---

## 10. Required agent response format

### Your FIRST response after inspecting the repo (no edits)

Use this structure:

```md
## What I verified
- Architecture with paths and currently running entry points.
- Existing frontend / observability / admin capabilities.

## Capability & gap matrix
| Feature | Verified evidence | Status | Proposed approach | Risk |

## Proposed MVP
- A small, testable scope and why it is feasible.

## Blocking questions for you
1. [Only questions the repository cannot answer; include options, trade-offs, recommendation.]

## What I will NOT change without approval
- [Concrete affected services, files, unsafe operations, dependencies, etc.]

I will wait for your answers before editing files.
```

### After owner decisions

Provide: confirmed decisions, proposed diagrams, architecture and API contracts, phased plan, file-change list, risks, and acceptance tests. Obtain approval where required.

### After each implemented slice

Provide: implemented behavior, files changed (paths), commands/tests run with results, screenshots or UI usage instructions if available, known limitations, and the next decision/step. Never claim a backend function exists when it was only simulated.

---

## 11. Final guiding principles

1. **Inspect and reuse before building.** This is a UI-first request for an existing, partly finished backend.
2. **Ask instead of assuming.** Architecture and safety decisions belong to the owner.
3. **Make the real Kafka mechanisms visible.** User interactions must connect to observable state.
4. **Be precise about semantics.** Broker ≠ partition ≠ replica ≠ consumer; topic partition scaling ≠ broker scaling.
5. **Keep experiment controls safe and reversible where possible.** Warn honestly when they are not.
6. **Treat replay differently from external effect redelivery.** Rebuild state without accidentally repeating real-world actions.
7. **Deliver incremental, demonstrable slices.** Avoid a large refactor before the first working visual experiment.

**Start now with Phase 0: inspect the repository read-only. Do not write code yet. Report findings and ask your blocking questions.**

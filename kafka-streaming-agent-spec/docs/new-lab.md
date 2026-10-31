# Task: Build a Visual Simulation Lab for the Existing Kafka Application

## 1. Mission

The application already contains a working lab connected to real infrastructure.

Your task is to **ADD a second, completely simulated lab** that functions as an interactive, visual distributed-systems playground.

Do not replace, remove, or redesign the existing lab.

The new lab should let users visually construct systems, configure components, duplicate them to demonstrate scaling, and watch synthetic messages travel through the architecture.

**Primary goal: Educational visualization with coherent simulated behavior.**

This is NOT a request to build another real Kafka environment or a production-grade distributed-system simulator.

### Non-negotiable requirements

- Keep the existing real lab operational.
- Add a separate "Simulation Lab" tab or route.
- Run the simulation entirely client-side.
- Never connect the simulation engine to real Kafka, databases, Docker, or existing infrastructure-control endpoints.
- Do not modify existing business logic unless absolutely necessary and explicitly approved.
- Clearly distinguish simulated state and telemetry from real infrastructure.
- The simulation must function without the application backend running, assuming the frontend itself is available.

---

## 2. Research Before Implementation

Inspect the existing repository and study these references:

**YADA**

https://github.com/bishoku/yada

Study:
- Component palette and custom nodes
- Interactive architecture canvas
- Connection wiring
- Message token animation
- Timeline and playback interactions

**Breakscale**

https://github.com/xevrion/breakscale

Study:
- UI-independent discrete-event simulation
- Configuration-driven component behavior
- Queueing, capacity, latency, and failure behavior
- Deterministic simulation and metrics

Note: Breakscale uses a custom SVG canvas. Do not assume it uses React Flow.

**Kafka Explained**

https://github.com/ChauhanParesh/kafka-explained

Study:
- Kafka topics and partition logs
- Consumer-group assignment
- Offsets and consumer lag
- Hot partitions and consumer scaling

**React Flow**

https://reactflow.dev/

Evaluate it as the default canvas implementation, unless the existing codebase already contains a more suitable solution.

Distinguish free examples from Pro-licensed examples. Do not copy restricted code.

Evaluate these projects for reusable techniques rather than attempting to integrate entire repositories.

---

## 3. Product Experience

### 3.1 Main Layout

The Simulation Lab should have four primary areas.

**Left — Component Palette**

A searchable list of draggable components, organized by category.

Candidate components:

- Messaging: Producer, Kafka Broker/Cluster, Topic, Consumer, Message Queue.
- Application: API Server, Worker, Service, Outbox Publisher.
- Storage: PostgreSQL/Database, Redis Cache.
- Infrastructure: Load Balancer, Gateway.
- Utilities: Message Generator, Delay, Failure Injector.

These are candidate components, not an instruction to implement all of them immediately.

Start with a coherent MVP and design an extensible component registry.

Each component should have:
- Clear name and recognizable icon
- Default configuration
- Defined input/output connection capabilities
- Simulation behavior, if supported
- Visual status during simulation

**Center — Interactive Architecture Canvas**

Features:
- Drag components from palette to canvas.
- Move and reposition nodes.
- Create, remove, and reconnect edges.
- Zoom, pan, fit view, and optionally use a minimap.
- Select one or multiple nodes.
- Duplicate, delete, and configure nodes.
- Show visually distinct component types.
- Show moving messages during playback.
- Highlight active connections and processing nodes.

Prefer a polished, legible, interactive presentation.

Do not sacrifice usability for decorative animations.

**Right — Node Inspector**

Clicking a component should open its configuration.

The inspector must be component-specific.

Examples:

Producer:
- Message rate
- Message key
- Batch size
- Payload template
- Simulated network delay

Kafka:
- Topic names
- Partition count
- Partitioning strategy
- Simulated broker/network delay
- Replication settings, only if replication is actually modeled

Consumer:
- Consumer group ID
- Subscribed topics
- Processing time
- Simulated failure rate
- Retry settings, where supported

Database:
- Simulated write latency
- Processing capacity
- Failure behavior

Every functional setting must affect the engine.

Settings not implemented must be visibly marked as informational or unavailable.

**Bottom — Simulation Controls and Timeline**

Provide:
- Run / Pause / Resume
- Step Forward
- Reset Simulation
- Send One Message
- Send Batch
- Generate Continuous Traffic
- Simulation Speed
- Event Timeline

The timeline should display events such as:
- Message created
- Message published
- Partition selected
- Offset appended
- Consumer assigned
- Consumption started
- Processing completed
- Database operation completed
- Failure or retry

---

## 4. Component Duplication and Scaling

Users must be able to add multiple instances of the same component.

Each component should support:
- Duplicate
- Configure
- Delete
- Enable/Disable

Duplicated components require unique IDs and independent runtime state.

Define clear behavior for copying existing connections.

For example, duplicating a consumer may preserve its topic subscriptions and consumer group configuration, but this must be an intentional, documented behavior.

### Kafka Consumer Scaling

Suppose a topic has 6 partitions.

With one consumer in a group, that consumer may own all 6 partitions.

After adding three consumers to the same group, partitions should be distributed among those consumers according to the chosen simulated assignment strategy.

The visualization should reflect:
- Consumer joining the group
- Rebalance beginning and completing
- Previous partition ownership
- New partition ownership
- Consumption continuing under the new assignments

A partition may be assigned to only one active member of a consumer group at a time.

Different consumer groups must maintain independent consumption progress.

Do not duplicate Kafka messages merely because another consumer joins the same group.

If the number of consumers exceeds the number of partitions, some consumers may remain idle.

Increasing consumer count must not automatically increase throughput when another bottleneck limits the system.

### Broker Scaling

Duplicating a broker is not equivalent to increasing topic partitions.

Do not automatically create partitions or increase throughput when brokers are added.

If broker replication, partition leadership, or failover is not implemented, do not present these as functional scaling effects.

---

## 5. Topology Semantics

The canvas must distinguish visual wiring from the actual simulated operations.

Do not assume that every connection means "forward the message to the next node."

Possible connection semantics include:
- HTTP request
- Database write
- Database read
- Event publish
- Topic subscription
- Service invocation

Define a typed connection model and validation rules.

A connected graph alone does not guarantee that every node will process or forward every event.

### Kafka Concepts

Model the following correctly for features that are implemented:

- A broker is a server, not a topic.
- Topics contain partitions.
- Each partition maintains an ordered append-only log.
- Records have partition-specific offsets.
- A producer publishes records to a topic partition.
- Partition selection depends on the configured strategy and message key.
- Consumers subscribe to topics.
- A consumer group distributes partition ownership among members.
- Different consumer groups independently consume the same underlying records.
- Ordering is guaranteed within a partition, not globally across all partitions.

Decide on an intuitive UI representation for brokers, topics, partitions, and consumer groups.

For example, a Kafka cluster node could contain topics and partitions internally, while the inspector allows users to expand and inspect them.

Do not expose separate draggable broker, topic, and partition nodes unless their relationships are modeled consistently.

For the initial version, prioritize understandable Kafka behavior over complete protocol fidelity.

Clearly document simplifications.

---

## 6. Simulated Message Lifecycle

**This is the central feature.**

A user should be able to build:

Producer -> Kafka -> Consumer -> Database

Then click "Send Message" and watch a visible token progress through the architecture.

The animation must correspond to a logical simulated message.

Example lifecycle:

1. Producer generates a synthetic message with a unique ID.
2. The simulation selects the destination topic and partition.
3. A token visibly travels from producer to Kafka.
4. Kafka appends the record to the partition log and assigns an offset.
5. The simulation determines eligible consumer groups and partition owners.
6. A consumer receives the record when its simulated processing capacity permits.
7. The consumer visibly processes the record.
8. Any configured handler action executes in the simulation.
9. If that action writes to a database, a corresponding visual operation travels to the database.
10. The simulation records completion, failure, or retry as applicable.

A database write must not happen automatically just because a database is connected to the consumer.

There must be a defined operation or handler behavior explaining that action.

Messages should be inspectable.

Suggested message metadata:
- Message ID
- Key
- Payload
- Event type
- Created time
- Topic
- Partition
- Offset
- Current processing status

Not every component needs all fields.

### Visual Behavior

Display:
- Animated tokens moving on edges
- Message arrival and departure
- Processing indicators
- Queueing/backlog where modeled
- Failure states
- Selected message inspection
- Active message path highlighting

Animation should look smooth and intentional.

However, CSS or SVG animation must not be the source of truth for simulation state.

---

## 7. Simulation Engine

Build a lightweight, deterministic, client-side simulation engine.

**Do not overengineer a complete Kafka implementation.**

The engine should support the behaviors necessary for the educational scenarios.

Separate the following concerns:

1. **Topology Model** — components and connections.
2. **Configuration Model** — behavior and parameters.
3. **Simulation Runtime** — messages, queues, offsets, events, and assignments.
4. **Simulation Clock / Scheduler** — ordering and timing.
5. **Presentation Layer** — nodes, animation, timeline, and metrics.

Prefer a small event-driven or discrete-event scheduler over scattered per-component timers.

Important requirements:
- One authoritative simulation clock.
- Pause freezes simulated progression.
- Resume continues without losing state.
- Step Forward advances one logical event or clearly documented step.
- Playback speed affects simulated progression consistently.
- Reset clears runtime state while preserving the topology.
- Animation reflects the same logical timeline.
- Support seeded randomness where stochastic behavior is implemented.
- Prevent infinite processing loops.
- Bound event history and memory usage.
- Avoid generating a DOM element for every message under high traffic; aggregate or sample visual tokens when necessary.

If topology or configuration changes during an active simulation could invalidate current events, define and document the policy.

Do not silently continue with inconsistent runtime state.

### Visual Simulation vs Quantitative Simulation

Prioritize visual correctness.

Only calculate metrics whose underlying behavior is implemented.

For example:
- Processed message count can be derived from completion events.
- Backlog comes from simulated queued records.
- Lag comes from partition log progress and consumer-group offsets.
- Latency comes from simulated timestamps.
- Throughput comes from simulated completed events over simulated time.

Do not fabricate graphs or random performance numbers to make the dashboard look active.

Clearly label metrics as SIMULATED.

---

## 8. Required Educational Presets

Provide a small set of predefined architectures.

### Preset A — Basic Kafka Flow

Producer -> Kafka -> Consumer

Demonstrates:
- Publish
- Partition selection
- Offset creation
- Consumption

### Preset B — Consumer Scaling

Producer -> Kafka Topic (6 partitions) -> Consumer Group

Start with one consumer.

Allow the user to duplicate consumers in the same group.

Demonstrates:
- Partition ownership
- Rebalance
- Parallel processing across partitions
- Idle consumers when consumer count exceeds partition count

### Preset C — Slow Consumer

Producer -> Kafka -> Slow Consumer

Demonstrates:
- Processing delays
- Growing consumer lag
- Backlog recovery after processing capacity increases

### Preset D — Multiple Consumer Groups

Producer -> Kafka -> Loyalty Consumers / Analytics Consumers

Demonstrates:
- Independent subscriptions
- Independent group offsets
- The same stored records consumed by separate groups

### Preset E — Transactional Outbox

Use the existing project architecture as the reference.

Illustrative flow:

API -> PostgreSQL (Purchase + Outbox in one transaction)

Outbox Publisher -> Kafka -> Loyalty Worker -> Read Model

The outbox publisher must obtain persisted outbox events before publishing.

Show the real conceptual ordering:
- Transaction commits.
- Pending outbox event becomes available.
- Publisher obtains it.
- Event is published.
- Consumer processes it.
- Read model updates, if the simulated handler specifies that action.

Inspect the actual repository before deciding whether to represent polling, CDC, or another publication mechanism.

Do not assume implementation details.

Do not communicate with the real outbox database or Kafka.

Each preset should include a short educational explanation and clearly stated simplifications.

---

## 9. State Management and Persistence

Support:
- Blank canvas
- Load example architecture
- Save topology locally
- Restore topology after refresh
- Reset canvas
- Reset simulation independently
- JSON import/export if straightforward

Keep topology state separate from simulation runtime state.

Do not persist active animation timers or runtime references as topology data.

Use versioned, validated serialization for saved graphs.

Do not automatically start a simulation after loading a topology.

---

## 10. Suggested Feature Architecture

Adapt to the actual repository.

Conceptual structure:

```text
simulation-lab/
  components/
    canvas/
    nodes/
    palette/
    inspector/
    controls/
    timeline/
  domain/
    topology/
    components/
    connections/
    messages/
  engine/
    scheduler/
    routing/
    kafka/
    processors/
    metrics/
  registry/
  presets/
  store/
  tests/
```

Important design principles:

- A component registry defines node types, defaults, configuration schemas, and supported behaviors.
- The simulation engine must not depend on React or DOM APIs.
- Custom node components should primarily render state and collect user interactions.
- Business simulation behavior belongs outside visual components.
- Existing real Kafka adapters must not be imported into the simulation engine.
- Do not introduce a backend or WebSocket server unless an actual requirement justifies it.

---

## 11. Implementation Strategy

Avoid building a large framework before a working demo exists.

### Phase 0 — Codebase Inspection

Inspect the existing frontend and provide a design proposal.

No source-code changes yet.

### Phase 1 — Working Vertical Slice

Deliver a complete small experience:

- New Simulation Lab tab
- Component palette
- Draggable Producer, Kafka, and Consumer
- Wiring and connection validation
- Component inspector
- Send One Message
- Animated message traversal
- Run/Pause/Reset
- Minimal event timeline

At the end of this phase, a user must already be able to build and demonstrate a functioning visual flow.

### Phase 2 — Kafka Semantics and Scaling

- Multiple partitions
- Message keys and partition routing
- Multiple consumers
- Consumer group IDs
- Partition assignment/rebalance visualization
- Consumer processing delays
- Derived lag and throughput metrics
- Component duplication

### Phase 3 — Additional Components and Scenarios

- Database operations
- Outbox Publisher
- Additional worker/service types
- Transactional outbox preset
- Failure and retry behavior where supported
- Multiple consumer groups
- Save/load

### Phase 4 — Polish

- Improved animation
- Better icons
- Message inspection
- Performance improvements
- Enhanced configuration UX
- Educational tooltips

Phase boundaries may change after repository inspection, but preserve the vertical-slice-first principle.

---

## 12. Technical Quality and Verification

Use TypeScript and existing project conventions.

Required verification should cover:

**Topology**
- Duplicate creates unique node IDs.
- Delete removes or updates affected connections correctly.
- Invalid connections are rejected.
- Importing invalid topology data fails safely.

**Simulation**
- Pause stops logical progression.
- Resume preserves message state.
- Step Forward is predictable.
- Reset preserves the architecture.
- Replaying with identical configuration and seed produces equivalent simulation results.

**Kafka**
- Messages receive valid partition assignments.
- Offsets increase monotonically within each partition.
- Records maintain partition order.
- Consumers in the same group do not concurrently own the same partition.
- Independent groups maintain independent progress.
- Scaling beyond partition count produces idle consumers.
- Adding brokers does not automatically increase partition count.

**Integration**
- Existing lab routes remain functional.
- Existing real APIs are not invoked by simulation actions.
- The simulation runs with the real backend unavailable.
- Existing project build and tests remain operational.

**Visual Acceptance**
- Synthetic messages visibly traverse active connections.
- Animation and timeline reflect the same logical state.
- Configuration changes have visible effects when supported.
- Heavy traffic does not produce unbounded visual elements or memory growth.

---

## 13. Mandatory Workflow Before Coding

**Do not implement immediately.**

First inspect the repository and return the following:

### A. Current Architecture Assessment

Identify:
- Frontend framework and libraries
- Routing/navigation
- Existing lab implementation
- Existing state management
- Reusable components
- Areas that must remain untouched

### B. Reference Comparison

Compare YADA, Breakscale, and Kafka Explained.

State exactly which design concepts or implementation techniques you recommend reusing.

### C. Proposed UI Design

Describe:
- Component palette
- Main canvas
- Inspector
- Timeline
- Playback controls
- Component visual style

### D. Proposed Technical Architecture

Explain:
- Data models
- Component registry
- Simulation clock and event scheduler
- Kafka abstraction
- Animation mechanism
- State management
- Integration with the current frontend

### E. MVP Scope

Identify:
- Components supported initially
- Behaviors implemented initially
- Features intentionally deferred
- Risks and potential conflicts with the current codebase

### F. Clarifying Questions

Ask me questions about decisions that materially affect implementation.

Important decisions include:

1. How much emphasis should be placed on pure graphic animation versus quantitatively meaningful simulation?
2. Should the initial component library focus on Kafka or general distributed-system architecture?
3. Should users be allowed to edit the graph while playback is running?
4. When duplicating a node, should existing connections be copied automatically, optionally, or not at all?
5. How should brokers, topics, and consumer groups appear visually on the canvas?

You may propose defaults, but do not silently decide these major issues.

Do not ask unnecessary questions about minor implementation details already dictated by existing project conventions.

**Wait for my approval of the implementation plan before modifying the repository.**

---

## 14. Definition of Done

The new lab is successful when a user can:

1. Open the Simulation Lab without affecting the existing lab.
2. Drag a producer, Kafka component, and consumers onto an empty canvas.
3. Wire them together.
4. Configure their simulated behavior.
5. Click "Send Message" and see a message travel through the architecture.
6. Inspect what happened at each component.
7. Duplicate a consumer and observe a simulated rebalance.
8. Run additional traffic and observe the resulting changes in simulated processing.
9. Pause, resume, step through, and reset the simulation.
10. Load a predefined scenario and repeat the demonstration.

Everything must be performed entirely through simulation, without requiring real Kafka infrastructure.

**The result should feel like an interactive distributed-systems laboratory, not a static diagram editor with decorative moving dots.**

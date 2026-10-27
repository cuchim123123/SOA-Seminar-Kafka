# 10 — Final Kafka Cluster: KRaft, Replicas, ISR and Failover

## When to implement

**Phase 7 only** — after a working single-broker pipeline, multi-partition consumers, durable state and scaling tests. This is a mandatory component of the **final demo**, but not a Phase 1 prerequisite.

## Target topology

- **3 Kafka nodes**, each with broker+controller roles in **combined mode** for the local lab.
- **3 KRaft controller voters** (one per node), distinct node IDs and persistent metadata/log directories.
- Topic `purchase.events`: 6 partitions, replication factor **3**.
- Topic `min.insync.replicas=2`, producer `acks=all`.
- Unclean leader election disabled. Broker data volumes persistent.
- Replication factor settings for Kafka's internal topics (`__consumer_offsets`, transaction state where used) set appropriately for a three-node lab.
- Clients bootstrap with **multiple** reachable brokers, not a single hardcoded node.

Combined broker/controller mode is convenient for demonstration but is **not** a production deployment recommendation. All containers share one host unless explicitly distributed.

## Illustrative assignment for partitions P0/P1/P2

| Broker | P0 | P1 | P2 |
|---|---|---|---|
| B1 | Leader | Follower | Follower |
| B2 | Follower | Leader | Follower |
| B3 | Follower | Follower | Leader |

P3–P5 follow analogous balanced leaders. Actual placement comes from Kafka topic metadata, not hardcoded dashboard state. RF3 with exactly three brokers gives each partition three replicas, one per broker, assuming normal assignment.

## Acks and ISR semantics

- Kafka assigns a leader to each partition. Follower replicas fetch/replicate its log and become/leave ISR based on synchronization requirements.
- `acks=all`: producer waits for **the current ISR** to acknowledge according to Kafka's commit rules; it is not merely "two acknowledgments" because minISR=2.
- `min.insync.replicas=2` means writes with `acks=all` fail if ISR size falls below two.
- With healthy ISR size 3: success after required ISR replication/commit.
- With one broker down and ISR size 2: **can** still write once the leader/election and controller quorum settle.
- With ISR size 1: produce fails under `acks=all`/minISR2.
- With two of three combined nodes down: additionally lose a majority of KRaft controllers, preventing normal metadata quorum operation; don't attribute all failure solely to ISR.
- Retries and request timeouts still matter during election. Producer errors must be surfaced and retry behavior observable.

## Broker failure demo

1. Start three-node Compose profile with fresh volumes and initialize `purchase.events` RF3 minISR2.
2. Wait for healthy ISR=3 for each partition; show broker IDs and current leaders.
3. Run producer and Loyalty/Analytics consumer workload.
4. Select a broker currently leading at least one partition and stop its container.
5. Watch metadata: surviving ISR member promoted to leader for affected partitions, two controllers maintain quorum, producer may temporarily retry.
6. Verify processing resumes and log continues to grow after leader election.
7. Restart failed broker; observe replica catch-up and ISR return to three.
8. Record downtime, error counts, retries and state correctness; do not promise instantaneous failover.

## Broker failure vs consumer failure

| Failure | Kafka mechanism | Observation |
|---|---|---|
| Loyalty worker exits | **Consumer group rebalance** | Assigned partitions move to remaining group members. |
| Kafka broker leader exits | **Partition leader election** | Eligible ISR replica assumes leadership. |
| Kafka follower falls behind | **ISR shrink / replica catch-up** | Under-replicated partitions, potential ack availability impact. |
| KRaft controller majority lost | **Metadata quorum unavailable** | Cluster cannot reliably progress metadata operations. |

A single dashboard should show these as **different** mechanisms and separate failure indicators.

## Clean environment transition

Phase 1 used a single KRaft controller and RF1 topic. Do **not** assume Compose can transform the metadata/controller quorum and replication assignment in place simply by adding two containers. For reliable seminar setup:

1. Stop single-node profile without deleting unrelated resources.
2. Start a **separate fresh three-node profile** with its own volumes and cluster configuration.
3. Create new topic with RF3/minISR2 and re-seed synthetic data.
4. Reconfigure apps with all three bootstrap brokers.
5. Retest consumer group committed offsets and producer connectivity.

Pin compatible Kafka image versions. For KRaft bootstrap/config details use Apache Kafka official documentation for the selected version. Avoid deprecated ZooKeeper assumptions.

## Test boundary and claims

- You demonstrate **broker/container failure tolerance** on one machine.
- You do **not** demonstrate loss of a physical host, multi-AZ durability or automatic disaster recovery.
- Acknowledgments improve durability under specified failure assumptions, not an absolute never-lose-data promise.
- Separate the producer's idempotent retry behavior from consumer DB idempotency.
- For a pure minISR test while keeping KRaft controller quorum healthy, use a failure targeted to replica synchronization instead of shutting down two combined nodes—or accurately explain both failure modes.

## Replication and comparison guardrail

The three-broker RF3/minISR2/`acks=all` Kafka setup is a **Kafka cluster fault-tolerance demonstration**, not automatically a fair throughput comparison to one-node Redis or RabbitMQ. Phase 9 single-node transport baselines use a separate clearly labeled Kafka RF1 profile; a replicated throughput comparison must document per-broker persistence and acknowledgment semantics. Do not mix single-node and replicated results on one unqualified chart. See docs 14 and 15.

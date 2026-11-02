import { describe, it, expect, beforeEach } from 'vitest';
import { SimEngine } from '../../src/simulation/engine/sim-engine';
import type { SimCustomNode, SimCustomEdge } from '../../src/simulation/domain/types';

describe('Milestone 3 — Failures, KRaft Quorum & Rebalancing', () => {
  let engine: SimEngine;

  beforeEach(() => {
    engine = new SimEngine();

    const testNodes: SimCustomNode[] = [
      {
        id: 'producer-1',
        type: 'producerNode',
        position: { x: 0, y: 0 },
        data: {
          id: 'producer-1',
          kind: 'producer',
          title: 'Producer',
          icon: '🚀',
          status: 'IDLE',
          messageKey: 'U002', // Maps to P0
          messageRate: 1,
          targetTopic: 'purchases',
          acks: '1',
          clientDelayMs: 20,
        },
      },
      {
        id: 'kafka-1',
        type: 'kafkaClusterNode',
        position: { x: 200, y: 0 },
        data: {
          id: 'kafka-1',
          kind: 'kafka-cluster',
          title: 'Kafka Cluster',
          icon: '📦',
          status: 'ACTIVE',
          clusterId: 'test-cluster',
          brokerCount: 3,
          brokerIds: [1, 2, 3],
          activeTopic: 'purchases',
          partitionCount: 4,
          replicationFactor: 3,
          minInsyncReplicas: 2,
          brokerLatencyMs: 10,
        },
      },
      {
        id: 'consumer-1',
        type: 'consumerNode',
        position: { x: 400, y: 0 },
        data: {
          id: 'consumer-1',
          kind: 'consumer',
          title: 'Consumer 1',
          icon: '👥',
          status: 'POLLING',
          instanceId: 'consumer-1',
          groupId: 'loyalty-v1',
          subscribedTopic: 'purchases',
          assignedPartitions: [0, 2],
          processingDelayMs: 50,
          concurrencySlots: 1,
          commitMode: 'auto',
          failureRate: 0,
        },
      },
      {
        id: 'consumer-2',
        type: 'consumerNode',
        position: { x: 400, y: 150 },
        data: {
          id: 'consumer-2',
          kind: 'consumer',
          title: 'Consumer 2',
          icon: '👥',
          status: 'POLLING',
          instanceId: 'consumer-2',
          groupId: 'loyalty-v1',
          subscribedTopic: 'purchases',
          assignedPartitions: [1, 3],
          processingDelayMs: 50,
          concurrencySlots: 1,
          commitMode: 'auto',
          failureRate: 0,
        },
      },
      {
        id: 'database-1',
        type: 'databaseNode',
        position: { x: 650, y: 0 },
        data: {
          id: 'database-1',
          kind: 'database',
          title: 'Database',
          icon: '🗄️',
          status: 'IDLE',
          engine: 'PostgreSQL 16',
          databaseName: 'loyalty_db',
          schemaTables: ['loyalty_accounts'],
          writeDelayMs: 20,
          committedRecordCount: 0,
        },
      },
      {
        id: 'dlq-1',
        type: 'deadLetterQueueNode',
        position: { x: 650, y: 150 },
        data: {
          id: 'dlq-1',
          kind: 'dead-letter-queue',
          title: 'Dead-Letter Queue',
          icon: '☠️',
          status: 'IDLE',
          topicName: 'purchases.DLQ',
          deadLetterCount: 0,
        },
      },
    ];

    const testEdges: SimCustomEdge[] = [
      {
        id: 'edge-p-k',
        source: 'producer-1',
        target: 'kafka-1',
        type: 'simFlowEdge',
        data: { portType: 'publish' },
      },
      {
        id: 'edge-k-c1',
        source: 'kafka-1',
        target: 'consumer-1',
        type: 'simFlowEdge',
        data: { portType: 'dispatch' },
      },
      {
        id: 'edge-k-c2',
        source: 'kafka-1',
        target: 'consumer-2',
        type: 'simFlowEdge',
        data: { portType: 'dispatch' },
      },
      {
        id: 'edge-c1-db',
        source: 'consumer-1',
        target: 'database-1',
        type: 'simFlowEdge',
        data: { portType: 'handler-action' },
      },
      {
        id: 'edge-c1-dlq',
        source: 'consumer-1',
        target: 'dlq-1',
        type: 'simFlowEdge',
        data: { portType: 'dlq-route' },
      },
    ];

    engine.setTopology(testNodes, testEdges);
    engine.clock.start();
  });

  describe('KRaft Broker Failure & Leader Election', () => {
    it('fails over partition leadership to an in-sync replica when leader broker crashes', () => {
      const p0 = engine.kafkaCluster.getPartition('purchases', 0);
      expect(p0).toBeDefined();
      const initialLeader = p0!.leaderBrokerId;

      // Crash the current leader broker
      engine.crashBroker(initialLeader);

      // Verify broker status is CRASHED
      expect(engine.kafkaCluster.brokers.get(initialLeader)?.status).toBe('CRASHED');

      // Verify partition 0 elected a new online leader from its ISR
      const newLeader = p0!.leaderBrokerId;
      expect(newLeader).not.toBe(initialLeader);
      expect(newLeader).toBeGreaterThan(0);
      expect(engine.kafkaCluster.brokers.get(newLeader)?.status).toBe('ONLINE');
    });

    it('transfers active KRaft controller to another voter broker if controller crashes', () => {
      // Find initial controller
      const initialController = Array.from(engine.kafkaCluster.brokers.values()).find(
        (b) => b.isController,
      );
      expect(initialController).toBeDefined();

      engine.crashBroker(initialController!.id);

      // Verify old controller is inactive
      expect(initialController!.isController).toBe(false);

      // Verify a new controller was elected among surviving online brokers
      const newController = Array.from(engine.kafkaCluster.brokers.values()).find(
        (b) => b.isController && b.status === 'ONLINE',
      );
      expect(newController).toBeDefined();
      expect(newController!.id).not.toBe(initialController!.id);
    });

    it('rejects partition writes when ISR falls below min.insync.replicas', () => {
      // purchases topic has min.insync.replicas = 2
      // Crash broker 1 and broker 2, leaving only 1 online broker
      engine.crashBroker(1);
      engine.crashBroker(2);

      const writeCheck = engine.kafkaCluster.canWriteToPartition('purchases', 0);
      expect(writeCheck.allowed).toBe(false);
      expect(writeCheck.reason).toContain('NOT_ENOUGH_REPLICAS');
    });

    it('allows revived broker to rejoin ISR and restore quorum', () => {
      engine.crashBroker(1);
      expect(engine.kafkaCluster.brokers.get(1)?.status).toBe('CRASHED');

      engine.reviveBroker(1);
      expect(engine.kafkaCluster.brokers.get(1)?.status).toBe('ONLINE');

      const p0 = engine.kafkaCluster.getPartition('purchases', 0);
      expect(p0?.isr).toContain(1);
    });
  });

  describe('Consumer Group Rebalancing on Instance Crash', () => {
    it('redistributes all partitions to surviving consumers when an instance crashes', () => {
      const group = engine.consumerGroups.get('loyalty-v1');
      expect(group).toBeDefined();

      // Initially, C1 has [0, 2] and C2 has [1, 3]
      expect(group!.members.get('consumer-1')?.assignedPartitions).toEqual([0, 2]);
      expect(group!.members.get('consumer-2')?.assignedPartitions).toEqual([1, 3]);

      // Crash consumer-1
      engine.crashConsumer('consumer-1');

      // Verify consumer-1 is CRASHED with 0 partitions
      const c1Node = engine.nodes.get('consumer-1');
      expect(c1Node?.data.status).toBe('CRASHED');
      expect(c1Node?.data.assignedPartitions).toEqual([]);

      // Verify consumer-2 immediately inherited all 4 partitions
      const c2Node = engine.nodes.get('consumer-2');
      expect(c2Node?.data.assignedPartitions).toEqual([0, 1, 2, 3]);
      expect(c2Node?.data.status).toBe('POLLING');
    });

    it('reclaims partitions when a crashed consumer revives and rejoins', () => {
      engine.crashConsumer('consumer-1');
      expect(engine.nodes.get('consumer-2')?.data.assignedPartitions).toEqual([0, 1, 2, 3]);

      // Revive consumer-1
      engine.reviveConsumer('consumer-1');

      // Partitions are rebalanced between C1 and C2
      const c1Partitions = engine.nodes.get('consumer-1')?.data.assignedPartitions as number[];
      const c2Partitions = engine.nodes.get('consumer-2')?.data.assignedPartitions as number[];

      expect(c1Partitions.length).toBe(2);
      expect(c2Partitions.length).toBe(2);
      expect(c1Partitions).toEqual([0, 2]);
      expect(c2Partitions).toEqual([1, 3]);
    });
  });

  describe('Dead-Letter Queue (DLQ) & Poison Pill Isolation', () => {
    it('diverts poison pill to DLQ node and unblocks partition offset', () => {
      // Dispatch poison pill targeted to consumer-1 (Key U002 -> P0 -> assigned to consumer-1)
      engine.dispatchProducerMessage('producer-1', 'POISON_PILL');

      // Step 1: APPEND_PARTITION
      const ev1 = engine.step();
      expect(ev1?.type).toBe('APPEND_PARTITION');

      // Step 2: START_PROCESSING
      const ev2 = engine.step();
      expect(ev2?.type).toBe('START_PROCESSING');

      // Step 3: COMPLETE_PROCESSING -> discovers poison pill and schedules ROUTE_DLQ
      const ev3 = engine.step();
      expect(ev3?.type).toBe('COMPLETE_PROCESSING');

      // Offset for partition 2 must be committed to prevent partition stall
      const group = engine.consumerGroups.get('loyalty-v1');
      expect(group?.getCommittedNextOffset('purchases', 2)).toBe(0);

      // Step 4: ROUTE_DLQ arrives at dlq-1 node
      const ev4 = engine.step();
      expect(ev4?.type).toBe('ROUTE_DLQ');
      expect(ev4?.sourceNodeId).toBe('dlq-1');

      const dlqNode = engine.nodes.get('dlq-1');
      expect(dlqNode?.data.deadLetterCount).toBe(1);
      expect(dlqNode?.data.status).toBe('ACTIVE');
      expect(engine.step()?.type).toBe('COMMIT_OFFSET');
      expect(group?.getCommittedNextOffset('purchases', 2)).toBe(1);

      // Verify Database received 0 writes (poison pill was not saved to DB)
      const dbNode = engine.nodes.get('database-1');
      expect(dbNode?.data.committedRecordCount).toBe(0);
    });
  });
});

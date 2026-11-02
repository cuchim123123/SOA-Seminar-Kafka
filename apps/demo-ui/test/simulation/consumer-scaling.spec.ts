import { describe, it, expect, beforeEach } from 'vitest';
import { SimConsumerGroupModel } from '../../src/simulation/domain/consumer-group';
import { SimEngine } from '../../src/simulation/engine/sim-engine';
import type { SimCustomNode, SimCustomEdge } from '../../src/simulation/domain/types';

describe('Milestone 2 — Kafka Semantics, Ordering & Consumer Scaling', () => {
  describe('Partition Assignor Strategies', () => {
    let group: SimConsumerGroupModel;

    beforeEach(() => {
      group = new SimConsumerGroupModel('loyalty-fleet');
      group.registerConsumer('c-1');
      group.registerConsumer('c-2');
    });

    it('assigns partitions using Round-Robin (interleaved)', () => {
      group.setAssignorStrategy('round-robin');
      const assignments = group.rebalance(4);

      expect(assignments.get('c-1')).toEqual([0, 2]);
      expect(assignments.get('c-2')).toEqual([1, 3]);
    });

    it('assigns partitions using Range (contiguous blocks)', () => {
      group.setAssignorStrategy('range');
      const assignments = group.rebalance(4);

      expect(assignments.get('c-1')).toEqual([0, 1]);
      expect(assignments.get('c-2')).toEqual([2, 3]);
    });

    it('handles uneven partitions in Range assignor correctly', () => {
      group.setAssignorStrategy('range');
      // 5 partitions among 2 consumers -> c-1 gets 3 partitions [0, 1, 2], c-2 gets 2 partitions [3, 4]
      const assignments = group.rebalance(5);

      expect(assignments.get('c-1')).toEqual([0, 1, 2]);
      expect(assignments.get('c-2')).toEqual([3, 4]);
    });

    it('marks surplus consumers as IDLE when consumer count > partition count', () => {
      group.registerConsumer('c-3');
      group.registerConsumer('c-4');
      group.registerConsumer('c-5');

      // 4 partitions among 5 consumers -> 4 active, 1 idle
      const assignments = group.rebalance(4);
      expect(assignments.get('c-1')?.length).toBeGreaterThan(0);
      expect(assignments.get('c-2')?.length).toBeGreaterThan(0);
      expect(assignments.get('c-3')?.length).toBeGreaterThan(0);
      expect(assignments.get('c-4')?.length).toBeGreaterThan(0);
      expect(assignments.get('c-5')).toEqual([]);
      expect(group.members.get('c-5')?.status).toBe('IDLE');
    });
  });

  describe('Strict Single-Partition Ordering Invariant', () => {
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
            brokerCount: 1,
            brokerIds: [1],
            activeTopic: 'purchases',
            partitionCount: 4,
            replicationFactor: 1,
            minInsyncReplicas: 1,
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
            title: 'Consumer',
            icon: '👥',
            status: 'POLLING',
            instanceId: 'c1',
            groupId: 'loyalty-v1',
            subscribedTopic: 'purchases',
            assignedPartitions: [0, 1, 2, 3],
            processingDelayMs: 100, // 100ms simulated processing
            concurrencySlots: 1,
            commitMode: 'auto',
            failureRate: 0,
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
          id: 'edge-k-c',
          source: 'kafka-1',
          target: 'consumer-1',
          type: 'simFlowEdge',
          data: { portType: 'dispatch' },
        },
      ];

      testNodes.push({
        id: 'database-1',
        type: 'databaseNode',
        position: { x: 600, y: 0 },
        data: {
          id: 'database-1',
          kind: 'database',
          title: 'DB',
          icon: 'DB',
          status: 'IDLE',
          engine: 'PostgreSQL',
          databaseName: 'projection',
          schemaTables: [],
          writeDelayMs: 20,
          committedRecordCount: 0,
        },
      });
      testEdges.push({
        id: 'edge-c-db',
        source: 'consumer-1',
        target: 'database-1',
        type: 'simFlowEdge',
        data: { portType: 'handler-action' },
      });
      engine.setTopology(testNodes, testEdges);
      engine.clock.start();
    });

    it('processes records on the same partition strictly sequentially (O=0 then O=1)', () => {
      // Send two records with the same customer key (both route to P0)
      engine.dispatchProducerMessage('producer-1', 'U002'); // Record #0
      engine.dispatchProducerMessage('producer-1', 'U002'); // Record #1

      // Advance clock until both reach the Kafka broker
      // Step 1: Record #0 APPEND_PARTITION at P0
      const ev1 = engine.step();
      expect(ev1?.type).toBe('APPEND_PARTITION');
      expect(ev1?.metadata?.partitionId).toBe(0);

      // Step 2: Record #1 APPEND_PARTITION at P0
      const ev2 = engine.step();
      expect(ev2?.type).toBe('APPEND_PARTITION');
      expect(ev2?.metadata?.partitionId).toBe(0);

      // Verify partition P0 log end offset is now 2
      const p0 = engine.kafkaCluster.getPartition('purchases', 0);
      expect(p0?.logEndOffset).toBe(2);

      // Step 3: Consumer polls Record #0 and starts processing
      const ev3 = engine.step();
      expect(ev3?.type).toBe('START_PROCESSING');
      expect(ev3?.metadata?.offset).toBe(0);

      const consumerNode = engine.nodes.get('consumer-1');
      expect(consumerNode?.data.status).toBe('PROCESSING');
      expect(consumerNode?.data.currentProcessingOffset).toBe(0);

      // At this point, partition 0 is in-flight! Record #1 must NOT be processed yet
      const group = engine.consumerGroups.get('loyalty-v1');
      expect(group?.isPartitionInFlight(0)).toBe(true);

      // Step 4: Record #0 finishes processing and commits offset 0 -> committedNextOffset = 1
      const ev4 = engine.step();
      expect(ev4?.type).toBe('COMPLETE_PROCESSING');
      expect(ev4?.metadata?.offset).toBe(0);
      expect(group?.getCommittedNextOffset('purchases', 0)).toBe(0);
      expect(engine.step()?.type).toBe('WRITE_DATABASE');
      expect(group?.getCommittedNextOffset('purchases', 0)).toBe(0);
      expect(engine.step()?.type).toBe('COMMIT_OFFSET');
      expect(group?.getCommittedNextOffset('purchases', 0)).toBe(1);

      // Step 5: Record #1 is now immediately scheduled and begins processing
      const ev5 = engine.step();
      expect(ev5?.type).toBe('START_PROCESSING');
      expect(ev5?.metadata?.offset).toBe(1);

      // Step 6: Record #1 completes processing and commits offset 1 -> committedNextOffset = 2
      const ev6 = engine.step();
      expect(ev6?.type).toBe('COMPLETE_PROCESSING');
      expect(ev6?.metadata?.offset).toBe(1);
      expect(engine.step()?.type).toBe('WRITE_DATABASE');
      expect(engine.step()?.type).toBe('COMMIT_OFFSET');
      expect(group?.getCommittedNextOffset('purchases', 0)).toBe(2);

      // Lag on P0 is now 0 (LEO 2 - Committed 2)
      expect(group?.calculateLag('purchases', 0, 2)).toBe(0);
    });
  });
});

import { describe, it, expect, beforeEach } from 'vitest';
import { SimPriorityQueue } from '../../src/simulation/engine/priority-queue';
import { SimClock } from '../../src/simulation/engine/sim-clock';
import { SimKafkaClusterModel } from '../../src/simulation/domain/kafka-cluster';
import { SimConsumerGroupModel } from '../../src/simulation/domain/consumer-group';
import { SimEngine } from '../../src/simulation/engine/sim-engine';
import { createSimEvent } from '../../src/simulation/domain/events';
import { createSimMessage } from '../../src/simulation/domain/message';
import { isValidConnection } from '../../src/simulation/registry/component-registry';
import type { SimCustomNode, SimCustomEdge } from '../../src/simulation/domain/types';

describe('Milestone 1 — Simulation Engine & Mechanics', () => {
  describe('SimPriorityQueue', () => {
    let queue: SimPriorityQueue;

    beforeEach(() => {
      queue = new SimPriorityQueue();
    });

    it('orders events strictly by scheduledTime ascending', () => {
      queue.push(createSimEvent(300, 'DISPATCH_MESSAGE', 'node-1', 'Event 300'));
      queue.push(createSimEvent(100, 'DISPATCH_MESSAGE', 'node-1', 'Event 100'));
      queue.push(createSimEvent(200, 'DISPATCH_MESSAGE', 'node-1', 'Event 200'));

      expect(queue.pop()?.scheduledTime).toBe(100);
      expect(queue.pop()?.scheduledTime).toBe(200);
      expect(queue.pop()?.scheduledTime).toBe(300);
      expect(queue.pop()).toBeUndefined();
    });

    it('tie-breaks equal scheduledTime using monotonic sequence counter', () => {
      const evA = createSimEvent(100, 'DISPATCH_MESSAGE', 'node-1', 'First at 100');
      const evB = createSimEvent(100, 'APPEND_PARTITION', 'node-2', 'Second at 100');

      queue.push(evA);
      queue.push(evB);

      expect(queue.pop()?.description).toBe('First at 100');
      expect(queue.pop()?.description).toBe('Second at 100');
    });
  });

  describe('SimClock', () => {
    it('advances virtual time based on speed multiplier', () => {
      const clock = new SimClock();
      clock.start();
      clock.advance(100);
      expect(clock.virtualTimeMs).toBe(100);

      clock.setSpeed(2.0);
      clock.advance(50);
      expect(clock.virtualTimeMs).toBe(200); // 100 + (50 * 2)

      clock.pause();
      clock.advance(100);
      expect(clock.virtualTimeMs).toBe(200); // no advance when paused
    });
  });

  describe('SimKafkaClusterModel', () => {
    it('distributes partitions and routes keys with exact Murmur2', () => {
      const cluster = new SimKafkaClusterModel('test-cluster', 3);
      cluster.createTopic('purchases', 4, 3, 2);

      const topic = cluster.topics.get('purchases');
      expect(topic).toBeDefined();
      expect(topic?.partitions.size).toBe(4);

      // Verify Murmur2 key routing
      const p1 = cluster.routeKeyToPartition('purchases', 'U001');
      const p2 = cluster.routeKeyToPartition('purchases', 'U002');
      expect(typeof p1).toBe('number');
      expect(p1).toBeGreaterThanOrEqual(0);
      expect(p1).toBeLessThan(4);
      expect(p2).toBeGreaterThanOrEqual(0);
      expect(p2).toBeLessThan(4);
    });

    it('appends records monotonically and advances logEndOffset', () => {
      const cluster = new SimKafkaClusterModel('test-cluster', 1);
      cluster.createTopic('purchases', 2, 1, 1);

      const msg1 = createSimMessage('U001', 1000, 50);
      const res1 = cluster.appendRecord('purchases', 0, msg1, 50);
      expect(res1.offset).toBe(0);
      expect(res1.partition.logEndOffset).toBe(1);

      const msg2 = createSimMessage('U002', 2000, 100);
      const res2 = cluster.appendRecord('purchases', 0, msg2, 100);
      expect(res2.offset).toBe(1);
      expect(res2.partition.logEndOffset).toBe(2);
    });
  });

  describe('SimConsumerGroupModel', () => {
    it('rebalances partitions evenly among consumers and computes true lag', () => {
      const group = new SimConsumerGroupModel('test-group');
      group.registerConsumer('c-1');
      group.registerConsumer('c-2');

      const assignments = group.rebalance(4);
      expect(assignments.get('c-1')).toEqual([0, 2]);
      expect(assignments.get('c-2')).toEqual([1, 3]);

      // Initially, committed offset is 0
      expect(group.getCommittedNextOffset('purchases', 0)).toBe(0);
      expect(group.calculateLag('purchases', 0, 5)).toBe(5);

      // Commit offset 4 (consumed record #4) -> committedNextOffset = 5
      group.commitOffset('purchases', 0, 4);
      expect(group.getCommittedNextOffset('purchases', 0)).toBe(5);
      expect(group.calculateLag('purchases', 0, 5)).toBe(0);
    });

    it('marks surplus consumers as IDLE when consumer count > partition count', () => {
      const group = new SimConsumerGroupModel('test-group');
      group.registerConsumer('c-1');
      group.registerConsumer('c-2');
      group.registerConsumer('c-3');

      // 2 partitions, 3 consumers -> 1 consumer must be idle
      const assignments = group.rebalance(2);
      expect(assignments.get('c-1')).toEqual([0]);
      expect(assignments.get('c-2')).toEqual([1]);
      expect(assignments.get('c-3')).toEqual([]);
      expect(group.members.get('c-3')?.status).toBe('IDLE');
    });
  });

  describe('Component Registry Connection Validation', () => {
    it('allows valid producer -> kafka -> consumer -> database pipeline', () => {
      expect(isValidConnection('producer', 'kafka-cluster')).toBe(true);
      expect(isValidConnection('kafka-cluster', 'consumer')).toBe(true);
      expect(isValidConnection('consumer', 'database')).toBe(true);
      expect(isValidConnection('consumer', 'worker-service')).toBe(true);
      expect(isValidConnection('worker-service', 'database')).toBe(true);
    });

    it('rejects semantically invalid connections', () => {
      expect(isValidConnection('database', 'producer')).toBe(false);
      expect(isValidConnection('database', 'kafka-cluster')).toBe(false);
      expect(isValidConnection('producer', 'database')).toBe(false);
      expect(isValidConnection('kafka-cluster', 'database')).toBe(false);
    });
  });

  describe('SimEngine Full End-to-End Pipeline', () => {
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
            messageKey: 'U001',
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
            partitionCount: 2,
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
            assignedPartitions: [0, 1],
            processingDelayMs: 50,
            concurrencySlots: 1,
            commitMode: 'auto',
            failureRate: 0,
          },
        },
        {
          id: 'database-1',
          type: 'databaseNode',
          position: { x: 600, y: 0 },
          data: {
            id: 'database-1',
            kind: 'database',
            title: 'Database',
            icon: '🗄️',
            status: 'IDLE',
            engine: 'PostgreSQL 16',
            databaseName: 'loyalty_db',
            schemaTables: ['loyalty_accounts'],
            writeDelayMs: 10,
            committedRecordCount: 0,
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
        {
          id: 'edge-c-d',
          source: 'consumer-1',
          target: 'database-1',
          type: 'simFlowEdge',
          data: { portType: 'handler-action' },
        },
      ];

      engine.setTopology(testNodes, testEdges);
    });

    it('traverses Producer -> Kafka -> Consumer -> Database via discrete events', () => {
      engine.clock.start();

      // Dispatch 1 message
      const msg = engine.dispatchProducerMessage('producer-1', 'U001');
      expect(msg).toBeDefined();

      // 1. Initial event is DISPATCH_MESSAGE
      expect(engine.eventLog[0].type).toBe('DISPATCH_MESSAGE');
      expect(engine.queue.size()).toBe(1);

      // Step 1: Arrive at Broker (APPEND_PARTITION)
      const ev1 = engine.step();
      expect(ev1?.type).toBe('APPEND_PARTITION');

      // Step 2: Poll and Start Processing
      const ev2 = engine.step();
      expect(ev2?.type).toBe('START_PROCESSING');

      // Step 3: Complete Processing and Commit Offset
      const ev3 = engine.step();
      expect(ev3?.type).toBe('COMPLETE_PROCESSING');

      // Step 4: Write to Database
      const ev4 = engine.step();
      expect(ev4?.type).toBe('WRITE_DATABASE');

      const dbNode = engine.nodes.get('database-1');
      expect(dbNode?.data.committedRecordCount).toBe(1);
    });
  });
});

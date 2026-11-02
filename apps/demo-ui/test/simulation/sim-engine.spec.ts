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

    it('pauses polling, accumulates partition lag, and drains backlog immediately upon resumption', () => {
      engine.clock.start();

      // 1. Pause consumer polling
      const isPaused = engine.togglePauseConsumer('consumer-1');
      expect(isPaused).toBe(true);

      const consumerNode = engine.nodes.get('consumer-1');
      expect(consumerNode?.data.status).toBe('PAUSED');

      // 2. Dispatch a message while paused
      const msg = engine.dispatchProducerMessage('producer-1', 'U001');
      expect(msg).toBeDefined();

      // Step 1: Arrives at Kafka broker
      const ev1 = engine.step();
      expect(ev1?.type).toBe('APPEND_PARTITION');

      // Consumer is paused, so NO START_PROCESSING event was scheduled!
      expect(engine.queue.size()).toBe(0);

      // Verify lag accumulated in consumer group
      const targetP = Number(ev1?.metadata?.partitionId ?? 0);
      const group = engine.consumerGroups.get('loyalty-v1');
      expect(group?.calculateLag('purchases', targetP, 1)).toBe(1);

      // 3. Resume consumer polling -> should immediately drain backlog!
      const isStillPaused = engine.togglePauseConsumer('consumer-1');
      expect(isStillPaused).toBe(false);
      expect(consumerNode?.data.status).toBe('POLLING');
      expect(engine.queue.size()).toBe(1);

      // Step 2: Poll and Start Processing
      const ev2 = engine.step();
      expect(ev2?.type).toBe('START_PROCESSING');

      // Step 3: Complete Processing
      const ev3 = engine.step();
      expect(ev3?.type).toBe('COMPLETE_PROCESSING');

      // Step 4: Write to Database
      const ev4 = engine.step();
      expect(ev4?.type).toBe('WRITE_DATABASE');

      const dbNode = engine.nodes.get('database-1');
      expect(dbNode?.data.committedRecordCount).toBe(1);
      expect(group?.calculateLag('purchases', targetP, 1)).toBe(1);
      expect(engine.step()?.type).toBe('COMMIT_OFFSET');
      expect(group?.calculateLag('purchases', targetP, 1)).toBe(0);
    });

    it('cuts individual partition polling wire and resumes backlog when reconnected', () => {
      engine.clock.start();

      // Find which partition U001 routes to
      const targetP = engine.kafkaCluster.routeKeyToPartition('purchases', 'U001');

      // Cut polling wire for target partition
      engine.togglePauseConsumerPartition('consumer-1', targetP);
      const consumerNode = engine.nodes.get('consumer-1');
      expect((consumerNode?.data.pausedPartitions as number[] | undefined)?.includes(targetP)).toBe(
        true,
      );

      // Dispatch message
      engine.dispatchProducerMessage('producer-1', 'U001');

      // Step 1: Arrives at Broker
      const ev1 = engine.step();
      expect(ev1?.type).toBe('APPEND_PARTITION');

      // Polling wire is severed for this partition -> queue is empty, lag accumulates!
      expect(engine.queue.size()).toBe(0);
      const group = engine.consumerGroups.get('loyalty-v1');
      expect(group?.calculateLag('purchases', targetP, 1)).toBe(1);

      // Reconnect wire -> drains backlog!
      engine.togglePauseConsumerPartition('consumer-1', targetP);
      expect((consumerNode?.data.pausedPartitions as number[] | undefined)?.includes(targetP)).toBe(
        false,
      );
      expect(engine.queue.size()).toBe(1);

      // Execute through to database
      engine.step(); // START_PROCESSING
      engine.step(); // COMPLETE_PROCESSING
      engine.step(); // WRITE_DATABASE

      const dbNode = engine.nodes.get('database-1');
      expect(dbNode?.data.committedRecordCount).toBe(1);
    });

    it('clears in-flight partition locks and resumes backlog when a consumer crashes mid-stream', () => {
      // Register second consumer so partitions can fail over
      const c2Node: SimCustomNode = {
        id: 'consumer-2',
        type: 'consumerNode',
        position: { x: 400, y: 150 },
        data: {
          id: 'consumer-2',
          kind: 'consumer',
          title: 'Consumer 2',
          status: 'POLLING',
          groupId: 'loyalty-v1',
          processingDelayMs: 50,
        } as unknown as SimCustomNode['data'],
      };
      const edgeK2 = {
        id: 'edge-k-c2',
        source: 'kafka-1',
        target: 'consumer-2',
        type: 'simFlowEdge',
      } as SimCustomEdge;
      const edgeC2D = {
        id: 'edge-c2-d',
        source: 'consumer-2',
        target: 'database-1',
        type: 'simFlowEdge',
      } as SimCustomEdge;

      engine.nodes.set('consumer-2', c2Node);
      engine.edges.set('edge-k-c2', edgeK2);
      engine.edges.set('edge-c2-d', edgeC2D);
      const currentNodes = Array.from(engine.nodes.values());
      const currentEdges = Array.from(engine.edges.values());
      engine.setTopology(currentNodes, currentEdges);

      engine.clock.start();

      // Dispatch message
      engine.dispatchProducerMessage('producer-1', 'U001');
      engine.step(); // APPEND_PARTITION
      const startEv = engine.step(); // START_PROCESSING
      expect(startEv?.type).toBe('START_PROCESSING');

      const targetConsumerId = startEv?.targetNodeId as string;
      const targetP = Number(startEv?.metadata?.partitionId ?? 0);
      const group = engine.consumerGroups.get('loyalty-v1')!;

      // Partition is locked during processing
      expect(group.isPartitionInFlight(targetP)).toBe(true);

      // Crash the processing consumer before it finishes
      engine.crashConsumer(targetConsumerId);

      // The surviving consumer has taken over the partition and acquired the in-flight lock for failover
      const survivingConsumerId = targetConsumerId === 'consumer-1' ? 'consumer-2' : 'consumer-1';
      const survivingMember = group.members.get(survivingConsumerId);
      expect(survivingMember?.assignedPartitions.includes(targetP)).toBe(true);
      expect(group.isPartitionInFlight(targetP)).toBe(true);

      // Queue has resumed processing on surviving consumer!
      expect(engine.queue.size()).toBeGreaterThan(0);
      const evResume = engine.step();
      expect(evResume?.type).toBe('START_PROCESSING');
      expect(evResume?.targetNodeId).toBe(survivingConsumerId);
    });

    it('routes ordered bursts and duplicate messages through Transactional Outbox and CDC relay', () => {
      const outboxNodes: SimCustomNode[] = [
        {
          id: 'producer-1',
          type: 'producerNode',
          position: { x: 0, y: 0 },
          data: { id: 'producer-1', kind: 'producer', title: 'Producer' } as SimCustomNode['data'],
        },
        {
          id: 'outbox-1',
          type: 'outboxTableNode',
          position: { x: 200, y: 0 },
          data: {
            id: 'outbox-1',
            kind: 'outbox-table',
            title: 'Outbox Table',
            pendingRecordsCount: 0,
            publishedRecordsCount: 0,
          } as SimCustomNode['data'],
        },
        {
          id: 'cdc-1',
          type: 'cdcRelayNode',
          position: { x: 400, y: 0 },
          data: {
            id: 'cdc-1',
            kind: 'cdc-relay',
            title: 'Debezium CDC',
            targetTopic: 'purchases',
          } as SimCustomNode['data'],
        },
        {
          id: 'kafka-1',
          type: 'kafkaClusterNode',
          position: { x: 600, y: 0 },
          data: { id: 'kafka-1', kind: 'kafka-cluster', title: 'Kafka' } as SimCustomNode['data'],
        },
      ];

      const outboxEdges: SimCustomEdge[] = [
        {
          id: 'e1',
          source: 'producer-1',
          target: 'outbox-1',
          type: 'simFlowEdge',
        } as SimCustomEdge,
        { id: 'e2', source: 'outbox-1', target: 'cdc-1', type: 'simFlowEdge' } as SimCustomEdge,
        { id: 'e3', source: 'cdc-1', target: 'kafka-1', type: 'simFlowEdge' } as SimCustomEdge,
      ];

      engine.setTopology(outboxNodes, outboxEdges);
      engine.clock.start();

      // A duplicate requires a real previous event; first publish and drain it.
      expect(engine.dispatchDuplicateMessage('U001')).toBeNull();
      engine.dispatchProducerMessage('producer-1', 'U001');
      while (engine.queue.size()) engine.step();
      engine.dispatchDuplicateMessage('U001');

      // Must be WRITE_OUTBOX
      const ev1 = engine.step();
      expect(ev1?.type).toBe('WRITE_OUTBOX');
      const outboxNode = engine.nodes.get('outbox-1');
      expect(outboxNode?.data.pendingRecordsCount).toBe(1);

      // CDC relay tails row
      const ev2 = engine.step();
      expect(ev2?.type).toBe('CDC_RELAY_STREAM');

      // CDC publishes to Kafka broker
      const ev3 = engine.step();
      expect(ev3?.type).toBe('APPEND_PARTITION');
      expect(ev3?.sourceNodeId).toBe('kafka-1');
    });

    it('flows duplicate request through outbox -> CDC -> Kafka -> consumer -> database when idempotency is disabled', () => {
      const fullNodes: SimCustomNode[] = [
        {
          id: 'prod-1',
          type: 'producerNode',
          position: { x: 0, y: 0 },
          data: { id: 'prod-1', kind: 'producer', title: 'Producer' } as SimCustomNode['data'],
        },
        {
          id: 'outbox-1',
          type: 'outboxTableNode',
          position: { x: 150, y: 0 },
          data: {
            id: 'outbox-1',
            kind: 'outbox-table',
            title: 'Outbox Table',
            pendingRecordsCount: 0,
            publishedRecordsCount: 0,
          } as SimCustomNode['data'],
        },
        {
          id: 'cdc-1',
          type: 'cdcRelayNode',
          position: { x: 300, y: 0 },
          data: {
            id: 'cdc-1',
            kind: 'cdc-relay',
            title: 'CDC Relay',
            targetTopic: 'purchases',
          } as SimCustomNode['data'],
        },
        {
          id: 'kafka-1',
          type: 'kafkaClusterNode',
          position: { x: 450, y: 0 },
          data: {
            id: 'kafka-1',
            kind: 'kafka-cluster',
            title: 'Kafka',
            activeTopic: 'purchases',
            partitionCount: 2,
          } as SimCustomNode['data'],
        },
        {
          id: 'cons-1',
          type: 'consumerNode',
          position: { x: 600, y: 0 },
          data: {
            id: 'cons-1',
            kind: 'consumer',
            title: 'Consumer',
            groupId: 'loyalty-v1',
            status: 'POLLING',
            assignedPartitions: [0, 1],
          } as SimCustomNode['data'],
        },
        {
          id: 'db-1',
          type: 'databaseNode',
          position: { x: 750, y: 0 },
          data: {
            id: 'db-1',
            kind: 'database',
            title: 'DB',
            committedRecordCount: 0,
          } as SimCustomNode['data'],
        },
      ];

      const fullEdges: SimCustomEdge[] = [
        { id: 'e1', source: 'prod-1', target: 'outbox-1', type: 'simFlowEdge' } as SimCustomEdge,
        { id: 'e2', source: 'outbox-1', target: 'cdc-1', type: 'simFlowEdge' } as SimCustomEdge,
        { id: 'e3', source: 'cdc-1', target: 'kafka-1', type: 'simFlowEdge' } as SimCustomEdge,
        { id: 'e4', source: 'kafka-1', target: 'cons-1', type: 'simFlowEdge' } as SimCustomEdge,
        { id: 'e5', source: 'cons-1', target: 'db-1', type: 'simFlowEdge' } as SimCustomEdge,
      ];

      engine.setTopology(fullNodes, fullEdges);
      engine.clock.start();

      // 1. Dispatch first purchase through outbox
      const msg1 = engine.dispatchProducerMessage('prod-1', 'U001');
      expect(msg1).not.toBeNull();
      expect(engine.lastDispatchedMessage?.id).toBe(msg1?.id);

      // Drain all events for first message
      let safety = 20;
      while (engine.queue.size() > 0 && safety-- > 0) {
        engine.step();
      }

      const dbNode = engine.nodes.get('db-1');
      expect(dbNode?.data.committedRecordCount).toBe(1);

      // 2. Disable consumer idempotency (hazard mode)
      engine.mechanicsConfig.consumerIdempotency = false;

      // 3. Dispatch DUPLICATE request through outbox
      const dup = engine.dispatchDuplicateMessage('U001');
      expect(dup).not.toBeNull();
      expect(dup?.id).toBe(msg1?.id);

      // Verify each stage executes and does not get stuck at outbox
      const executedTypes: string[] = [];
      safety = 25;
      while (engine.queue.size() > 0 && safety-- > 0) {
        const ev = engine.step();
        if (ev) executedTypes.push(ev.type);
      }

      expect(executedTypes).toContain('WRITE_OUTBOX');
      expect(executedTypes).toContain('CDC_RELAY_STREAM');
      expect(executedTypes).toContain('APPEND_PARTITION');
      expect(executedTypes).toContain('START_PROCESSING');
      expect(executedTypes).toContain('COMPLETE_PROCESSING');
      expect(executedTypes).toContain('WRITE_DATABASE');

      // Verify phantom duplicate write occurred in DB
      expect(dbNode?.data.committedRecordCount).toBe(2);
    });

    it('automatically routes poison pill to DLQ when DLQ node is wired, and stalls when unwired', () => {
      const baseNodes: SimCustomNode[] = [
        {
          id: 'kafka-1',
          type: 'kafkaClusterNode',
          position: { x: 0, y: 0 },
          data: {
            id: 'kafka-1',
            kind: 'kafka-cluster',
            activeTopic: 'purchases',
            partitionCount: 1,
          } as SimCustomNode['data'],
        },
        {
          id: 'cons-1',
          type: 'consumerNode',
          position: { x: 200, y: 0 },
          data: {
            id: 'cons-1',
            kind: 'consumer',
            groupId: 'loyalty-v1',
            status: 'POLLING',
            assignedPartitions: [0],
          } as SimCustomNode['data'],
        },
        {
          id: 'dlq-1',
          type: 'deadLetterQueueNode',
          position: { x: 400, y: 0 },
          data: {
            id: 'dlq-1',
            kind: 'dead-letter-queue',
            deadLetterCount: 0,
          } as SimCustomNode['data'],
        },
      ];

      // Case A: DLQ node is WIRED -> automatically routes to DLQ
      const wiredEdges: SimCustomEdge[] = [
        { id: 'ekc', source: 'kafka-1', target: 'cons-1', type: 'simFlowEdge' } as SimCustomEdge,
        { id: 'ecd', source: 'cons-1', target: 'dlq-1', type: 'simFlowEdge' } as SimCustomEdge,
      ];

      engine.setTopology(baseNodes, wiredEdges);
      engine.clock.start();

      const poisonMsg = createSimMessage('POISON_PILL', 9999, 100);
      engine.kafkaCluster.appendRecord('purchases', 0, poisonMsg, 100);
      engine.scheduleConsumerPoll('purchases', 0, 0, poisonMsg, 100);

      let safety = 10;
      const typesA: string[] = [];
      while (engine.queue.size() > 0 && safety-- > 0) {
        const ev = engine.step();
        if (ev) typesA.push(ev.type);
      }

      expect(typesA).toContain('ROUTE_DLQ');
      const dlqNode = engine.nodes.get('dlq-1');
      expect(dlqNode?.data.deadLetterCount).toBe(1);

      // Case B: DLQ node is UNWIRED -> stalls consumer with CONSUMER_ERROR
      const unwiredEdges: SimCustomEdge[] = [
        { id: 'ekc', source: 'kafka-1', target: 'cons-1', type: 'simFlowEdge' } as SimCustomEdge,
      ];
      engine.reset();
      engine.setTopology(baseNodes, unwiredEdges);
      engine.clock.start();

      const poisonMsg2 = createSimMessage('POISON_PILL', 9999, 200);
      engine.kafkaCluster.appendRecord('purchases', 0, poisonMsg2, 200);
      engine.scheduleConsumerPoll('purchases', 0, 0, poisonMsg2, 200);

      safety = 10;
      const typesB: string[] = [];
      while (engine.queue.size() > 0 && safety-- > 0) {
        const ev = engine.step();
        if (ev) typesB.push(ev.type);
      }

      expect(typesB).toContain('START_PROCESSING');
      expect(typesB).not.toContain('ROUTE_DLQ');
      const consNode = engine.nodes.get('cons-1');
      expect(consNode?.data.status).toBe('ERROR');
    });

    it('preserves topic records and monotonically advances offsets across re-renders and consecutive requests', () => {
      const nodes: SimCustomNode[] = [
        {
          id: 'producer-1',
          type: 'producerNode',
          position: { x: 0, y: 0 },
          data: {
            id: 'producer-1',
            kind: 'producer',
            messageKey: 'U001',
            targetTopic: 'purchases',
          } as SimCustomNode['data'],
        },
        {
          id: 'kafka-1',
          type: 'kafkaClusterNode',
          position: { x: 200, y: 0 },
          data: {
            id: 'kafka-1',
            kind: 'kafka-cluster',
            activeTopic: 'purchases',
            partitionCount: 2,
          } as SimCustomNode['data'],
        },
        {
          id: 'cons-1',
          type: 'consumerNode',
          position: { x: 400, y: 0 },
          data: {
            id: 'cons-1',
            kind: 'consumer',
            groupId: 'loyalty-v1',
            subscribedTopic: 'purchases',
            assignedPartitions: [0, 1],
          } as SimCustomNode['data'],
        },
        {
          id: 'db-1',
          type: 'databaseNode',
          position: { x: 600, y: 0 },
          data: { id: 'db-1', kind: 'database' } as SimCustomNode['data'],
        },
      ];
      const edges: SimCustomEdge[] = [
        {
          id: 'e-pk',
          source: 'producer-1',
          target: 'kafka-1',
          type: 'simFlowEdge',
        } as SimCustomEdge,
        { id: 'e-kc', source: 'kafka-1', target: 'cons-1', type: 'simFlowEdge' } as SimCustomEdge,
        { id: 'e-cd', source: 'cons-1', target: 'db-1', type: 'simFlowEdge' } as SimCustomEdge,
      ];

      engine.setTopology(nodes, edges);
      engine.clock.start();

      // Dispatch request 1
      engine.dispatchProducerMessage('producer-1', 'U001');

      // Drain queue for request 1
      let safety = 20;
      while (engine.queue.size() > 0 && safety-- > 0) {
        engine.step();
      }

      const dbNode = engine.nodes.get('db-1');
      expect(dbNode?.data.committedRecordCount).toBe(1);

      // Re-call setTopology (simulating React node drag or UI re-render)
      engine.setTopology(nodes, edges);

      // Verify records and LEO were NOT wiped to 0!
      const targetP = engine.kafkaCluster.routeKeyToPartition('purchases', 'U001');
      const partition = engine.kafkaCluster.getPartition('purchases', targetP);
      expect(partition?.logEndOffset).toBe(1);
      expect(partition?.records.length).toBe(1);

      // Dispatch request 2
      engine.dispatchProducerMessage('producer-1', 'U001');

      // Drain queue for request 2
      safety = 20;
      while (engine.queue.size() > 0 && safety-- > 0) {
        engine.step();
      }

      // Request 2 must NOT stall at broker! It must reach database as 2nd committed record!
      expect(dbNode?.data.committedRecordCount).toBe(2);
      expect(partition?.logEndOffset).toBe(2);
      expect(partition?.records.length).toBe(2);
    });
  });
});

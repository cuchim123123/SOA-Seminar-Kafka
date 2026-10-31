import { describe, it, expect, beforeEach } from 'vitest';
import { SimEngine } from '../../src/simulation/engine/sim-engine';
import type { SimCustomNode, SimCustomEdge } from '../../src/simulation/domain/types';

describe('Custom Component Naming, Multiple Consumer Groups & MongoDB Projections', () => {
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
          title: 'Checkout API Gateway',
          icon: '🛒',
          status: 'IDLE',
          messageKey: 'U001', // Murmur2 -> P3
          messageRate: 1,
          targetTopic: 'purchases',
          acks: '1',
          clientDelayMs: 20,
        },
      },
      {
        id: 'kafka-1',
        type: 'kafkaClusterNode',
        position: { x: 250, y: 0 },
        data: {
          id: 'kafka-1',
          kind: 'kafka-cluster',
          title: 'Retail Event Hub',
          icon: '📦',
          status: 'ACTIVE',
          clusterId: 'event-hub',
          brokerCount: 3,
          brokerIds: [1, 2, 3],
          activeTopic: 'purchases',
          partitionCount: 4,
          replicationFactor: 3,
          minInsyncReplicas: 2,
          brokerLatencyMs: 10,
        },
      },
      // Group 1: Loyalty Consumer -> PostgreSQL
      {
        id: 'consumer-loyalty',
        type: 'consumerNode',
        position: { x: 500, y: -80 },
        data: {
          id: 'consumer-loyalty',
          kind: 'consumer',
          title: 'Loyalty Worker',
          icon: '👥',
          status: 'POLLING',
          instanceId: 'consumer-loyalty',
          groupId: 'loyalty-v1',
          subscribedTopic: 'purchases',
          assignedPartitions: [0, 1, 2, 3],
          processingDelayMs: 50,
          concurrencySlots: 1,
          commitMode: 'auto',
          failureRate: 0,
        },
      },
      {
        id: 'db-postgres',
        type: 'databaseNode',
        position: { x: 750, y: -80 },
        data: {
          id: 'db-postgres',
          kind: 'database',
          title: 'PostgreSQL DB',
          icon: '🗄️',
          status: 'IDLE',
          engine: 'PostgreSQL 16',
          databaseName: 'loyalty_db',
          schemaTables: ['loyalty_accounts'],
          writeDelayMs: 15,
          committedRecordCount: 0,
        },
      },
      // Group 2: Analytics Consumer -> MongoDB Projection
      {
        id: 'consumer-analytics',
        type: 'consumerNode',
        position: { x: 500, y: 120 },
        data: {
          id: 'consumer-analytics',
          kind: 'consumer',
          title: 'Analytic Consumer',
          icon: '📈',
          status: 'POLLING',
          instanceId: 'consumer-analytics',
          groupId: 'analytics-v1',
          subscribedTopic: 'purchases',
          assignedPartitions: [0, 1, 2, 3],
          processingDelayMs: 60,
          concurrencySlots: 1,
          commitMode: 'auto',
          failureRate: 0,
        },
      },
      {
        id: 'db-mongo',
        type: 'databaseNode',
        position: { x: 750, y: 120 },
        data: {
          id: 'db-mongo',
          kind: 'database',
          title: 'MongoDB Projection',
          icon: '🍃',
          status: 'IDLE',
          engine: 'MongoDB 7',
          databaseName: 'analytics_db',
          schemaTables: ['sales_summaries'],
          writeDelayMs: 20,
          committedRecordCount: 0,
        },
      },
    ];

    const testEdges: SimCustomEdge[] = [
      {
        id: 'edge-prod-kafka',
        source: 'producer-1',
        target: 'kafka-1',
        type: 'simFlowEdge',
        data: { portType: 'publish' },
      },
      // Kafka -> Loyalty Group
      {
        id: 'edge-kafka-loyalty',
        source: 'kafka-1',
        target: 'consumer-loyalty',
        type: 'simFlowEdge',
        data: { portType: 'dispatch' },
      },
      {
        id: 'edge-loyalty-postgres',
        source: 'consumer-loyalty',
        target: 'db-postgres',
        type: 'simFlowEdge',
        data: { portType: 'handler-action' },
      },
      // Kafka -> Analytics Group
      {
        id: 'edge-kafka-analytics',
        source: 'kafka-1',
        target: 'consumer-analytics',
        type: 'simFlowEdge',
        data: { portType: 'dispatch' },
      },
      {
        id: 'edge-analytics-mongo',
        source: 'consumer-analytics',
        target: 'db-mongo',
        type: 'simFlowEdge',
        data: { portType: 'handler-action' },
      },
    ];

    engine.setTopology(testNodes, testEdges);
    engine.clock.start();
  });

  it('correctly tracks custom titles and storage engines on nodes', () => {
    const producer = engine.nodes.get('producer-1');
    expect(producer?.data.title).toBe('Checkout API Gateway');

    const loyaltyConsumer = engine.nodes.get('consumer-loyalty');
    expect(loyaltyConsumer?.data.title).toBe('Loyalty Worker');
    expect(loyaltyConsumer?.data.groupId).toBe('loyalty-v1');

    const analyticsConsumer = engine.nodes.get('consumer-analytics');
    expect(analyticsConsumer?.data.title).toBe('Analytic Consumer');
    expect(analyticsConsumer?.data.groupId).toBe('analytics-v1');

    const mongoDb = engine.nodes.get('db-mongo');
    expect(mongoDb?.data.title).toBe('MongoDB Projection');
    expect(mongoDb?.data.engine).toBe('MongoDB 7');
  });

  it('broadcasts identical purchase event to multiple independent consumer groups', () => {
    // Both loyalty-v1 and analytics-v1 should be registered
    expect(engine.consumerGroups.has('loyalty-v1')).toBe(true);
    expect(engine.consumerGroups.has('analytics-v1')).toBe(true);

    const loyaltyGroup = engine.consumerGroups.get('loyalty-v1');
    const analyticsGroup = engine.consumerGroups.get('analytics-v1');

    // Both groups own all 4 partitions
    expect(loyaltyGroup?.members.get('consumer-loyalty')?.assignedPartitions).toEqual([0, 1, 2, 3]);
    expect(analyticsGroup?.members.get('consumer-analytics')?.assignedPartitions).toEqual([
      0, 1, 2, 3,
    ]);

    // Dispatch 1 message from Checkout API Gateway
    const targetP = engine.kafkaCluster.routeKeyToPartition('purchases', 'U001');
    engine.dispatchProducerMessage('producer-1', 'U001');

    // Step 1: APPEND_PARTITION to Kafka Cluster
    const ev1 = engine.step();
    expect(ev1?.type).toBe('APPEND_PARTITION');

    // Kafka appends to target partition (LEO becomes 1)
    const partition = engine.kafkaCluster.getPartition('purchases', targetP);
    expect(partition?.logEndOffset).toBe(1);

    // Both consumer groups should schedule polls!
    // Step through queue until both consumers finish and write to their databases
    let postgresWrites = 0;
    let mongoWrites = 0;

    while (engine.queue.size() > 0) {
      const ev = engine.step();
      if (ev?.type === 'WRITE_DATABASE') {
        if (ev.sourceNodeId === 'db-postgres') postgresWrites++;
        if (ev.sourceNodeId === 'db-mongo') mongoWrites++;
      }
    }

    // Verify both PostgreSQL AND MongoDB projection received 1 committed write!
    expect(postgresWrites).toBe(1);
    expect(mongoWrites).toBe(1);

    const postgresNode = engine.nodes.get('db-postgres');
    const mongoNode = engine.nodes.get('db-mongo');

    expect(postgresNode?.data.committedRecordCount).toBe(1);
    expect(mongoNode?.data.committedRecordCount).toBe(1);

    // Verify independent offset commitments
    expect(loyaltyGroup?.getCommittedNextOffset('purchases', targetP)).toBe(1);
    expect(analyticsGroup?.getCommittedNextOffset('purchases', targetP)).toBe(1);
  });
});

import { describe, it, expect } from 'vitest';
import { SYSTEM_BLUEPRINTS } from '../../src/simulation/blueprints/systemBlueprints';
import { isValidConnection } from '../../src/simulation/registry/component-registry';
import { SimEngine } from '../../src/simulation/engine/sim-engine';
import type { SimCustomNode } from '../../src/simulation/domain/types';

describe('System Architecture Blueprints', () => {
  it('provides the required set of architectural blueprints', () => {
    const ids = SYSTEM_BLUEPRINTS.map((b) => b.id);
    expect(ids).toContain('full-enterprise-production');
    expect(ids).toContain('minimal-system');
    expect(ids).toContain('transactional-outbox-cdc');
    expect(ids).toContain('competing-consumers-scaling');
    expect(ids).toContain('dead-letter-queue-isolation');
  });

  it('validates that every edge in each blueprint satisfies connection topology rules', () => {
    for (const blueprint of SYSTEM_BLUEPRINTS) {
      const nodeMap = new Map(blueprint.nodes.map((n) => [n.id, n]));

      for (const edge of blueprint.edges) {
        const sourceNode = nodeMap.get(edge.source);
        const targetNode = nodeMap.get(edge.target);

        expect(
          sourceNode,
          `Source node "${edge.source}" not found in blueprint "${blueprint.id}"`,
        ).toBeDefined();
        expect(
          targetNode,
          `Target node "${edge.target}" not found in blueprint "${blueprint.id}"`,
        ).toBeDefined();

        const valid = isValidConnection(sourceNode!.data.kind, targetNode!.data.kind);
        expect(
          valid,
          `Invalid connection in blueprint "${blueprint.id}": ${sourceNode!.data.kind} -> ${targetNode!.data.kind}`,
        ).toBe(true);
      }
    }
  });

  it('loads Full Enterprise Production System into SimEngine with dual consumer groups and outbox CDC', () => {
    const enterprise = SYSTEM_BLUEPRINTS.find((b) => b.id === 'full-enterprise-production')!;
    expect(enterprise).toBeDefined();
    expect(enterprise.nodes.length).toBe(10);
    expect(enterprise.edges.length).toBe(10);

    const engine = new SimEngine();
    const clonedNodes: SimCustomNode[] = JSON.parse(JSON.stringify(enterprise.nodes));
    engine.setTopology(clonedNodes, enterprise.edges);

    // Verify consumer groups are registered
    expect(engine.consumerGroups.has('loyalty-v1')).toBe(true);
    expect(engine.consumerGroups.has('analytics-v1')).toBe(true);

    // Verify fleet rebalance
    const loyaltyGroup = engine.consumerGroups.get('loyalty-v1')!;
    expect(loyaltyGroup.members.size).toBe(2);

    const analyticsGroup = engine.consumerGroups.get('analytics-v1')!;
    expect(analyticsGroup.members.size).toBe(1);

    // Verify dispatch writes to outbox
    const pNode = clonedNodes.find((n) => n.data.kind === 'producer')!;
    const msg = engine.dispatchProducerMessage(pNode.id, 'U001');
    expect(msg).toBeDefined();

    // Verify event queue received WRITE_OUTBOX
    const outboxEvent = engine.queue.peek();
    expect(outboxEvent).toBeDefined();
    expect(outboxEvent?.type).toBe('WRITE_OUTBOX');
  });

  it('loads Minimal System into SimEngine and dispatches directly to Kafka', () => {
    const minimal = SYSTEM_BLUEPRINTS.find((b) => b.id === 'minimal-system')!;
    expect(minimal).toBeDefined();
    expect(minimal.nodes.length).toBe(4);
    expect(minimal.edges.length).toBe(3);

    const engine = new SimEngine();
    const clonedNodes: SimCustomNode[] = JSON.parse(JSON.stringify(minimal.nodes));
    engine.setTopology(clonedNodes, minimal.edges);

    // Verify 2 partitions topic initialized
    const topic = engine.kafkaCluster.topics.get('purchases');
    expect(topic).toBeDefined();
    expect(topic?.partitions.size).toBe(2);

    // Single consumer receives both partitions
    const consumer = engine.nodes.get('node-consumer-1')!;
    expect(consumer.data.assignedPartitions).toEqual([0, 1]);

    // Dispatch directly to Kafka
    const msg = engine.dispatchProducerMessage('node-producer-1', 'U002');
    expect(msg).toBeDefined();

    const appendEvent = engine.queue.peek();
    expect(appendEvent?.type).toBe('APPEND_PARTITION');
  });
});

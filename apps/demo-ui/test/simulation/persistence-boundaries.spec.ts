import { expect, it } from 'vitest';
import { SimEngine } from '../../src/simulation/engine/sim-engine';
import { minimalSystemBlueprint } from '../../src/simulation/blueprints/catalog/minimalSystemBlueprint';

function fixture() {
  const engine = new SimEngine();
  const nodes = structuredClone(minimalSystemBlueprint.nodes);
  engine.setTopology(nodes, structuredClone(minimalSystemBlueprint.edges));
  engine.setMechanicsConfig({ autoOffsetReset: 'earliest', commitMode: 'manual' });
  const producer = nodes.find((node) => node.data.kind === 'producer')!;
  const consumer = nodes.find((node) => node.data.kind === 'consumer')!;
  const database = nodes.find((node) => node.data.kind === 'database')!;
  engine.dispatchProducerMessage(producer.id, 'U002');
  return { engine, consumer, database };
}
function until(engine: SimEngine, type: string) {
  for (let i = 0; i < 100; i++) {
    const event = engine.step();
    if (event?.type === type) return event;
    if (!event) break;
  }
  throw new Error(`Missing ${type}`);
}
function drain(engine: SimEngine) {
  for (let i = 0; i < 100 && engine.queue.size(); i++) engine.step();
  expect(engine.queue.size()).toBe(0);
}

it('moving a node does not rebalance or overwrite an active delivery', () => {
  const { engine, consumer } = fixture();
  until(engine, 'START_PROCESSING');
  const member = engine.consumerGroups
    .get(String(consumer.data.groupId))!
    .members.get(consumer.id)!;
  const deliveryId = member.deliveryId;
  const pending = engine.queue.toArray().map((event) => event.id);
  engine.setTopology(
    Array.from(engine.nodes.values()).map((node) => ({
      ...node,
      position: { x: node.position.x + 10, y: node.position.y },
    })),
    Array.from(engine.edges.values()),
  );
  expect(engine.nodes.get(consumer.id)?.data.status).toBe('PROCESSING');
  expect(member.deliveryId).toBe(deliveryId);
  expect(engine.queue.toArray().map((event) => event.id)).toEqual(pending);
  drain(engine);
});

it('reset clears crashed membership and runtime while preserving topic configuration', () => {
  const { engine, consumer, database } = fixture();
  engine.kafkaCluster.reassignReplicas('purchases', 1);
  engine.kafkaCluster.topics.get('purchases')!.minInsyncReplicas = 1;
  engine.crashConsumer(consumer.id);
  engine.reset();
  expect(engine.clock.status).toBe('PAUSED');
  expect(engine.kafkaCluster.topics.get('purchases')?.replicationFactor).toBe(1);
  const group = engine.consumerGroups.get(String(consumer.data.groupId))!;
  expect(group.members.get(consumer.id)?.status).not.toBe('CRASHED');
  expect(group.members.get(consumer.id)?.deliveryId).toBeUndefined();
  const producer = Array.from(engine.nodes.values()).find((node) => node.data.kind === 'producer')!;
  engine.dispatchProducerMessage(producer.id, 'U002');
  drain(engine);
  expect(database.data.committedRecordCount).toBe(1);
});

it('replica reassignment preserves partition records and offsets', () => {
  const { engine } = fixture();
  const event = until(engine, 'APPEND_PARTITION');
  const partition = engine.kafkaCluster.getPartition(
    'purchases',
    Number(event.metadata?.partitionId),
  )!;
  const records = partition.records;
  engine.kafkaCluster.reassignReplicas('purchases', 1);
  expect(partition.records).toBe(records);
  expect(partition.logEndOffset).toBe(1);
  expect(partition.highWatermark).toBe(1);
  expect(() => engine.kafkaCluster.reassignReplicas('purchases', 99)).toThrow();
  expect(partition.records).toHaveLength(1);
});

it('records a broker rejection instead of terminating the animation loop', () => {
  const { engine, database } = fixture();
  engine.kafkaCluster.topics.get('purchases')!.minInsyncReplicas = 2;
  engine.kafkaCluster.reassignReplicas('purchases', 1);
  expect(() => engine.step()).not.toThrow();
  expect(engine.eventLog.some((event) => event.type === 'PRODUCE_REJECTED')).toBe(true);
  expect(database.data.committedRecordCount).toBe(0);
  expect(engine.kafkaCluster.getPartition('purchases', 0)?.logEndOffset).toBe(0);
});

it('distinguishes reset run settings from restoring scenario defaults', () => {
  const { engine } = fixture();
  engine.setMechanicsConfig({ consumerIdempotency: false, producerAcks: '1' });
  engine.reset();
  expect(engine.mechanicsConfig.consumerIdempotency).toBe(false);
  expect(engine.mechanicsConfig.producerAcks).toBe('1');
  engine.reset(false);
  expect(engine.mechanicsConfig.consumerIdempotency).toBe(true);
  expect(engine.mechanicsConfig.producerAcks).toBe('all');
  expect(engine.mechanicsConfig.partitioningStrategy).toBe('murmur2-key');
});

it.each([0, 2])('does not silently commit with %s database sinks', (count) => {
  const { engine, consumer, database } = fixture();
  for (const [id, edge] of engine.edges) if (edge.target === database.id) engine.edges.delete(id);
  for (let index = 0; index < count; index++) {
    const id = `sink-${index}`;
    engine.nodes.set(id, { ...structuredClone(database), id });
    engine.edges.set(`sink-edge-${index}`, {
      id: `sink-edge-${index}`,
      source: consumer.id,
      target: id,
    });
  }
  until(engine, 'COMPLETE_PROCESSING');
  expect(consumer.data.status).toBe('ERROR');
  expect(engine.processedEvents.size).toBe(0);
  expect(
    engine.consumerGroups
      .get(String(consumer.data.groupId))
      ?.getCommittedNextOffset('purchases', 0),
  ).toBe(0);
});

it('does not commit or create a dedup receipt before the database write', () => {
  const { engine, consumer, database } = fixture();
  const completed = until(engine, 'COMPLETE_PROCESSING');
  const group = engine.consumerGroups.get(String(consumer.data.groupId))!;
  const partition = Number(completed.metadata?.partitionId);
  expect(group.getCommittedNextOffset('purchases', partition)).toBe(0);
  expect(engine.processedEvents.size).toBe(0);
  expect(database.data.committedRecordCount).toBe(0);
  until(engine, 'WRITE_DATABASE');
  expect(database.data.committedRecordCount).toBe(1);
  expect(group.getCommittedNextOffset('purchases', partition)).toBe(0);
  until(engine, 'COMMIT_OFFSET');
  expect(group.getCommittedNextOffset('purchases', partition)).toBe(1);
});

it.each(['COMPLETE_PROCESSING', 'WRITE_DATABASE'])(
  'redelivers after crash at %s without ghost or duplicate persistence',
  (boundary) => {
    const { engine, consumer, database } = fixture();
    until(engine, boundary);
    engine.crashConsumer(consumer.id);
    engine.reviveConsumer(consumer.id);
    drain(engine);
    expect(database.data.committedRecordCount).toBe(1);
    expect(engine.processedEvents.size).toBe(1);
  },
);

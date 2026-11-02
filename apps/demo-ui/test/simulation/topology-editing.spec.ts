import { expect, it } from 'vitest';
import { SimEngine } from '../../src/simulation/engine/sim-engine';
import { SYSTEM_BLUEPRINTS } from '../../src/simulation/blueprints/systemBlueprints';
import { minimalSystemBlueprint } from '../../src/simulation/blueprints/catalog/minimalSystemBlueprint';
import { validateTopology } from '../../src/simulation/domain/topology-validation';
import { scaleConsumers } from '../../src/simulation/domain/scale-consumers';

function fixture() {
  const nodes = structuredClone(minimalSystemBlueprint.nodes),
    edges = structuredClone(minimalSystemBlueprint.edges);
  const engine = new SimEngine();
  engine.setTopology(nodes, edges);
  engine.setMechanicsConfig({ autoOffsetReset: 'earliest', commitMode: 'manual' });
  const consumer = nodes.find((node) => node.data.kind === 'consumer')!;
  const producer = nodes.find((node) => node.data.kind === 'producer')!;
  const db = nodes.find((node) => node.data.kind === 'database')!;
  let sequence = 0;
  return { nodes, edges, engine, consumer, producer, db, allocate: () => `scaled-${++sequence}` };
}
function advance(engine: SimEngine, type?: string) {
  for (let i = 0; i < 200; i++) {
    const event = engine.step();
    if (type && event?.type === type) return event;
    if (!event) return;
  }
  throw new Error('Unexpected unbounded work');
}

it('validates every supplied blueprint with its supported connections', () => {
  for (const blueprint of SYSTEM_BLUEPRINTS)
    expect(
      validateTopology(blueprint.nodes, blueprint.edges, new Set(['purchases'])),
      blueprint.id,
    ).toEqual([]);
});

it('scales a group using unique identities and all input/sink/DLQ wires, then removes attached wires', () => {
  const { nodes, edges, consumer, allocate } = fixture();
  const expanded = scaleConsumers(nodes, edges, String(consumer.data.groupId), 3, allocate);
  expect(expanded.nodes.filter((node) => node.data.kind === 'consumer')).toHaveLength(3);
  expect(validateTopology(expanded.nodes, expanded.edges, new Set(['purchases']))).toEqual([]);
  expect(
    new Set(
      expanded.nodes
        .filter((node) => node.data.kind === 'consumer')
        .map((node) => node.data.instanceId),
    ).size,
  ).toBe(3);
  const shrunk = scaleConsumers(
    expanded.nodes,
    expanded.edges,
    String(consumer.data.groupId),
    1,
    allocate,
  );
  expect(
    shrunk.edges.every(
      (edge) =>
        shrunk.nodes.some((node) => node.id === edge.source) &&
        shrunk.nodes.some((node) => node.id === edge.target),
    ),
  ).toBe(true);
  expect(() => scaleConsumers(nodes, edges, String(consumer.data.groupId), 0, allocate)).toThrow();
});

it('rejects duplicate edges, ghost endpoints, broker identity collisions and conflicting group sinks/topics', () => {
  const { nodes, edges, consumer, db, allocate } = fixture();
  expect(validateTopology(nodes, [...edges, { ...edges[0], id: 'duplicate' }]).join(' ')).toContain(
    'Duplicate connection',
  );
  expect(
    validateTopology(nodes, [...edges, { id: 'ghost', source: 'missing', target: db.id }]).join(
      ' ',
    ),
  ).toContain('missing endpoint');
  const broker = nodes.find((node) => node.data.kind === 'kafka-broker')!;
  expect(validateTopology([...nodes, { ...broker, id: 'other' }], edges).join(' ')).toContain(
    'Broker IDs',
  );
  const expanded = scaleConsumers(nodes, edges, String(consumer.data.groupId), 2, allocate);
  const replica = expanded.nodes.find((node) => node.id === 'scaled-1')!;
  replica.data.subscribedTopic = 'other';
  expect(
    validateTopology(expanded.nodes, expanded.edges, new Set(['purchases', 'other'])).join(' '),
  ).toContain('replicas must share');
  replica.data.subscribedTopic = 'purchases';
  expanded.nodes.push({ ...structuredClone(db), id: 'other-db' });
  expanded.edges = expanded.edges.map((edge) =>
    edge.source === replica.id ? { ...edge, target: 'other-db' } : edge,
  );
  expect(validateTopology(expanded.nodes, expanded.edges).join(' ')).toContain(
    'replicas must share',
  );
  replica.data.groupId = 'independent';
  expect(validateTopology(expanded.nodes, expanded.edges)).toEqual([]);
});

it.each(['COMPLETE_PROCESSING', 'WRITE_DATABASE'])(
  'scaling at %s pauses and redelivers once without stale completion writes',
  (boundary) => {
    const { nodes, edges, engine, consumer, producer, db, allocate } = fixture();
    engine.dispatchProducerMessage(producer.id, 'U002');
    advance(engine, boundary);
    const expanded = scaleConsumers(nodes, edges, String(consumer.data.groupId), 3, allocate);
    engine.setTopology(expanded.nodes, expanded.edges);
    expect(engine.clock.status).toBe('PAUSED');
    expect(engine.kafkaCluster.getPartition('purchases', 0)?.logEndOffset).toBe(1);
    advance(engine);
    expect(db.data.committedRecordCount).toBe(1);
    expect(
      engine.consumerGroups
        .get(String(consumer.data.groupId))
        ?.getCommittedNextOffset('purchases', 0),
    ).toBe(1);
  },
);

it('deleting a consumer cancels its pending sink write', () => {
  const { nodes, edges, engine, consumer, producer, db } = fixture();
  engine.dispatchProducerMessage(producer.id, 'U002');
  advance(engine, 'COMPLETE_PROCESSING');
  engine.setTopology(
    nodes.filter((node) => node.id !== consumer.id),
    edges.filter((edge) => edge.source !== consumer.id && edge.target !== consumer.id),
  );
  advance(engine);
  expect(db.data.committedRecordCount).toBe(0);
});

it('keeps subscriptions, polling and offset reset scoped to the group topic', () => {
  const { nodes, edges, engine, consumer, producer, db, allocate } = fixture();
  engine.kafkaCluster.createTopic('other', 2, 1, 1);
  const expanded = scaleConsumers(nodes, edges, String(consumer.data.groupId), 2, allocate);
  const replica = expanded.nodes.find((node) => node.id === 'scaled-1')!;
  replica.data.groupId = 'other-group';
  replica.data.subscribedTopic = 'other';
  const secondDb = { ...structuredClone(db), id: 'second-db' };
  expanded.nodes.push(secondDb);
  expanded.edges = expanded.edges.map((edge) =>
    edge.source === replica.id ? { ...edge, target: secondDb.id } : edge,
  );
  engine.setTopology(expanded.nodes, expanded.edges);
  engine.dispatchProducerMessage(producer.id, 'U002');
  advance(engine);
  expect(db.data.committedRecordCount).toBe(1);
  expect(secondDb.data.committedRecordCount).toBe(0);
  producer.data.targetTopic = 'other';
  engine.dispatchProducerMessage(producer.id, 'U002');
  advance(engine);
  expect(db.data.committedRecordCount).toBe(1);
  expect(secondDb.data.committedRecordCount).toBe(1);
  const before = engine.consumerGroups
    .get(String(consumer.data.groupId))!
    .getCommittedNextOffset('purchases', 0);
  engine.resetGroupOffsets('other-group', 'earliest');
  advance(engine);
  expect(
    engine.consumerGroups
      .get(String(consumer.data.groupId))!
      .getCommittedNextOffset('purchases', 0),
  ).toBe(before);
  expect(secondDb.data.committedRecordCount).toBe(1);
});

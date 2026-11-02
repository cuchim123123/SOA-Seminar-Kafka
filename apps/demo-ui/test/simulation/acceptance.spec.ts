import { expect, it } from 'vitest';
import { SimEngine } from '../../src/simulation/engine/sim-engine';
import { loadScenario } from '../../src/simulation/store/load-scenario';
import { minimalSystemBlueprint } from '../../src/simulation/blueprints/catalog/minimalSystemBlueprint';
import { transactionalOutboxBlueprint } from '../../src/simulation/blueprints/catalog/transactionalOutboxBlueprint';
import { scaleConsumers } from '../../src/simulation/domain/scale-consumers';
import { SIMULATION_LIMITS } from '../../src/simulation/domain/limits';
import {
  createTopic,
  configureTopic,
} from '../../src/simulation/engine/operations/topic-configuration';
import {
  applyDiagram,
  captureNodeData,
  projectNodeViews,
} from '../../src/simulation/store/diagram-state';

function fixture() {
  const engine = new SimEngine();
  const { nodes, edges } = loadScenario(engine, minimalSystemBlueprint);
  engine.setMechanicsConfig({ autoOffsetReset: 'earliest' });
  return { engine, nodes, edges };
}
function drain(engine: SimEngine) {
  for (let step = 0; step < 20000 && engine.queue.size(); step++) engine.step();
  expect(engine.queue.size()).toBe(0);
}

it('scopes database receipts to the sink when a group is rewired and replayed', () => {
  const { engine, nodes, edges } = fixture();
  engine.dispatchProducerMessage('node-producer-1', 'U001');
  drain(engine);
  const db = nodes.find((node) => node.data.kind === 'database')!;
  const second = {
    ...structuredClone(db),
    id: 'second-db',
    data: { ...structuredClone(db.data), id: 'second-db', committedRecordCount: 0 },
  };
  const rewired = edges.map((edge) =>
    edge.target === db.id ? { ...edge, target: second.id } : edge,
  );
  applyDiagram(engine, [...nodes, second], rewired);
  engine.resetGroupOffsets('loyalty-v1', 'earliest');
  drain(engine);
  expect(engine.nodes.get(db.id)?.data.committedRecordCount).toBe(1);
  expect(engine.nodes.get(second.id)?.data.committedRecordCount).toBe(1);
  applyDiagram(engine, [...nodes, second], edges);
  engine.resetGroupOffsets('loyalty-v1', 'earliest');
  drain(engine);
  expect(engine.nodes.get(db.id)?.data.committedRecordCount).toBe(1);
  expect(engine.processedEvents.size).toBe(2);
});

it('rejects a whole burst at the run limit without discarding existing work', () => {
  const { engine } = fixture();
  engine.dispatchProducerMessage('node-producer-1');
  const pending = engine.queue.toArray().map((event) => event.id),
    previous = engine.lastDispatchedMessage;
  engine.acceptedMessageCount = SIMULATION_LIMITS.messagesPerRun - 1;
  engine.dispatchOrderedBurst('U001', 2);
  expect(engine.queue.toArray().map((event) => event.id)).toEqual(pending);
  expect(engine.lastDispatchedMessage).toBe(previous);
  expect(engine.lastCommandError).toContain('limit');
  expect(engine.acceptedMessageCount).toBe(SIMULATION_LIMITS.messagesPerRun - 1);
  engine.dispatchDuplicateMessage();
  expect(engine.acceptedMessageCount).toBe(SIMULATION_LIMITS.messagesPerRun);
  const queueSize = engine.queue.size();
  expect(engine.dispatchProducerMessage('node-producer-1')).toBeNull();
  expect(engine.queue.size()).toBe(queueSize);
  drain(engine);
  expect(engine.nodes.get('node-db-1')?.data.committedRecordCount).toBe(1);
  engine.reset();
  expect(engine.acceptedMessageCount).toBe(0);
  expect(engine.reserveMessages(Number.NaN)).toBe(false);
});

it('processes a 1000-message burst workload with bounded history and no lost logical records', () => {
  const { engine } = fixture();
  for (let burst = 0; burst < 10; burst++) engine.dispatchOrderedBurst(`customer-${burst}`, 100);
  engine.clock.start();
  let ticks = 0;
  while (engine.queue.size() && ticks++ < 200) engine.tick(10000);
  expect(engine.queue.size()).toBe(0);
  expect(engine.acceptedMessageCount).toBe(1000);
  expect(engine.processedMessageCount).toBe(1000);
  expect(engine.nodes.get('node-db-1')?.data.committedRecordCount).toBe(1000);
  expect(engine.processedEvents.size).toBe(1000);
  expect(engine.eventLog.length).toBe(SIMULATION_LIMITS.retainedEvents);
  expect(engine.totalEventsRecorded).toBeGreaterThan(6000);
  expect(engine.inFlightTokens).toEqual([]);
  const group = engine.consumerGroups.get('loyalty-v1')!;
  for (const partition of engine.kafkaCluster.getAllPartitions())
    expect(group.getCommittedNextOffset('purchases', partition.partitionId)).toBe(
      partition.logEndOffset,
    );
});

it('records future burst dispatches only when virtual time reaches them', () => {
  const { engine } = fixture();
  engine.dispatchOrderedBurst('U001', 5);
  expect(engine.eventLog.every((event) => event.scheduledTime <= engine.clock.virtualTimeMs)).toBe(
    true,
  );
  while (engine.queue.size()) {
    engine.step();
    expect(
      engine.eventLog.every((event) => event.scheduledTime <= engine.clock.virtualTimeMs),
    ).toBe(true);
  }
});

it('does not revive another crashed replica during membership recovery', () => {
  const { engine, nodes, edges } = fixture();
  let id = 0;
  const expanded = scaleConsumers(nodes, edges, 'loyalty-v1', 3, () => `worker-${++id}`);
  engine.setTopology(expanded.nodes, expanded.edges);
  engine.crashConsumer('node-consumer-1');
  engine.crashConsumer('worker-1');
  engine.reviveConsumer('node-consumer-1');
  expect(engine.nodes.get('worker-1')?.data.status).toBe('CRASHED');
  expect(engine.consumerGroups.get('loyalty-v1')?.members.get('worker-1')?.status).toBe('CRASHED');
});

it('seeking one partition reconciles pending deliveries on the other partitions', () => {
  const { engine, nodes, edges } = fixture();
  const expanded = scaleConsumers(nodes, edges, 'loyalty-v1', 2, () => 'second-worker');
  engine.setTopology(expanded.nodes, expanded.edges);
  engine.dispatchProducerMessage('node-producer-1', 'U002');
  engine.dispatchProducerMessage('node-producer-1', 'U001');
  engine.step();
  engine.step();
  expect(engine.seekPartitionOffset('loyalty-v1', 0, 0)).toBe(true);
  drain(engine);
  expect(engine.nodes.get('node-db-1')?.data.committedRecordCount).toBe(2);
});

it('captures the relay acknowledgment policy before its scheduled broker append', () => {
  const engine = new SimEngine();
  loadScenario(engine, transactionalOutboxBlueprint);
  engine.setMechanicsConfig({ producerAcks: '1' });
  engine.kafkaCluster.topics.get('purchases')!.minInsyncReplicas = 3;
  engine.dispatchProducerMessage('node-producer-1');
  expect(engine.step()?.type).toBe('WRITE_OUTBOX');
  expect(engine.step()?.type).toBe('CDC_RELAY_STREAM');
  engine.setMechanicsConfig({ producerAcks: 'all' });
  engine.step();
  expect(
    engine.kafkaCluster.getAllPartitions().reduce((sum, part) => sum + part.records.length, 0),
  ).toBe(1);
});

it('keeps unchanged rendered nodes stable when one sink counter changes', () => {
  const { engine, nodes } = fixture();
  const beforeData = captureNodeData(engine, new Map()),
    before = projectNodeViews(nodes, beforeData, []);
  engine.nodes.get('node-db-1')!.data.committedRecordCount = 1;
  const afterData = captureNodeData(engine, beforeData),
    after = projectNodeViews(nodes, afterData, before);
  expect(after.filter((node, index) => node !== before[index]).map((node) => node.id)).toEqual([
    'node-db-1',
  ]);
});

it('validates topic edits atomically and preserves records when increasing partitions', () => {
  const { engine } = fixture();
  engine.dispatchProducerMessage('node-producer-1', 'U001');
  drain(engine);
  const oldPartition = engine.kafkaCluster.getPartition('purchases', 1)!;
  expect(() =>
    configureTopic(engine, 'purchases', {
      partitionCount: 4,
      replicationFactor: 2,
      minInsyncReplicas: 1,
    }),
  ).toThrow();
  expect(engine.kafkaCluster.getAllPartitions()).toHaveLength(2);
  configureTopic(engine, 'purchases', {
    partitionCount: 4,
    replicationFactor: 1,
    minInsyncReplicas: 1,
  });
  expect(engine.kafkaCluster.getAllPartitions()).toHaveLength(4);
  expect(engine.kafkaCluster.getPartition('purchases', 1)).toBe(oldPartition);
  expect(oldPartition.records).toHaveLength(1);
  expect(engine.clock.status).toBe('PAUSED');
  expect(() =>
    createTopic(engine, 'purchases', {
      partitionCount: 1,
      replicationFactor: 1,
      minInsyncReplicas: 1,
    }),
  ).toThrow('already exists');
  createTopic(engine, 'projection-events', {
    partitionCount: 2,
    replicationFactor: 1,
    minInsyncReplicas: 1,
  });
  expect(engine.kafkaCluster.topics.get('projection-events')?.partitions.size).toBe(2);
});

it('stops new fetches without an online leader and resumes backlog on broker recovery', () => {
  const { engine } = fixture();
  engine.togglePauseConsumer('node-consumer-1');
  engine.dispatchProducerMessage('node-producer-1', 'U001');
  engine.step();
  engine.crashBroker(1);
  engine.togglePauseConsumer('node-consumer-1');
  expect(engine.queue.size()).toBe(0);
  expect(engine.nodes.get('node-db-1')?.data.committedRecordCount).toBe(0);
  engine.reviveBroker(1);
  expect(engine.queue.size()).toBe(1);
  drain(engine);
  expect(engine.nodes.get('node-db-1')?.data.committedRecordCount).toBe(1);
});

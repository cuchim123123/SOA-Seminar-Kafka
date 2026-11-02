import { expect, it } from 'vitest';
import { SimEngine } from '../../src/simulation/engine/sim-engine';
import { SimKafkaClusterModel } from '../../src/simulation/domain/kafka-cluster';
import { createSimMessage } from '../../src/simulation/domain/message';
import { SYSTEM_BLUEPRINTS } from '../../src/simulation/blueprints/systemBlueprints';
import { minimalSystemBlueprint } from '../../src/simulation/blueprints/catalog/minimalSystemBlueprint';
import { loadScenario } from '../../src/simulation/store/load-scenario';
import { producerTopic } from '../../src/simulation/domain/producer-route';
import { filterEvents } from '../../src/simulation/domain/event-filter';
import { validateTopology } from '../../src/simulation/domain/topology-validation';

it('loads every scenario with explicit effective topic configuration and no phantom brokers', () => {
  const engine = new SimEngine();
  for (const blueprint of SYSTEM_BLUEPRINTS) {
    const { nodes, edges } = loadScenario(engine, blueprint);
    const topic = engine.kafkaCluster.topics.get('purchases')!;
    expect(topic.partitions.size, blueprint.id).toBe(blueprint.topicConfiguration.partitionCount);
    expect(topic.replicationFactor).toBe(blueprint.topicConfiguration.replicationFactor);
    expect(engine.kafkaCluster.brokers.size).toBe(
      nodes.filter((node) => node.data.kind === 'kafka-broker').length,
    );
    expect(engine.mechanicsConfig.producerAcks).toBe(blueprint.topicConfiguration.producerAcks);
    expect(engine.clock.status).toBe('PAUSED');
    expect(validateTopology(nodes, edges, new Set(engine.kafkaCluster.topics.keys()))).toEqual([]);
    engine.dispatchProducerMessage(nodes.find((node) => node.data.kind === 'producer')!.id);
    while (engine.queue.size()) engine.step();
    expect(engine.processedMessageCount).toBe(
      nodes.some((node) => node.data.groupId === 'analytics-v1') ? 2 : 1,
    );
  }
});

it('routes a selected producer burst and duplicate through that same original source', () => {
  const engine = new SimEngine();
  const { nodes, edges } = loadScenario(engine, minimalSystemBlueprint);
  const second = structuredClone(nodes.find((node) => node.data.kind === 'producer')!);
  second.id = second.data.id = 'second-producer';
  engine.setTopology(
    [...nodes, second],
    [...edges, { id: 'second-wire', source: second.id, target: edges[0].target }],
  );
  expect(engine.dispatchDuplicateMessage()).toBeNull();
  engine.dispatchOrderedBurst('customer', 2, second.id);
  expect(engine.lastDispatchedProducerId).toBe(second.id);
  expect(engine.eventLog.every((event) => event.sourceNodeId === second.id)).toBe(true);
  const previous = engine.lastDispatchedMessage!;
  const duplicate = engine.dispatchDuplicateMessage()!;
  expect(duplicate.id).toBe(previous.id);
  expect(engine.eventLog[0].sourceNodeId).toBe(second.id);
});

it('uses the relay target topic when previewing an outbox producer route', () => {
  const blueprint = structuredClone(
    SYSTEM_BLUEPRINTS.find((item) => item.id === 'transactional-outbox-cdc')!,
  );
  const producer = blueprint.nodes.find((node) => node.data.kind === 'producer')!;
  blueprint.nodes.find((node) => node.data.kind === 'cdc-relay')!.data.targetTopic = 'custom-topic';
  expect(producerTopic(producer, blueprint.nodes, blueprint.edges)).toBe('custom-topic');
});

it.each(['all', '1', '0'] as const)(
  'applies minISR to acks=%s correctly while still requiring an online leader',
  (acks) => {
    const cluster = new SimKafkaClusterModel();
    cluster.crashBroker(2);
    cluster.crashBroker(3);
    expect(cluster.canWriteToPartition('purchases', 0, acks).allowed).toBe(acks !== 'all');
    const send = () =>
      cluster.appendRecord('purchases', 0, createSimMessage('customer', 10, 0), 0, acks);
    if (acks === 'all') expect(send).toThrow('NOT_ENOUGH_REPLICAS');
    else expect(send().offset).toBe(0);
    cluster.crashBroker(1);
    expect(cluster.canWriteToPartition('purchases', 0, acks).allowed).toBe(false);
  },
);

it('preserves the acknowledgment policy captured when a record was queued', () => {
  const engine = new SimEngine();
  loadScenario(engine, minimalSystemBlueprint);
  engine.kafkaCluster.topics.get('purchases')!.minInsyncReplicas = 2;
  engine.dispatchProducerMessage('node-producer-1', 'U001');
  engine.setMechanicsConfig({ producerAcks: 'all' });
  engine.step();
  expect(
    engine.kafkaCluster
      .getAllPartitions()
      .reduce((sum, partition) => sum + partition.records.length, 0),
  ).toBe(1);
  expect(engine.eventLog.some((event) => event.type === 'PRODUCE_REJECTED')).toBe(false);
});

it('filters retained evidence by actual group and message identity', () => {
  const engine = new SimEngine();
  loadScenario(engine, minimalSystemBlueprint);
  const message = engine.dispatchProducerMessage('node-producer-1', 'find-customer')!;
  while (engine.queue.size()) engine.step();
  expect(filterEvents(engine.eventLog, 'COMMIT_OFFSET', 'loyalty-v1', message.id)).toHaveLength(1);
  expect(filterEvents(engine.eventLog, '', 'other-group', '')).toEqual([]);
  expect(filterEvents(engine.eventLog, '', '', 'find-customer').length).toBeGreaterThan(0);
  expect(engine.totalEventsRecorded).toBeGreaterThan(0);
  engine.reset();
  expect(engine.totalEventsRecorded).toBe(0);
});

it('rejects non-finite effective timing controls before execution', () => {
  const engine = new SimEngine();
  const { nodes, edges } = loadScenario(engine, minimalSystemBlueprint);
  nodes.find((node) => node.data.kind === 'database')!.data.writeDelayMs = Number.NaN;
  expect(validateTopology(nodes, edges).join(' ')).toContain('writeDelayMs must be');
});

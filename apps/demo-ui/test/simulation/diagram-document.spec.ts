import { expect, it } from 'vitest';
import { SimEngine } from '../../src/simulation/engine/sim-engine';
import { minimalSystemBlueprint } from '../../src/simulation/blueprints/catalog/minimalSystemBlueprint';
import { loadScenario } from '../../src/simulation/store/load-scenario';
import {
  createDiagramDocument,
  parseDiagramDocument,
  restoreDiagramDocument,
  readSavedDiagram,
  writeSavedDiagram,
  DIAGRAM_STORAGE_KEY,
  type DiagramStorage,
} from '../../src/simulation/store/diagram-document';
import { scaleConsumers } from '../../src/simulation/domain/scale-consumers';
import { validateTopology } from '../../src/simulation/domain/topology-validation';

function fixture() {
  const engine = new SimEngine();
  const { nodes, edges } = loadScenario(engine, minimalSystemBlueprint);
  return {
    engine,
    nodes,
    edges,
    doc: () =>
      createDiagramDocument(engine, nodes, edges, minimalSystemBlueprint.id, true, {
        x: 15,
        y: -30,
        zoom: 0.7,
      }),
  };
}

it('restores only diagram/settings into a fresh paused runtime after processing and faults', () => {
  const { engine, nodes, edges, doc } = fixture();
  engine.dispatchProducerMessage('node-producer-1', 'customer');
  while (engine.queue.size()) engine.step();
  engine.crashConsumer('node-consumer-1');
  engine.crashBroker(1);
  engine.clock.setSpeed(2);
  engine.setMechanicsConfig({ consumerIdempotency: false, autoOffsetReset: 'earliest' });
  nodes.find((node) => node.data.kind === 'database')!.data.databaseName = 'custom-db';
  const serialized = JSON.stringify(doc());
  for (const field of [
    'committedRecordCount',
    'committedNextOffsets',
    'inFlightTokens',
    'processedEvents',
    'CRASHED',
    'assignedPartitions',
    'eventLog',
    'measured',
  ])
    expect(serialized).not.toContain(field);
  const saved = parseDiagramDocument(serialized);
  const restored = restoreDiagramDocument(saved);
  expect(restored.nodes.map((node) => node.id)).toEqual(nodes.map((node) => node.id));
  expect(restored.edges.map((edge) => edge.id)).toEqual(edges.map((edge) => edge.id));
  expect(saved.viewport).toEqual({ x: 15, y: -30, zoom: 0.7 });
  expect(restored.engine.nodes.get('node-db-1')?.data.databaseName).toBe('custom-db');
  expect(restored.engine.nodes.get('node-db-1')?.data.committedRecordCount).toBe(0);
  expect(restored.engine.nodes.get('node-consumer-1')?.data.status).toBe('POLLING');
  expect(restored.engine.kafkaCluster.brokers.get(1)?.status).toBe('ONLINE');
  expect(restored.engine.kafkaCluster.brokers.get(1)?.isController).toBe(true);
  expect(restored.engine.kafkaCluster.getPartition('purchases', 0)?.isr).toEqual([1]);
  expect(restored.engine.clock.status).toBe('PAUSED');
  expect(restored.engine.clock.virtualTimeMs).toBe(0);
  expect(restored.engine.clock.speedMultiplier).toBe(2);
  expect(restored.engine.mechanicsConfig.consumerIdempotency).toBe(false);
  expect(restored.engine.queue.size()).toBe(0);
  expect(restored.engine.eventLog).toEqual([]);
  expect(restored.engine.processedEvents.size).toBe(0);
  expect(
    restored.engine.kafkaCluster
      .getAllPartitions()
      .every((part) => part.logEndOffset === 0 && part.records.length === 0),
  ).toBe(true);
  expect(restored.engine.consumerGroups.get('loyalty-v1')?.members.size).toBe(1);
});

it('preserves scaled membership and the configured group assignor', () => {
  const { engine, nodes, edges } = fixture();
  let id = 0;
  const expanded = scaleConsumers(nodes, edges, 'loyalty-v1', 3, () => `replica-${++id}`);
  engine.setTopology(expanded.nodes, expanded.edges);
  engine.consumerGroups.get('loyalty-v1')!.setAssignorStrategy('range');
  const doc = createDiagramDocument(
    engine,
    expanded.nodes,
    expanded.edges,
    'minimal-system',
    true,
    null,
  );
  const restored = restoreDiagramDocument(parseDiagramDocument(JSON.stringify(doc)));
  const group = restored.engine.consumerGroups.get('loyalty-v1')!;
  expect(group.members.size).toBe(3);
  expect(group.assignorStrategy).toBe('range');
  for (const member of group.members.values())
    expect(restored.engine.nodes.get(member.id)?.data.assignedPartitions).toEqual(
      member.assignedPartitions,
    );
});

it('retains an incomplete supported draft for repair and preserves its desired replication factor', () => {
  const { doc } = fixture();
  const saved = doc();
  saved.edges = saved.edges.filter((edge) => edge.target !== 'node-db-1');
  saved.topics[0].replicationFactor = 3;
  const restored = restoreDiagramDocument(parseDiagramDocument(JSON.stringify(saved)));
  expect(validateTopology(restored.nodes, restored.edges).join(' ')).toContain('database sink');
  expect(restored.engine.kafkaCluster.topics.get('purchases')?.replicationFactor).toBe(3);
  expect(restored.engine.clock.status).toBe('PAUSED');
});

it.each(['version', 'kind', 'endpoint', 'duplicate', 'coordinates', 'timing', 'oversize'])(
  'rejects malformed or unsupported %s without replacing the original stored document',
  (defect) => {
    const { doc } = fixture();
    const saved = doc() as unknown as Record<string, unknown>;
    const nodes = saved.nodes as {
      id: string;
      data: Record<string, unknown>;
      position: { x: number };
    }[];
    if (defect === 'version') saved.version = 2;
    if (defect === 'kind') nodes[0].data.kind = 'worker-service';
    if (defect === 'endpoint') (saved.edges as { target: string }[])[0].target = 'missing';
    if (defect === 'duplicate') nodes.push(nodes[0]);
    if (defect === 'coordinates') nodes[0].position.x = Infinity;
    if (defect === 'timing') nodes[0].data.clientDelayMs = -1;
    if (defect === 'oversize') saved.nodes = Array(101).fill(nodes[0]);
    const raw = JSON.stringify(saved);
    let writes = 0;
    const storage: DiagramStorage = {
      getItem: () => raw,
      setItem: () => {
        writes++;
      },
    };
    const result = readSavedDiagram(storage);
    expect(result.blocked).toBe(true);
    expect(result.document).toBeNull();
    expect(result.raw).toBe(raw);
    expect(writes).toBe(0);
  },
);

it('strips injected runtime fields instead of reviving old statuses or offsets', () => {
  const { doc } = fixture();
  const saved = doc();
  Object.assign(saved.nodes[0].data, {
    status: 'CRASHED',
    committedRecordCount: 100,
    eventLog: ['forged'],
  });
  const parsed = parseDiagramDocument(JSON.stringify(saved));
  expect(parsed.nodes[0].data).not.toHaveProperty('status');
  expect(parsed.nodes[0].data).not.toHaveProperty('committedRecordCount');
});

it('writes an unchanged serialized document once and handles denied/quota storage without clearing it', () => {
  const { doc } = fixture();
  const serialized = JSON.stringify(doc());
  let raw: string | null = null,
    writes = 0;
  const storage: DiagramStorage = {
    getItem: () => raw,
    setItem: (key, value) => {
      expect(key).toBe(DIAGRAM_STORAGE_KEY);
      raw = value;
      writes++;
    },
  };
  for (let frame = 0; frame < 600; frame++) expect(writeSavedDiagram(storage, serialized)).toBe('');
  expect(writes).toBe(1);
  const denied: DiagramStorage = {
    getItem: () => {
      throw new Error('SecurityError');
    },
    setItem: () => {
      throw new Error('QuotaExceededError');
    },
  };
  expect(readSavedDiagram(denied).blocked).toBe(true);
  expect(writeSavedDiagram(denied, serialized)).toContain('Autosave failed');
  expect(raw).toBe(serialized);
  expect(readSavedDiagram(null).blocked).toBe(true);
});

it('preserves an empty corrupt saved value for recovery', () => {
  const result = readSavedDiagram({
    getItem: () => '',
    setItem: () => {
      throw new Error('Must not write');
    },
  });
  expect(result.blocked).toBe(true);
  expect(result.raw).toBe('');
  expect(result.error).toContain('Saved diagram could not be restored');
});

it('round-trips source/key/burst settings without persisting messages', () => {
  const { engine, nodes, edges } = fixture();
  const doc = createDiagramDocument(engine, nodes, edges, 'minimal-system', true, null, {
    producerId: 'node-producer-1',
    key: 'custom-key',
    burstCount: 7,
  });
  expect(parseDiagramDocument(JSON.stringify(doc)).composer).toEqual({
    producerId: 'node-producer-1',
    key: 'custom-key',
    burstCount: 7,
  });
});

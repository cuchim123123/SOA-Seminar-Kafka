import type { SimCustomNode, SimCustomEdge, SimNodeData } from '../domain/types';
import type { SimEngine } from '../engine/sim-engine';
import { COMPONENT_REGISTRY } from '../registry/component-registry';

// Configuration crosses into the engine; runtime status/counters never travel back as edits.
const CONFIGURATION_KEYS = [
  'id',
  'kind',
  'title',
  'icon',
  'messageKey',
  'messageRate',
  'targetTopic',
  'acks',
  'clientDelayMs',
  'clusterId',
  'brokerCount',
  'brokerIds',
  'activeTopic',
  'topicsList',
  'partitionCount',
  'replicationFactor',
  'minInsyncReplicas',
  'brokerLatencyMs',
  'instanceId',
  'groupId',
  'subscribedTopic',
  'processingDelayMs',
  'concurrencySlots',
  'commitMode',
  'failureRate',
  'engine',
  'databaseName',
  'schemaTables',
  'writeDelayMs',
  'topicName',
  'tableName',
  'retentionCount',
  'relayType',
  'pollIntervalMs',
  'batchSize',
  'sourceTable',
  'brokerId',
  'host',
] as const;

export function configurationData(data: SimNodeData): Record<string, unknown> {
  return Object.fromEntries(
    CONFIGURATION_KEYS.filter((key) => key in data).map((key) => [key, structuredClone(data[key])]),
  );
}

export function applyDiagram(engine: SimEngine, nodes: SimCustomNode[], edges: SimCustomEdge[]) {
  engine.setTopology(
    nodes.map((node) => ({
      ...structuredClone(node),
      data: {
        ...structuredClone(
          engine.nodes.get(node.id)?.data ??
            COMPONENT_REGISTRY.find((entry) => entry.kind === node.data.kind)?.defaultData ??
            {},
        ),
        ...configurationData(node.data),
      } as SimNodeData,
    })),
    structuredClone(edges),
  );
}

export function captureNodeData(
  engine: SimEngine,
  previous: ReadonlyMap<string, SimNodeData>,
): ReadonlyMap<string, SimNodeData> {
  let changed = previous.size !== engine.nodes.size;
  const next = new Map<string, SimNodeData>();
  for (const [id, node] of engine.nodes) {
    const cached = previous.get(id);
    const same = cached && JSON.stringify(cached) === JSON.stringify(node.data);
    next.set(id, same ? cached : structuredClone(node.data));
    changed ||= !same;
  }
  return changed ? next : previous;
}

export function projectNodeViews(
  nodes: SimCustomNode[],
  runtime: ReadonlyMap<string, SimNodeData>,
  previous: SimCustomNode[],
): SimCustomNode[] {
  const cached = new Map(previous.map((node) => [node.id, node]));
  return nodes.map((node) => {
    const data = runtime.get(node.id) ?? node.data;
    const old = cached.get(node.id);
    if (
      old &&
      old.data === data &&
      Object.entries(node).every(
        ([key, value]) =>
          key === 'data' || (old as unknown as Record<string, unknown>)[key] === value,
      )
    )
      return old;
    return { ...node, data };
  });
}

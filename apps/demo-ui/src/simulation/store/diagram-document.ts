import { z } from 'zod';
import { SIMULATION_LIMITS } from '../domain/limits';
import type { SimEngine } from '../engine/sim-engine';
import type { SimCustomNode, SimCustomEdge, SimNodeData } from '../domain/types';
import { configurationData, applyDiagram } from './diagram-state';
import { SimEngine as Engine } from '../engine/sim-engine';

export const DIAGRAM_STORAGE_KEY = 'kafka-lab.simulation.diagram.v1';
const text = z.string().max(1000),
  delay = z.number().finite().min(0).max(60000);
const kinds = [
  'producer',
  'kafka-broker',
  'kafka-cluster',
  'consumer',
  'database',
  'dead-letter-queue',
  'outbox-table',
  'cdc-relay',
] as const;
const nodeType: Record<(typeof kinds)[number], string> = {
  producer: 'producerNode',
  'kafka-broker': 'kafkaBrokerNode',
  'kafka-cluster': 'kafkaClusterNode',
  consumer: 'consumerNode',
  database: 'databaseNode',
  'dead-letter-queue': 'deadLetterQueueNode',
  'outbox-table': 'outboxTableNode',
  'cdc-relay': 'cdcRelayNode',
};
const dataSchema = z.object({
  id: z.string().min(1).max(100),
  kind: z.enum(kinds),
  title: z.string().max(200),
  icon: z.string().max(32),
  messageKey: text.optional(),
  messageRate: z.number().finite().nonnegative().optional(),
  targetTopic: text.optional(),
  acks: z.enum(['0', '1', 'all']).optional(),
  clientDelayMs: delay.optional(),
  clusterId: text.optional(),
  brokerCount: z.number().int().min(1).max(100).optional(),
  brokerIds: z.array(z.number().int().positive()).max(100).optional(),
  activeTopic: text.optional(),
  topicsList: z.array(text).max(24).optional(),
  partitionCount: z.number().int().min(1).max(12).optional(),
  replicationFactor: z.number().int().min(1).max(100).optional(),
  minInsyncReplicas: z.number().int().min(1).max(100).optional(),
  brokerLatencyMs: delay.optional(),
  instanceId: text.optional(),
  groupId: text.optional(),
  subscribedTopic: text.optional(),
  processingDelayMs: delay.optional(),
  concurrencySlots: z.number().int().min(1).max(100).optional(),
  commitMode: z.enum(['auto', 'manual']).optional(),
  failureRate: z.number().min(0).max(100).optional(),
  engine: text.optional(),
  databaseName: text.optional(),
  schemaTables: z.array(text).max(100).optional(),
  writeDelayMs: delay.optional(),
  topicName: text.optional(),
  tableName: text.optional(),
  retentionCount: z.number().int().nonnegative().optional(),
  relayType: z.literal('debezium').optional(),
  pollIntervalMs: delay.optional(),
  batchSize: z.number().int().positive().optional(),
  sourceTable: text.optional(),
  brokerId: z.number().int().positive().optional(),
  host: text.optional(),
});
const nodeSchema = z
  .object({
    id: z.string().min(1).max(100),
    position: z.object({ x: z.number().finite(), y: z.number().finite() }),
    data: dataSchema,
  })
  .refine((node) => node.id === node.data.id, 'Node/data identity mismatch');
const edgeSchema = z.object({
  id: z.string().min(1).max(200),
  source: z.string(),
  target: z.string(),
  sourceHandle: z.string().max(100).nullable().optional(),
  targetHandle: z.string().max(100).nullable().optional(),
  data: z.object({
    portType: z.enum(['publish', 'dispatch', 'handler-action', 'invoke', 'dlq-route', 'cdc-tail']),
  }),
});
export const viewportSchema = z.object({
  x: z.number().finite(),
  y: z.number().finite(),
  zoom: z.number().finite().min(0.2).max(2),
});
export type DiagramViewport = z.infer<typeof viewportSchema>;
const composerSchema = z.object({
  producerId: z.string().nullable(),
  key: z.string().max(100).nullable(),
  burstCount: z.number().int().min(1).max(100),
});
export type MessageComposerSettings = z.infer<typeof composerSchema>;
export const DEFAULT_COMPOSER: MessageComposerSettings = {
  producerId: null,
  key: null,
  burstCount: 5,
};
const documentSchema = z
  .object({
    version: z.literal(1),
    scenarioId: z.string().min(1).max(100),
    customized: z.boolean(),
    composer: composerSchema.default(DEFAULT_COMPOSER),
    nodes: z.array(nodeSchema).max(SIMULATION_LIMITS.components),
    edges: z.array(edgeSchema).max(SIMULATION_LIMITS.wires),
    viewport: viewportSchema.nullable(),
    topics: z
      .array(
        z.object({
          name: z
            .string()
            .min(1)
            .max(249)
            .regex(/^[a-zA-Z0-9._-]+$/),
          partitionCount: z.number().int().min(1).max(12),
          replicationFactor: z.number().int().min(1).max(100),
          minInsyncReplicas: z.number().int().min(1).max(100),
        }),
      )
      .max(24),
    mechanics: z.object({
      commitMode: z.enum(['manual', 'auto']),
      autoOffsetReset: z.enum(['earliest', 'latest']),
      consumerIdempotency: z.boolean(),
      keyOrderingMode: z.enum(['MURMUR2_KEYED', 'ROUND_ROBIN']),
      partitioningStrategy: z.enum(['murmur2-key', 'round-robin']).optional(),
      producerAcks: z.enum(['all', '1', '0']),
    }),
    speed: z.number().finite().min(0.1).max(10),
    assignors: z.record(z.enum(['range', 'round-robin'])),
  })
  .superRefine((doc, context) => {
    const ids = new Set(doc.nodes.map((node) => node.id));
    if (
      ids.size !== doc.nodes.length ||
      new Set(doc.edges.map((edge) => edge.id)).size !== doc.edges.length ||
      new Set(doc.topics.map((topic) => topic.name)).size !== doc.topics.length
    )
      context.addIssue({ code: 'custom', message: 'Duplicate document identities' });
    if (doc.edges.some((edge) => !ids.has(edge.source) || !ids.has(edge.target)))
      context.addIssue({ code: 'custom', message: 'Missing wire endpoint' });
    if (Object.keys(doc.assignors).length > 100)
      context.addIssue({ code: 'custom', message: 'Too many group settings' });
  });
export type DiagramDocument = z.infer<typeof documentSchema>;

export function createDiagramDocument(
  engine: SimEngine,
  nodes: SimCustomNode[],
  edges: SimCustomEdge[],
  scenarioId: string,
  customized: boolean,
  viewport: DiagramViewport | null,
  composer: MessageComposerSettings = DEFAULT_COMPOSER,
): DiagramDocument {
  return documentSchema.parse({
    version: 1,
    scenarioId,
    customized,
    viewport,
    composer,
    nodes: nodes.map((node) => ({
      id: node.id,
      position: node.position,
      data: configurationData(node.data),
    })),
    edges: edges.map((edge) => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      sourceHandle: edge.sourceHandle,
      targetHandle: edge.targetHandle,
      data: { portType: edge.data?.portType ?? 'handler-action' },
    })),
    topics: Array.from(engine.kafkaCluster.topics.values()).map((topic) => ({
      name: topic.name,
      partitionCount: topic.partitions.size,
      replicationFactor: topic.replicationFactor,
      minInsyncReplicas: topic.minInsyncReplicas,
    })),
    mechanics: engine.mechanicsConfig,
    speed: engine.clock.speedMultiplier,
    assignors: Object.fromEntries(
      Array.from(engine.consumerGroups.values()).map((group) => [
        group.groupId,
        group.assignorStrategy,
      ]),
    ),
  });
}

export function parseDiagramDocument(raw: string): DiagramDocument {
  if (raw.length > 1_000_000) throw new Error('Saved diagram is too large.');
  return documentSchema.parse(JSON.parse(raw));
}

export function restoreDiagramDocument(doc: DiagramDocument) {
  const engine = new Engine();
  const nodes: SimCustomNode[] = doc.nodes.map((node) => ({
    ...node,
    type: nodeType[node.data.kind],
    data: node.data as SimNodeData,
  }));
  const edges: SimCustomEdge[] = doc.edges.map((edge) => ({ ...edge, type: 'simFlowEdge' }));
  engine.setMechanicsConfig(doc.mechanics);
  engine.kafkaCluster.topics.clear();
  // Reconcile broker membership before recreating the topic configuration.
  applyDiagram(engine, nodes, edges);
  engine.kafkaCluster.topics.clear();
  for (const saved of doc.topics) {
    const topic = engine.kafkaCluster.createTopic(
      saved.name,
      saved.partitionCount,
      saved.replicationFactor,
      saved.minInsyncReplicas,
    );
    // Preserve an invalid draft's desired RF; execution validation requires it to be repaired.
    topic.replicationFactor = saved.replicationFactor;
  }
  engine.reset();
  for (const topic of doc.topics)
    engine.kafkaCluster.topics.get(topic.name)!.replicationFactor = topic.replicationFactor;
  for (const name of engine.kafkaCluster.topics.keys())
    if (!doc.topics.some((topic) => topic.name === name)) engine.kafkaCluster.topics.delete(name);
  for (const [groupId, strategy] of Object.entries(doc.assignors)) {
    const group = engine.consumerGroups.get(groupId);
    if (!group) continue;
    group.setAssignorStrategy(strategy);
    group.rebalance(engine.kafkaCluster.topics.get(group.topicName)?.partitions.size ?? 0);
    for (const member of group.members.values())
      engine.nodes.get(member.id)!.data.assignedPartitions = [...member.assignedPartitions];
  }
  engine.clock.setSpeed(doc.speed);
  engine.clock.pause();
  engine.notifyStateUpdated();
  return { engine, nodes, edges };
}

export type DiagramStorage = Pick<Storage, 'getItem' | 'setItem'>;
export function readSavedDiagram(storage: DiagramStorage | null) {
  let raw: string | null = null;
  try {
    if (!storage) throw new Error('Browser storage is unavailable.');
    raw = storage.getItem(DIAGRAM_STORAGE_KEY);
    return {
      document: raw !== null ? parseDiagramDocument(raw) : null,
      raw,
      error: '',
      blocked: false,
    };
  } catch {
    return {
      document: null,
      raw,
      error:
        raw !== null
          ? 'Saved diagram could not be restored. Its original document is preserved; autosave is paused.'
          : 'Browser storage is unavailable. Changes are retained only in this session.',
      blocked: true,
    };
  }
}

export function writeSavedDiagram(storage: DiagramStorage | null, serialized: string): string {
  try {
    if (!storage) throw new Error('Unavailable');
    if (storage.getItem(DIAGRAM_STORAGE_KEY) !== serialized)
      storage.setItem(DIAGRAM_STORAGE_KEY, serialized);
    return '';
  } catch {
    return 'Autosave failed. Changes are retained in this session; browser storage may be unavailable or full.';
  }
}

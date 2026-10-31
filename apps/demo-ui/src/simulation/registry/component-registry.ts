import type { ComponentMetadata, ComponentKind } from '../domain/types';

export const COMPONENT_REGISTRY: ComponentMetadata[] = [
  {
    kind: 'producer',
    title: 'Purchase Producer',
    category: 'messaging',
    icon: '🚀',
    description: 'Generates synthetic retail purchase events with Murmur2 customer keys.',
    allowedSourcePorts: ['publish'],
    allowedTargetPorts: [],
    defaultData: {
      title: 'Purchase Producer',
      icon: '🚀',
      status: 'IDLE',
      messageKey: 'U001',
      messageRate: 1,
      targetTopic: 'purchases',
      acks: '1',
      clientDelayMs: 50,
    },
  },
  {
    kind: 'kafka-cluster',
    title: 'Kafka Cluster',
    category: 'messaging',
    icon: '📦',
    description:
      'KRaft distributed log with multi-broker cluster, topics, and append-only partitions.',
    allowedSourcePorts: ['dispatch'],
    allowedTargetPorts: ['publish'],
    defaultData: {
      title: 'Kafka Cluster',
      icon: '📦',
      status: 'ACTIVE',
      clusterId: 'kraft-sim',
      brokerCount: 3,
      brokerIds: [1, 2, 3],
      activeTopic: 'purchases',
      partitionCount: 4,
      replicationFactor: 3,
      minInsyncReplicas: 2,
      brokerLatencyMs: 25,
    },
  },
  {
    kind: 'consumer',
    title: 'Consumer Instance',
    category: 'application',
    icon: '👥',
    description: 'Consumer Group member receiving assigned partitions with offset commits.',
    allowedSourcePorts: ['handler-action', 'dlq-route'],
    allowedTargetPorts: ['dispatch'],
    defaultData: {
      title: 'Consumer Instance',
      icon: '👥',
      status: 'POLLING',
      instanceId: 'consumer-1',
      groupId: 'loyalty-v1',
      subscribedTopic: 'purchases',
      assignedPartitions: [0, 1, 2, 3],
      processingDelayMs: 120,
      concurrencySlots: 1,
      commitMode: 'auto',
      failureRate: 0,
    },
  },
  {
    kind: 'database',
    title: 'Database / Storage Sink',
    category: 'storage',
    icon: '🗄️',
    description:
      'Storage engine (PostgreSQL, MongoDB, Redis, Elasticsearch, ClickHouse) storing committed projections.',
    allowedSourcePorts: [],
    allowedTargetPorts: ['handler-action'],
    defaultData: {
      title: 'PostgreSQL DB',
      icon: '🗄️',
      status: 'IDLE',
      engine: 'PostgreSQL 16',
      databaseName: 'loyalty_db',
      schemaTables: ['loyalty_accounts', 'processed_events'],
      writeDelayMs: 20,
      committedRecordCount: 0,
    },
  },
  {
    kind: 'dead-letter-queue',
    title: 'Dead-Letter Queue (DLQ)',
    category: 'messaging',
    icon: '☠️',
    description: 'Isolation queue receiving poison-pill messages and deserialization failures.',
    allowedSourcePorts: [],
    allowedTargetPorts: ['dlq-route', 'handler-action'],
    defaultData: {
      title: 'Dead-Letter Queue',
      icon: '☠️',
      status: 'IDLE',
      topicName: 'purchases.DLQ',
      deadLetterCount: 0,
    },
  },
  {
    kind: 'outbox-table',
    title: 'Transactional Outbox Table',
    category: 'storage',
    icon: '📦',
    description:
      'Atomic transactional buffer table (PostgreSQL/MySQL) capturing domain events before broker dispatch.',
    allowedSourcePorts: ['cdc-tail'],
    allowedTargetPorts: ['handler-action', 'publish'],
    defaultData: {
      title: 'Outbox Table',
      icon: '📦',
      status: 'IDLE',
      tableName: 'outbox_events',
      pendingRecordsCount: 0,
      publishedRecordsCount: 0,
      retentionCount: 50,
    },
  },
  {
    kind: 'cdc-relay',
    title: 'CDC / Outbox Relay',
    category: 'application',
    icon: '🔄',
    description:
      'Debezium CDC connector or polling worker streaming committed outbox rows to Kafka topics.',
    allowedSourcePorts: ['publish'],
    allowedTargetPorts: ['cdc-tail'],
    defaultData: {
      title: 'Debezium CDC Relay',
      icon: '🔄',
      status: 'POLLING',
      relayType: 'debezium',
      pollIntervalMs: 150,
      batchSize: 5,
      sourceTable: 'outbox_events',
      targetTopic: 'purchases',
      streamedCount: 0,
    },
  },
];

/**
 * Validates connection semantics between two component kinds.
 * Enforces:
 *   producer -> kafka-cluster (publish)
 *   producer -> outbox-table (publish)
 *   outbox-table -> cdc-relay (cdc-tail)
 *   cdc-relay -> kafka-cluster (publish)
 *   kafka-cluster -> consumer (dispatch)
 *   consumer -> database (handler-action)
 *   consumer -> dead-letter-queue (dlq-route)
 */
export function isValidConnection(sourceKind: ComponentKind, targetKind: ComponentKind): boolean {
  if (sourceKind === 'producer' && targetKind === 'kafka-cluster') return true;
  if (sourceKind === 'producer' && targetKind === 'outbox-table') return true;
  if (sourceKind === 'outbox-table' && targetKind === 'cdc-relay') return true;
  if (sourceKind === 'cdc-relay' && targetKind === 'kafka-cluster') return true;
  if (sourceKind === 'kafka-cluster' && targetKind === 'consumer') return true;
  if (sourceKind === 'consumer' && targetKind === 'database') return true;
  if (sourceKind === 'consumer' && targetKind === 'dead-letter-queue') return true;
  if (sourceKind === 'consumer' && targetKind === 'worker-service') return true;
  if (sourceKind === 'worker-service' && targetKind === 'database') return true;
  return false;
}

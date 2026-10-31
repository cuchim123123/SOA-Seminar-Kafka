import type { Node, Edge } from '@xyflow/react';

export type ComponentCategory = 'messaging' | 'application' | 'storage' | 'utility';

export type ComponentKind =
  | 'producer'
  | 'kafka-cluster'
  | 'consumer'
  | 'worker-service'
  | 'database'
  | 'dead-letter-queue'
  | 'outbox-table'
  | 'cdc-relay'
  | 'group-container';

export type PortType =
  'publish' | 'dispatch' | 'handler-action' | 'invoke' | 'dlq-route' | 'cdc-tail';

export interface ComponentMetadata {
  kind: ComponentKind;
  title: string;
  category: ComponentCategory;
  icon: string;
  description: string;
  defaultData: Record<string, unknown>;
  allowedSourcePorts: PortType[];
  allowedTargetPorts: PortType[];
}

export interface SimBaseNodeData extends Record<string, unknown> {
  id: string;
  kind: ComponentKind;
  title: string;
  icon: string;
  status: 'IDLE' | 'ACTIVE' | 'PROCESSING' | 'POLLING' | 'PAUSED' | 'ERROR' | 'OFFLINE' | 'CRASHED';
  statusText?: string;
}

export interface SimProducerData extends SimBaseNodeData {
  kind: 'producer';
  messageKey: string;
  messageRate: number; // msg/sec
  targetTopic: string;
  acks: '0' | '1' | 'all';
  clientDelayMs: number;
}

export interface SimKafkaClusterData extends SimBaseNodeData {
  kind: 'kafka-cluster';
  clusterId: string;
  brokerCount: number;
  brokerIds: number[];
  activeTopic: string;
  topicsList?: string[];
  partitionCount: number;
  replicationFactor: number;
  minInsyncReplicas: number;
  brokerLatencyMs: number;
  partitionLags?: Record<number, number>;
  partitionLogEndOffsets?: Record<number, number>;
}

export interface SimConsumerData extends SimBaseNodeData {
  kind: 'consumer';
  instanceId: string;
  groupId: string;
  subscribedTopic: string;
  assignedPartitions: number[];
  processingDelayMs: number;
  concurrencySlots: number;
  commitMode: 'auto' | 'manual';
  failureRate: number; // 0 to 100%
  isPaused?: boolean;
  pausedPartitions?: number[];
  currentProcessingPartition?: number;
  currentProcessingOffset?: number;
}

export interface SimDatabaseData extends SimBaseNodeData {
  kind: 'database';
  engine: string;
  databaseName: string;
  schemaTables: string[];
  writeDelayMs: number;
  committedRecordCount: number;
}

export interface SimDlqData extends SimBaseNodeData {
  kind: 'dead-letter-queue';
  topicName: string;
  deadLetterCount: number;
}

export interface SimOutboxTableData extends SimBaseNodeData {
  kind: 'outbox-table';
  tableName: string;
  pendingRecordsCount: number;
  publishedRecordsCount: number;
  retentionCount: number;
}

export interface SimCdcRelayData extends SimBaseNodeData {
  kind: 'cdc-relay';
  relayType: 'debezium' | 'polling-publisher';
  pollIntervalMs: number;
  batchSize: number;
  sourceTable: string;
  targetTopic: string;
  streamedCount: number;
}

export interface SimConsumerGroupContainerData extends SimBaseNodeData {
  kind: 'group-container';
  groupId: string;
  memberCount: number;
  assignedPartitions: number[];
  assignorStrategy: string;
  isPaused?: boolean;
}

export type SimProducerAcks = 'all' | '1' | '0';
export type SimCommitMode = 'manual' | 'auto';
export type SimOffsetResetPolicy = 'earliest' | 'latest';

export interface SimKafkaMechanicsConfig {
  commitMode: SimCommitMode; // default: 'manual' (commit N+1 after handler succeeds)
  autoOffsetReset: SimOffsetResetPolicy; // applies only when a partition has no committed offset
  consumerIdempotency: boolean; // default: true (safe deduplication)
  keyOrderingMode: 'MURMUR2_KEYED' | 'ROUND_ROBIN'; // default: 'MURMUR2_KEYED'
  partitioningStrategy?: 'murmur2-key' | 'round-robin';
  dlqRoutingEnabled: boolean; // default: true
  producerAcks: SimProducerAcks; // default: 'all'
}

export type KafkaMechanicsConfig = SimKafkaMechanicsConfig;

export type SimNodeData =
  | SimProducerData
  | SimKafkaClusterData
  | SimConsumerData
  | SimConsumerGroupContainerData
  | SimDatabaseData
  | SimDlqData
  | SimOutboxTableData
  | SimCdcRelayData
  | SimBaseNodeData;

export type SimCustomNode = Node<SimNodeData, string>;

export interface SimInFlightToken {
  id: string;
  messageId: string;
  messageKey: string;
  edgeId: string;
  sourceNodeId: string;
  targetNodeId: string;
  startTime: number;
  arriveTime: number;
  color?: string;
}

export interface SimEdgeData extends Record<string, unknown> {
  portType: PortType;
  inFlightTokens?: SimInFlightToken[];
}

export type SimCustomEdge = Edge<SimEdgeData>;

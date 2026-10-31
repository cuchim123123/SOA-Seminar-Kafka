export interface PartitionPosition {
  readonly topic: string;
  readonly partition: number;
  readonly leader: number;
  readonly replicas: number[];
  readonly isr: number[];
  readonly logStartOffset: string;
  readonly logEndOffset: string;
}

export interface TopicInspectionResult {
  readonly topic: string;
  readonly partitionCount: number;
  readonly partitions: PartitionPosition[];
}

export interface PartitionLag {
  readonly topic: string;
  readonly partition: number;
  readonly logStartOffset: string;
  readonly logEndOffset: string;
  readonly committedOffset: string | null;
  readonly lag: string | 'UNCOMMITTED';
  readonly isUncommitted: boolean;
}

export interface ConsumerGroupLagResult {
  readonly groupId: string;
  readonly topic: string;
  readonly totalLag: string | 'UNCOMMITTED';
  readonly partitions: PartitionLag[];
}

export interface BrokerMessageRecord {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly key: string | null;
  readonly timestamp: string;
  readonly payload: unknown;
}

export interface ConsumerGroupMemberDescription {
  readonly memberId: string;
  readonly clientId: string;
  readonly clientHost: string;
  readonly assignedPartitions: number[];
}

export interface ConsumerGroupDescriptionResult {
  readonly groupId: string;
  readonly state: string;
  readonly protocol: string;
  readonly protocolType: string;
  readonly members: ConsumerGroupMemberDescription[];
}

export const STREAM_INSPECTOR_PORT = Symbol('STREAM_INSPECTOR_PORT');

export interface StreamInspectorPort {
  inspectTopic(topic: string): Promise<TopicInspectionResult>;
  inspectConsumerGroupLag(groupId: string, topic?: string): Promise<ConsumerGroupLagResult>;
  describeConsumerGroup(groupId: string, topic?: string): Promise<ConsumerGroupDescriptionResult>;
  scaleTopicPartitions(topic: string, newPartitionCount: number): Promise<TopicInspectionResult>;
  peekPartitionMessages(
    topic: string,
    partition: number,
    limit?: number,
  ): Promise<BrokerMessageRecord[]>;
  publishRawMessage(
    topic: string,
    key: string,
    payload: string,
  ): Promise<{ topic: string; partition: number; offset: string }>;
}

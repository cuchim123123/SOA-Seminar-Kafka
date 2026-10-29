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

export const KAFKA_INSPECTOR_PORT = Symbol('KAFKA_INSPECTOR_PORT');

export interface KafkaInspectorPort {
  inspectTopic(topic: string): Promise<TopicInspectionResult>;
  inspectConsumerGroupLag(groupId: string, topic?: string): Promise<ConsumerGroupLagResult>;
  scaleTopicPartitions(topic: string, newPartitionCount: number): Promise<TopicInspectionResult>;
  peekPartitionMessages(
    topic: string,
    partition: number,
    limit?: number,
  ): Promise<BrokerMessageRecord[]>;
}

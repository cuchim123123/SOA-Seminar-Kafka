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

export const KAFKA_INSPECTOR_PORT = Symbol('KAFKA_INSPECTOR_PORT');

export interface KafkaInspectorPort {
  inspectTopic(topic: string): Promise<TopicInspectionResult>;
}

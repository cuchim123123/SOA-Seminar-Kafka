export interface ConsumerControlStatus {
  readonly groupId: string;
  readonly topic: string;
  readonly isPaused: boolean;
  readonly pausedPartitions: number[];
}

export const KAFKA_CONSUMER_CONTROL_PORT = Symbol('KAFKA_CONSUMER_CONTROL_PORT');

export interface KafkaConsumerControlPort {
  pause(): Promise<void>;
  resume(): Promise<void>;
  getStatus(): Promise<ConsumerControlStatus>;
}

export interface ConsumerControlStatus {
  readonly groupId: string;
  readonly topic: string;
  readonly isPaused: boolean;
  readonly pausedPartitions: number[];
}

export const CONSUMER_CONTROL_PORT = Symbol('CONSUMER_CONTROL_PORT');

export interface ConsumerControlPort {
  pause(): Promise<void>;
  resume(): Promise<void>;
  getStatus(): Promise<ConsumerControlStatus>;
}

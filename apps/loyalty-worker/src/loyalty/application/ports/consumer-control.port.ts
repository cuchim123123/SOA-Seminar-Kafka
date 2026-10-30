export interface LoyaltyConsumerControlStatus {
  readonly groupId: string;
  readonly topic: string;
  readonly isPaused: boolean;
  readonly pausedPartitions: number[];
  readonly processingDelayMs: number;
}

export const LOYALTY_CONSUMER_CONTROL_PORT = Symbol('LOYALTY_CONSUMER_CONTROL_PORT');

export interface LoyaltyConsumerControlPort {
  pause(): Promise<void>;
  resume(): Promise<void>;
  getStatus(): Promise<LoyaltyConsumerControlStatus>;
  setProcessingDelay(delayMs: number): void;
  getProcessingDelay(): number;
}

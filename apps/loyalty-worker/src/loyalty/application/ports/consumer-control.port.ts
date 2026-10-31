export interface RebalanceEventLog {
  readonly id: string;
  readonly timestamp: string;
  readonly eventType:
    'REBALANCING' | 'GROUP_JOIN' | 'MEMBER_JOINED' | 'MEMBER_LEFT' | 'MEMBER_KILLED';
  readonly memberId: string;
  readonly clientId: string;
  readonly groupGenerationId?: number;
  readonly assignedPartitions?: number[];
  readonly durationMs?: number;
  readonly details: string;
}

export interface LoyaltyConsumerInstanceInfo {
  readonly instanceId: string;
  readonly clientId: string;
  readonly isPrimary: boolean;
  readonly status: 'RUNNING' | 'PAUSED' | 'KILLED' | 'STOPPED';
  readonly assignedPartitions: number[];
}

export interface LoyaltyConsumerControlStatus {
  readonly groupId: string;
  readonly topic: string;
  readonly isPaused: boolean;
  readonly pausedPartitions: number[];
  readonly processingDelayMs: number;
  readonly instanceCount: number;
  readonly instances: LoyaltyConsumerInstanceInfo[];
  readonly totalRebalanceCount: number;
}

export const LOYALTY_CONSUMER_CONTROL_PORT = Symbol('LOYALTY_CONSUMER_CONTROL_PORT');

export interface LoyaltyConsumerControlPort {
  pause(): Promise<void>;
  resume(): Promise<void>;
  pausePartition(partition: number): Promise<void>;
  resumePartition(partition: number): Promise<void>;
  getStatus(): Promise<LoyaltyConsumerControlStatus>;
  setProcessingDelay(delayMs: number): void;
  getProcessingDelay(): number;
  scaleInstances(
    targetCount: number,
  ): Promise<{ targetCount: number; instances: LoyaltyConsumerInstanceInfo[] }>;
  killReplicaInstance(
    instanceId?: string,
  ): Promise<{ killedInstanceId: string; remainingCount: number }>;
  getRebalanceHistory(): RebalanceEventLog[];
}

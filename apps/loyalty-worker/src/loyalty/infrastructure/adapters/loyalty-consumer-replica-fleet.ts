import { Consumer, Kafka, KafkaMessage } from 'kafkajs';
import { StructuredLogger } from '@lab/observability';
import { AppConfigService } from '../../../config/app-config.service';
import {
  LoyaltyConsumerInstanceInfo,
  RebalanceEventLog,
} from '../../application/ports/consumer-control.port';

export interface ReplicaInstance {
  readonly id: string;
  readonly clientId: string;
  readonly consumer: Consumer;
  status: 'RUNNING' | 'PAUSED' | 'KILLED' | 'STOPPED';
  assignedPartitions: number[];
}

const MAX_REBALANCE_HISTORY = 50;

export class LoyaltyConsumerReplicaFleet {
  private readonly replicas = new Map<string, ReplicaInstance>();
  private readonly rebalanceHistory: RebalanceEventLog[] = [];
  private totalRebalanceCount = 0;

  constructor(
    private readonly config: AppConfigService,
    private readonly kafka: Kafka,
    private readonly topic: string,
    private readonly logger: StructuredLogger,
  ) {}

  recordRebalanceEvent(event: RebalanceEventLog): void {
    this.rebalanceHistory.unshift(event);
    if (this.rebalanceHistory.length > MAX_REBALANCE_HISTORY) {
      this.rebalanceHistory.pop();
    }
  }

  getRebalanceHistory(): RebalanceEventLog[] {
    return [...this.rebalanceHistory];
  }

  getTotalRebalanceCount(): number {
    return this.totalRebalanceCount;
  }

  getReplicaInstances(): LoyaltyConsumerInstanceInfo[] {
    return Array.from(this.replicas.values()).map((replica) => ({
      instanceId: replica.id,
      clientId: replica.clientId,
      isPrimary: false,
      status: replica.status,
      assignedPartitions: replica.assignedPartitions,
    }));
  }

  setupConsumerListeners(
    consumer: Consumer,
    instanceId: string,
    clientId: string,
    onAssignment: (partitions: number[]) => void,
  ): void {
    consumer.on(consumer.events.REBALANCING, (e) => {
      this.totalRebalanceCount++;
      this.recordRebalanceEvent({
        id: `reb_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
        timestamp: new Date().toISOString(),
        eventType: 'REBALANCING',
        memberId: (e.payload as { memberId?: string })?.memberId ?? instanceId,
        clientId,
        details: `Rebalance initiated for consumer group ${this.config.kafkaGroupId} on member ${clientId}`,
      });
      this.logger.warn('[KafkaRebalance] Rebalancing triggered', { instanceId, clientId });
    });

    consumer.on(consumer.events.GROUP_JOIN, (e) => {
      const payload = e.payload as {
        memberAssignment?: Record<string, number[]>;
        memberId: string;
        isLeader: boolean;
        duration: number;
        groupGenerationId?: number;
      };
      const assignment = payload.memberAssignment?.[this.topic] ?? [];
      const sortedPartitions = assignment.slice().sort((a: number, b: number) => a - b);
      onAssignment(sortedPartitions);

      this.recordRebalanceEvent({
        id: `join_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
        timestamp: new Date().toISOString(),
        eventType: 'GROUP_JOIN',
        memberId: payload.memberId,
        clientId,
        groupGenerationId: payload.groupGenerationId,
        assignedPartitions: sortedPartitions,
        durationMs: payload.duration,
        details: `Rebalance completed in ${payload.duration}ms. Leader: ${payload.isLeader}. Assigned ${sortedPartitions.length} partitions: [${sortedPartitions.join(', ')}]`,
      });
      this.logger.info('[KafkaRebalance] Group join complete', { instanceId, clientId });
    });

    consumer.on(consumer.events.CRASH, (e) => {
      const payload = e.payload as { error: Error };
      this.recordRebalanceEvent({
        id: `crash_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
        timestamp: new Date().toISOString(),
        eventType: 'MEMBER_KILLED',
        memberId: instanceId,
        clientId,
        details: `Consumer crash detected on ${clientId}: ${payload.error?.message ?? 'Unknown error'}`,
      });
    });
  }

  async scaleInstances(
    targetCount: number,
    isPaused: boolean,
    onMessage: (
      consumer: Consumer,
      topic: string,
      partition: number,
      message: KafkaMessage,
    ) => Promise<void>,
  ): Promise<void> {
    const clamped = Math.max(1, Math.min(4, targetCount));
    const currentReplicaCount = this.replicas.size;
    const neededReplicas = clamped - 1;

    if (neededReplicas > currentReplicaCount) {
      for (let i = currentReplicaCount + 1; i <= neededReplicas; i++) {
        const replicaId = `loyalty-worker-replica-${i}`;
        const clientId = `${this.config.kafkaClientId}-replica-${i}`;
        const replicaConsumer = this.kafka.consumer({ groupId: this.config.kafkaGroupId });

        this.setupConsumerListeners(replicaConsumer, replicaId, clientId, (partitions) => {
          const replica = this.replicas.get(replicaId);
          if (replica) replica.assignedPartitions = partitions;
        });

        await replicaConsumer.connect();
        await replicaConsumer.subscribe({ topic: this.topic, fromBeginning: false });

        this.replicas.set(replicaId, {
          id: replicaId,
          clientId,
          consumer: replicaConsumer,
          status: isPaused ? 'PAUSED' : 'RUNNING',
          assignedPartitions: [],
        });

        await replicaConsumer.run({
          autoCommit: false,
          eachMessage: async ({ topic, partition, message }) => {
            await onMessage(replicaConsumer, topic, partition, message);
          },
        });

        this.recordRebalanceEvent({
          id: `scale_up_${Date.now()}_${i}`,
          timestamp: new Date().toISOString(),
          eventType: 'MEMBER_JOINED',
          memberId: replicaId,
          clientId,
          details: `Replica instance ${clientId} created and connected to consumer group ${this.config.kafkaGroupId}. Kafka initiating rebalance.`,
        });
        this.logger.info(`Scaled up: ${clientId} joined group ${this.config.kafkaGroupId}`);
      }
    } else if (neededReplicas < currentReplicaCount) {
      const keys = Array.from(this.replicas.keys());
      const toRemove = keys.slice(neededReplicas);

      for (const replicaId of toRemove) {
        const replica = this.replicas.get(replicaId);
        if (replica) {
          replica.status = 'STOPPED';
          await replica.consumer.disconnect().catch(() => {});
          this.replicas.delete(replicaId);

          this.recordRebalanceEvent({
            id: `scale_down_${Date.now()}_${replicaId}`,
            timestamp: new Date().toISOString(),
            eventType: 'MEMBER_LEFT',
            memberId: replicaId,
            clientId: replica.clientId,
            details: `Replica instance ${replica.clientId} gracefully disconnected. Kafka rebalancing partitions back to remaining members.`,
          });
          this.logger.info(`Scaled down: ${replica.clientId} disconnected from group`);
        }
      }
    }
  }

  async killReplicaInstance(
    instanceId?: string,
  ): Promise<{ killedInstanceId: string; remainingCount: number }> {
    const keys = Array.from(this.replicas.keys());
    if (keys.length === 0) {
      throw new Error(
        'No replica consumer instances are currently running to kill. Primary instance cannot be killed via this endpoint.',
      );
    }

    const targetId = instanceId ?? keys[keys.length - 1];
    const replica = this.replicas.get(targetId);
    if (!replica) {
      throw new Error(`Replica consumer instance '${targetId}' not found.`);
    }

    replica.status = 'KILLED';
    await replica.consumer.stop().catch(() => {});
    await replica.consumer.disconnect().catch(() => {});
    this.replicas.delete(targetId);

    this.recordRebalanceEvent({
      id: `kill_${Date.now()}_${targetId}`,
      timestamp: new Date().toISOString(),
      eventType: 'MEMBER_KILLED',
      memberId: targetId,
      clientId: replica.clientId,
      details: `[Fault Injection / Chaos] Replica instance ${replica.clientId} abruptly killed! Kafka broker detecting leave/heartbeat timeout and reassigning partitions.`,
    });

    return {
      killedInstanceId: targetId,
      remainingCount: 1 + this.replicas.size,
    };
  }

  pauseReplicas(partitions?: number[]): void {
    for (const replica of this.replicas.values()) {
      if (partitions) {
        replica.consumer.pause([{ topic: this.topic, partitions }]);
      } else {
        replica.consumer.pause([{ topic: this.topic }]);
        replica.status = 'PAUSED';
      }
    }
  }

  resumeReplicas(partitions?: number[]): void {
    for (const replica of this.replicas.values()) {
      if (partitions) {
        replica.consumer.resume([{ topic: this.topic, partitions }]);
      } else {
        replica.consumer.resume([{ topic: this.topic }]);
        replica.status = 'RUNNING';
      }
    }
  }

  async disconnectAll(): Promise<void> {
    for (const replica of this.replicas.values()) {
      await replica.consumer.disconnect().catch(() => {});
    }
  }
}

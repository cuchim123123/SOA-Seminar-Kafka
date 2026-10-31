import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { deserializeEnvelope } from '@lab/contracts';
import { processKafkaRecord, StructuredLogger } from '@lab/observability';
import { Consumer, Kafka, KafkaMessage } from 'kafkajs';
import { AppConfigService } from '../../../config/app-config.service';
import { ApplyPurchaseEventCommand } from '../../application/commands/apply-purchase-event/apply-purchase-event.command';
import { ApplyPurchaseEventHandler } from '../../application/commands/apply-purchase-event/apply-purchase-event.handler';
import { ApplyPurchaseInvalidationCommand } from '../../application/commands/apply-purchase-invalidation/apply-purchase-invalidation.command';
import { ApplyPurchaseInvalidationHandler } from '../../application/commands/apply-purchase-invalidation/apply-purchase-invalidation.handler';
import {
  LoyaltyConsumerControlPort,
  LoyaltyConsumerControlStatus,
  LoyaltyConsumerInstanceInfo,
  RebalanceEventLog,
} from '../../application/ports/consumer-control.port';

interface ReplicaInstance {
  readonly id: string;
  readonly clientId: string;
  readonly consumer: Consumer;
  status: 'RUNNING' | 'PAUSED' | 'KILLED' | 'STOPPED';
  assignedPartitions: number[];
}

@Injectable()
export class KafkaLoyaltyConsumerAdapter
  implements OnModuleInit, OnModuleDestroy, LoyaltyConsumerControlPort
{
  private readonly logger: StructuredLogger;
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;
  private readonly topic: string;
  private isPaused = false;
  private processingDelayMs = 0;

  // Rebalance tracking and scaling state
  private readonly replicas = new Map<string, ReplicaInstance>();
  private readonly rebalanceHistory: RebalanceEventLog[] = [];
  private primaryAssignedPartitions: number[] = [];
  private totalRebalanceCount = 0;

  constructor(
    @Inject(AppConfigService)
    private readonly config: AppConfigService,
    @Inject(ApplyPurchaseEventHandler)
    private readonly handler: ApplyPurchaseEventHandler,
    @Inject(ApplyPurchaseInvalidationHandler)
    private readonly invalidationHandler: ApplyPurchaseInvalidationHandler,
  ) {
    this.logger = new StructuredLogger(KafkaLoyaltyConsumerAdapter.name, {
      serviceName: 'loyalty-worker',
      format: this.config.nodeEnv === 'production' ? 'json' : 'pretty',
    });
    this.topic = this.config.purchaseTopic;
    this.kafka = new Kafka({
      clientId: this.config.kafkaClientId,
      brokers: this.config.kafkaBrokers,
    });
    this.consumer = this.kafka.consumer({
      groupId: this.config.kafkaGroupId,
    });
  }

  private setupConsumerListeners(
    consumer: Consumer,
    instanceId: string,
    clientId: string,
    isPrimary: boolean,
  ): void {
    consumer.on(consumer.events.REBALANCING, (e) => {
      this.totalRebalanceCount++;
      const event: RebalanceEventLog = {
        id: `reb_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
        timestamp: new Date().toISOString(),
        eventType: 'REBALANCING',
        memberId: (e.payload as { memberId?: string })?.memberId ?? instanceId,
        clientId,
        details: `Rebalance initiated for consumer group ${this.config.kafkaGroupId} on member ${clientId}`,
      };
      this.rebalanceHistory.unshift(event);
      if (this.rebalanceHistory.length > 50) this.rebalanceHistory.pop();
      this.logger.warn('[KafkaRebalance] Rebalancing triggered', { instanceId, clientId, event });
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
      if (isPrimary) {
        this.primaryAssignedPartitions = sortedPartitions;
      } else {
        const replica = this.replicas.get(instanceId);
        if (replica) {
          replica.assignedPartitions = sortedPartitions;
        }
      }

      const event: RebalanceEventLog = {
        id: `join_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
        timestamp: new Date().toISOString(),
        eventType: 'GROUP_JOIN',
        memberId: payload.memberId,
        clientId,
        groupGenerationId: payload.groupGenerationId,
        assignedPartitions: sortedPartitions,
        durationMs: payload.duration,
        details: `Rebalance completed in ${payload.duration}ms. Leader: ${payload.isLeader}. Assigned ${sortedPartitions.length} partitions: [${sortedPartitions.join(', ')}]`,
      };
      this.rebalanceHistory.unshift(event);
      if (this.rebalanceHistory.length > 50) this.rebalanceHistory.pop();
      this.logger.info('[KafkaRebalance] Group join complete', { instanceId, clientId, event });
    });

    consumer.on(consumer.events.CRASH, (e) => {
      const payload = e.payload as { error: Error };
      const event: RebalanceEventLog = {
        id: `crash_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
        timestamp: new Date().toISOString(),
        eventType: 'MEMBER_KILLED',
        memberId: instanceId,
        clientId,
        details: `Consumer crash detected on ${clientId}: ${payload.error?.message ?? 'Unknown error'}`,
      };
      this.rebalanceHistory.unshift(event);
      if (this.rebalanceHistory.length > 50) this.rebalanceHistory.pop();
    });
  }

  private async handleMessage(
    consumer: Consumer,
    topic: string,
    partition: number,
    message: KafkaMessage,
  ): Promise<void> {
    if (this.processingDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.processingDelayMs));
    }

    let processedEventId: string | undefined;

    await processKafkaRecord({
      topic,
      partition,
      offset: message.offset,
      key: message.key?.toString(),
      rawValue: message.value?.toString(),
      logger: this.logger,
      consumerGroupId: this.config.kafkaGroupId,
      deserializer: deserializeEnvelope,
      handler: async (envelope) => {
        processedEventId = envelope.eventId;
        if (envelope.eventType === 'PurchaseCompleted') {
          const command = new ApplyPurchaseEventCommand({
            eventId: envelope.eventId,
            customerId: envelope.payload.customerId,
            purchaseId: envelope.payload.purchaseId,
            amountCents: envelope.payload.amountCents,
            customerSequence: envelope.customerSequence,
            correlationId: envelope.correlationId,
          });
          const result = await this.handler.execute(command);
          return {
            eventId: result.eventId,
            customerId: result.customerId,
            pointsEarned: result.pointsEarned,
            totalPoints: result.totalPoints,
            currentTier: result.currentTier,
            isDuplicate: result.isDuplicate,
          };
        }
        if (envelope.eventType === 'PurchaseInvalidated') {
          const command = new ApplyPurchaseInvalidationCommand({
            eventId: envelope.eventId,
            customerId: envelope.payload.customerId,
            purchaseId: envelope.payload.purchaseId,
            customerSequence: envelope.customerSequence,
            correlationId: envelope.correlationId,
            reason: envelope.payload.reason,
          });
          const result = await this.invalidationHandler.execute(command);
          return {
            eventId: result.eventId,
            customerId: result.customerId,
            correctionRequired: result.correctionRequired,
            lastCustomerSequence: result.lastCustomerSequence,
            isDuplicate: result.isDuplicate,
          };
        }
        return {
          eventType: (envelope as { eventType?: string }).eventType,
          ignored: true,
        };
      },
    });

    // Fault injection hook (P4-06): simulate crash AFTER DB commit and BEFORE Kafka offset commit
    if (
      this.config.faultInjectionPoint === 'AFTER_DB_COMMIT_BEFORE_OFFSET_COMMIT' &&
      (!this.config.faultTriggerEventId || this.config.faultTriggerEventId === processedEventId)
    ) {
      this.logger.warn(
        '[FaultInjection] Triggering process exit AFTER DB commit and BEFORE offset commit!',
        {
          eventId: processedEventId,
          topic,
          partition,
          offset: message.offset,
          exitCode: 42,
        },
      );
      process.exit(42);
    }

    // Explicit manual commit discipline: Consumed offset N is committed as N + 1
    const nextOffset = (BigInt(message.offset) + 1n).toString();
    await consumer.commitOffsets([
      {
        topic,
        partition,
        offset: nextOffset,
      },
    ]);
  }

  async onModuleInit(): Promise<void> {
    this.logger.info('Connecting Loyalty Kafka consumer to brokers...', {
      brokers: this.config.kafkaBrokers,
      groupId: this.config.kafkaGroupId,
      topic: this.topic,
    });

    this.setupConsumerListeners(
      this.consumer,
      'loyalty-worker-primary',
      this.config.kafkaClientId,
      true,
    );

    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.topic,
      fromBeginning: this.config.kafkaFromBeginning,
    });

    this.logger.info('Loyalty consumer subscribed and started message loop', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        await this.handleMessage(this.consumer, topic, partition, message);
      },
    });
  }

  async pause(): Promise<void> {
    this.consumer.pause([{ topic: this.topic }]);
    for (const replica of this.replicas.values()) {
      replica.consumer.pause([{ topic: this.topic }]);
      replica.status = 'PAUSED';
    }
    this.isPaused = true;
    this.logger.warn('Loyalty Kafka consumer PAUSED by administrative request', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });
  }

  async resume(): Promise<void> {
    this.consumer.resume([{ topic: this.topic }]);
    for (const replica of this.replicas.values()) {
      replica.consumer.resume([{ topic: this.topic }]);
      replica.status = 'RUNNING';
    }
    this.isPaused = false;
    this.logger.info('Loyalty Kafka consumer RESUMED by administrative request', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });
  }

  async pausePartition(partition: number): Promise<void> {
    this.consumer.pause([{ topic: this.topic, partitions: [partition] }]);
    for (const replica of this.replicas.values()) {
      replica.consumer.pause([{ topic: this.topic, partitions: [partition] }]);
    }
    this.logger.warn(
      `Loyalty Kafka consumer PAUSED partition ${partition} by administrative request`,
      {
        topic: this.topic,
        partition,
        groupId: this.config.kafkaGroupId,
      },
    );
  }

  async resumePartition(partition: number): Promise<void> {
    this.consumer.resume([{ topic: this.topic, partitions: [partition] }]);
    for (const replica of this.replicas.values()) {
      replica.consumer.resume([{ topic: this.topic, partitions: [partition] }]);
    }
    this.logger.info(
      `Loyalty Kafka consumer RESUMED partition ${partition} by administrative request`,
      {
        topic: this.topic,
        partition,
        groupId: this.config.kafkaGroupId,
      },
    );
  }

  setProcessingDelay(delayMs: number): void {
    this.processingDelayMs = Math.max(0, delayMs);
    this.logger.info(`Simulated processing delay set to ${this.processingDelayMs}ms`, {
      delayMs: this.processingDelayMs,
    });
  }

  getProcessingDelay(): number {
    return this.processingDelayMs;
  }

  async scaleInstances(
    targetCount: number,
  ): Promise<{ targetCount: number; instances: LoyaltyConsumerInstanceInfo[] }> {
    const clamped = Math.max(1, Math.min(4, targetCount));
    const currentReplicaCount = this.replicas.size;
    const neededReplicas = clamped - 1;

    if (neededReplicas > currentReplicaCount) {
      // Scale UP: spawn new replica consumers
      for (let i = currentReplicaCount + 1; i <= neededReplicas; i++) {
        const replicaId = `loyalty-worker-replica-${i}`;
        const clientId = `${this.config.kafkaClientId}-replica-${i}`;
        const replicaConsumer = this.kafka.consumer({
          groupId: this.config.kafkaGroupId,
        });

        this.setupConsumerListeners(replicaConsumer, replicaId, clientId, false);

        await replicaConsumer.connect();
        await replicaConsumer.subscribe({
          topic: this.topic,
          fromBeginning: false,
        });

        const replicaInfo: ReplicaInstance = {
          id: replicaId,
          clientId,
          consumer: replicaConsumer,
          status: this.isPaused ? 'PAUSED' : 'RUNNING',
          assignedPartitions: [],
        };
        this.replicas.set(replicaId, replicaInfo);

        await replicaConsumer.run({
          autoCommit: false,
          eachMessage: async ({ topic, partition, message }) => {
            await this.handleMessage(replicaConsumer, topic, partition, message);
          },
        });

        const event: RebalanceEventLog = {
          id: `scale_up_${Date.now()}_${i}`,
          timestamp: new Date().toISOString(),
          eventType: 'MEMBER_JOINED',
          memberId: replicaId,
          clientId,
          details: `Replica instance ${clientId} created and connected to consumer group ${this.config.kafkaGroupId}. Kafka initiating rebalance.`,
        };
        this.rebalanceHistory.unshift(event);
        if (this.rebalanceHistory.length > 50) this.rebalanceHistory.pop();
        this.logger.info(`Scaled up: ${clientId} joined group ${this.config.kafkaGroupId}`);
      }
    } else if (neededReplicas < currentReplicaCount) {
      // Scale DOWN: gracefully disconnect excess replicas
      const keys = Array.from(this.replicas.keys());
      const toRemove = keys.slice(neededReplicas);

      for (const replicaId of toRemove) {
        const replica = this.replicas.get(replicaId);
        if (replica) {
          replica.status = 'STOPPED';
          await replica.consumer.disconnect().catch(() => {});
          this.replicas.delete(replicaId);

          const event: RebalanceEventLog = {
            id: `scale_down_${Date.now()}_${replicaId}`,
            timestamp: new Date().toISOString(),
            eventType: 'MEMBER_LEFT',
            memberId: replicaId,
            clientId: replica.clientId,
            details: `Replica instance ${replica.clientId} gracefully disconnected. Kafka rebalancing partitions back to remaining members.`,
          };
          this.rebalanceHistory.unshift(event);
          if (this.rebalanceHistory.length > 50) this.rebalanceHistory.pop();
          this.logger.info(`Scaled down: ${replica.clientId} disconnected from group`);
        }
      }
    }

    const status = await this.getStatus();
    return { targetCount: clamped, instances: status.instances };
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

    const event: RebalanceEventLog = {
      id: `kill_${Date.now()}_${targetId}`,
      timestamp: new Date().toISOString(),
      eventType: 'MEMBER_KILLED',
      memberId: targetId,
      clientId: replica.clientId,
      details: `[Fault Injection / Chaos] Replica instance ${replica.clientId} abruptly killed! Kafka broker detecting leave/heartbeat timeout and reassigning partitions.`,
    };
    this.rebalanceHistory.unshift(event);
    if (this.rebalanceHistory.length > 50) this.rebalanceHistory.pop();

    return {
      killedInstanceId: targetId,
      remainingCount: 1 + this.replicas.size,
    };
  }

  getRebalanceHistory(): RebalanceEventLog[] {
    return [...this.rebalanceHistory];
  }

  async getStatus(): Promise<LoyaltyConsumerControlStatus> {
    const pausedPartitions = this.consumer
      .paused()
      .filter((p) => p.topic === this.topic)
      .flatMap((p) => p.partitions);

    const instances: LoyaltyConsumerInstanceInfo[] = [
      {
        instanceId: 'loyalty-worker-primary',
        clientId: this.config.kafkaClientId,
        isPrimary: true,
        status: this.isPaused ? 'PAUSED' : 'RUNNING',
        assignedPartitions: this.primaryAssignedPartitions,
      },
    ];

    for (const replica of this.replicas.values()) {
      instances.push({
        instanceId: replica.id,
        clientId: replica.clientId,
        isPrimary: false,
        status: replica.status,
        assignedPartitions: replica.assignedPartitions,
      });
    }

    return {
      groupId: this.config.kafkaGroupId,
      topic: this.topic,
      isPaused: this.isPaused,
      pausedPartitions,
      processingDelayMs: this.processingDelayMs,
      instanceCount: instances.length,
      instances,
      totalRebalanceCount: this.totalRebalanceCount,
    };
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.info('Disconnecting Loyalty Kafka consumers...', {
      groupId: this.config.kafkaGroupId,
    });
    for (const replica of this.replicas.values()) {
      await replica.consumer.disconnect().catch(() => {});
    }
    await this.consumer.disconnect().catch(() => {});
    this.logger.info('All Loyalty Kafka consumers disconnected');
  }
}

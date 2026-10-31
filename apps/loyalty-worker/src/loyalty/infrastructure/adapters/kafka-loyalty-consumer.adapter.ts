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
import { LoyaltyConsumerReplicaFleet } from './loyalty-consumer-replica-fleet';

@Injectable()
export class KafkaLoyaltyConsumerAdapter
  implements OnModuleInit, OnModuleDestroy, LoyaltyConsumerControlPort
{
  private readonly logger: StructuredLogger;
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;
  private readonly topic: string;
  private readonly replicaFleet: LoyaltyConsumerReplicaFleet;
  private isPaused = false;
  private processingDelayMs = 0;
  private primaryAssignedPartitions: number[] = [];

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
    this.replicaFleet = new LoyaltyConsumerReplicaFleet(
      this.config,
      this.kafka,
      this.topic,
      this.logger,
    );
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

    this.replicaFleet.setupConsumerListeners(
      this.consumer,
      'loyalty-worker-primary',
      this.config.kafkaClientId,
      (partitions) => {
        this.primaryAssignedPartitions = partitions;
      },
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
    this.replicaFleet.pauseReplicas();
    this.isPaused = true;
    this.logger.warn('Loyalty Kafka consumer PAUSED by administrative request', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });
  }

  async resume(): Promise<void> {
    this.consumer.resume([{ topic: this.topic }]);
    this.replicaFleet.resumeReplicas();
    this.isPaused = false;
    this.logger.info('Loyalty Kafka consumer RESUMED by administrative request', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });
  }

  async pausePartition(partition: number): Promise<void> {
    this.consumer.pause([{ topic: this.topic, partitions: [partition] }]);
    this.replicaFleet.pauseReplicas([partition]);
    this.logger.warn(
      `Loyalty Kafka consumer PAUSED partition ${partition} by administrative request`,
      { topic: this.topic, partition, groupId: this.config.kafkaGroupId },
    );
  }

  async resumePartition(partition: number): Promise<void> {
    this.consumer.resume([{ topic: this.topic, partitions: [partition] }]);
    this.replicaFleet.resumeReplicas([partition]);
    this.logger.info(
      `Loyalty Kafka consumer RESUMED partition ${partition} by administrative request`,
      { topic: this.topic, partition, groupId: this.config.kafkaGroupId },
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
    await this.replicaFleet.scaleInstances(
      targetCount,
      this.isPaused,
      async (consumer, topic, partition, message) => {
        await this.handleMessage(consumer, topic, partition, message);
      },
    );
    const status = await this.getStatus();
    return { targetCount: Math.max(1, Math.min(4, targetCount)), instances: status.instances };
  }

  async killReplicaInstance(
    instanceId?: string,
  ): Promise<{ killedInstanceId: string; remainingCount: number }> {
    return this.replicaFleet.killReplicaInstance(instanceId);
  }

  getRebalanceHistory(): RebalanceEventLog[] {
    return this.replicaFleet.getRebalanceHistory();
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
      ...this.replicaFleet.getReplicaInstances(),
    ];

    return {
      groupId: this.config.kafkaGroupId,
      topic: this.topic,
      isPaused: this.isPaused,
      pausedPartitions,
      processingDelayMs: this.processingDelayMs,
      instanceCount: instances.length,
      instances,
      totalRebalanceCount: this.replicaFleet.getTotalRebalanceCount(),
    };
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.info('Disconnecting Loyalty Kafka consumers...', {
      groupId: this.config.kafkaGroupId,
    });
    await this.replicaFleet.disconnectAll();
    await this.consumer.disconnect().catch(() => {});
    this.logger.info('All Loyalty Kafka consumers disconnected');
  }
}

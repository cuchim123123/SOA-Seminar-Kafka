import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { deserializeEnvelope } from '@lab/contracts';
import { processKafkaRecord, StructuredLogger } from '@lab/observability';
import { Consumer, Kafka } from 'kafkajs';
import { AppConfigService } from '../../../config/app-config.service';
import { ApplyPurchaseEventCommand } from '../../application/commands/apply-purchase-event.command';
import { ApplyPurchaseEventHandler } from '../../application/commands/apply-purchase-event.handler';
import { ApplyPurchaseInvalidationCommand } from '../../application/commands/apply-purchase-invalidation.command';
import { ApplyPurchaseInvalidationHandler } from '../../application/commands/apply-purchase-invalidation.handler';

import {
  LoyaltyConsumerControlPort,
  LoyaltyConsumerControlStatus,
} from '../../application/ports/consumer-control.port';

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

  async onModuleInit(): Promise<void> {
    this.logger.info('Connecting Loyalty Kafka consumer to brokers...', {
      brokers: this.config.kafkaBrokers,
      groupId: this.config.kafkaGroupId,
      topic: this.topic,
    });
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.topic,
      fromBeginning: true,
    });

    this.logger.info('Loyalty consumer subscribed and started message loop', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
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
        await this.consumer.commitOffsets([
          {
            topic,
            partition,
            offset: nextOffset,
          },
        ]);
      },
    });
  }

  async pause(): Promise<void> {
    this.consumer.pause([{ topic: this.topic }]);
    this.isPaused = true;
    this.logger.warn('Loyalty Kafka consumer PAUSED by administrative request', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });
  }

  async resume(): Promise<void> {
    this.consumer.resume([{ topic: this.topic }]);
    this.isPaused = false;
    this.logger.info('Loyalty Kafka consumer RESUMED by administrative request', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });
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

  async getStatus(): Promise<LoyaltyConsumerControlStatus> {
    const pausedPartitions = this.consumer
      .paused()
      .filter((p) => p.topic === this.topic)
      .flatMap((p) => p.partitions);

    return {
      groupId: this.config.kafkaGroupId,
      topic: this.topic,
      isPaused: this.isPaused,
      pausedPartitions,
      processingDelayMs: this.processingDelayMs,
    };
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.info('Disconnecting Loyalty Kafka consumer...', {
      groupId: this.config.kafkaGroupId,
    });
    await this.consumer.disconnect();
    this.logger.info('Loyalty Kafka consumer disconnected');
  }
}

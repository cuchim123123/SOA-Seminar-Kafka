import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { deserializeEnvelope } from '@lab/contracts';
import { processKafkaRecord, StructuredLogger } from '@lab/observability';
import { Consumer, Kafka } from 'kafkajs';
import { AppConfigService } from '../../../config/app-config.service';
import { ApplyPurchaseEventCommand } from '../../application/commands/apply-purchase-event.command';
import { ApplyPurchaseEventHandler } from '../../application/commands/apply-purchase-event.handler';

@Injectable()
export class KafkaLoyaltyConsumerAdapter implements OnModuleInit, OnModuleDestroy {
  private readonly logger: StructuredLogger;
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;
  private readonly topic: string;

  constructor(
    @Inject(AppConfigService)
    private readonly config: AppConfigService,
    @Inject(ApplyPurchaseEventHandler)
    private readonly handler: ApplyPurchaseEventHandler,
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

  async onModuleDestroy(): Promise<void> {
    this.logger.info('Disconnecting Loyalty Kafka consumer...', {
      groupId: this.config.kafkaGroupId,
    });
    await this.consumer.disconnect();
    this.logger.info('Loyalty Kafka consumer disconnected');
  }
}

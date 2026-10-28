import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { deserializeEnvelope } from '@lab/contracts';
import { Consumer, Kafka } from 'kafkajs';
import { AppConfigService } from '../../../config/app-config.service';
import { ApplyPurchaseEventCommand } from '../../application/commands/apply-purchase-event.command';
import { ApplyPurchaseEventHandler } from '../../application/commands/apply-purchase-event.handler';

@Injectable()
export class KafkaLoyaltyConsumerAdapter implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaLoyaltyConsumerAdapter.name);
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;
  private readonly topic: string;

  constructor(
    @Inject(AppConfigService)
    private readonly config: AppConfigService,
    @Inject(ApplyPurchaseEventHandler)
    private readonly handler: ApplyPurchaseEventHandler,
  ) {
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
    this.logger.log(
      `Connecting Loyalty consumer [groupId=${this.config.kafkaGroupId}] to brokers: ${this.config.kafkaBrokers.join(', ')}...`,
    );
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.topic,
      fromBeginning: true,
    });

    this.logger.log(
      `Loyalty consumer subscribed to topic '${this.topic}' with groupId '${this.config.kafkaGroupId}'. Starting message consumption...`,
    );

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value?.toString();
        const key = message.key?.toString() ?? '<null>';
        const offset = message.offset;

        if (!rawValue) {
          this.logger.warn(
            `Received empty message payload from topic=${topic} partition=${partition} offset=${offset}`,
          );
          return;
        }

        try {
          const envelope = deserializeEnvelope(rawValue);

          this.logger.log(
            `[KafkaBrokerMetadata] Received record: key=${key}, topic=${topic}, partition=${partition}, offset=${offset}, eventId=${envelope.eventId}, eventType=${envelope.eventType}`,
          );

          if (envelope.eventType === 'PurchaseCompleted') {
            const command = new ApplyPurchaseEventCommand(
              envelope.eventId,
              envelope.payload.customerId,
              envelope.payload.purchaseId,
              envelope.payload.amountCents,
              envelope.customerSequence,
              envelope.correlationId,
            );
            const result = await this.handler.execute(command);
            this.logger.log(
              `[KafkaLoyaltyConsumer] Applied event ${result.eventId} from topic=${topic} [p=${partition}, offset=${offset}]: customer=${result.customerId}, earned=${result.pointsEarned}, total=${result.totalPoints}`,
            );
          }
        } catch (err: unknown) {
          this.logger.error(
            `Error processing message from topic=${topic} partition=${partition} offset=${offset}: ${err instanceof Error ? err.message : String(err)}`,
          );
        }
      },
    });
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.log('Disconnecting Loyalty Kafka consumer...');
    await this.consumer.disconnect();
    this.logger.log('Loyalty Kafka consumer disconnected');
  }
}

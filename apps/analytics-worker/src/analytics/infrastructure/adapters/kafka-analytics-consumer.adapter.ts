import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { deserializeEnvelope } from '@lab/contracts';
import { processKafkaRecord, StructuredLogger } from '@lab/observability';
import { Consumer, Kafka } from 'kafkajs';
import { AppConfigService } from '../../../config/app-config.service';
import { ApplySalesEventCommand } from '../../application/commands/apply-sales-event.command';
import { ApplySalesEventHandler } from '../../application/commands/apply-sales-event.handler';
import {
  ConsumerControlStatus,
  ConsumerControlPort,
} from '../../application/ports/consumer-control.port';

@Injectable()
export class KafkaAnalyticsConsumerAdapter
  implements ConsumerControlPort, OnModuleInit, OnModuleDestroy
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
    @Inject(ApplySalesEventHandler)
    private readonly handler: ApplySalesEventHandler,
  ) {
    this.logger = new StructuredLogger(KafkaAnalyticsConsumerAdapter.name, {
      serviceName: 'analytics-worker',
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
    this.logger.info('Connecting Analytics Kafka consumer to brokers...', {
      brokers: this.config.kafkaBrokers,
      groupId: this.config.kafkaGroupId,
      topic: this.topic,
    });
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.topic,
      fromBeginning: true,
    });

    this.logger.info('Analytics consumer subscribed and started message loop', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        if (this.processingDelayMs > 0) {
          await new Promise((resolve) => setTimeout(resolve, this.processingDelayMs));
        }

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
            if (envelope.eventType === 'PurchaseCompleted') {
              const command = new ApplySalesEventCommand({
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
                totalRevenueCents: result.totalRevenueCents,
                purchaseCount: result.purchaseCount,
                uniqueCustomerCount: result.uniqueCustomerCount,
                isDuplicate: result.isDuplicate,
              };
            }
            return {
              eventType: (envelope as { eventType?: string }).eventType,
              ignored: true,
            };
          },
        });

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
    this.logger.warn('Analytics Kafka consumer PAUSED by operator request', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });
  }

  async resume(): Promise<void> {
    this.consumer.resume([{ topic: this.topic }]);
    this.isPaused = false;
    this.logger.info('Analytics Kafka consumer RESUMED by operator request', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });
  }

  setProcessingDelay(delayMs: number): void {
    this.processingDelayMs = Math.max(0, Math.round(delayMs));
    this.logger.info('Analytics consumer simulated processing delay updated', {
      delayMs: this.processingDelayMs,
      groupId: this.config.kafkaGroupId,
    });
  }

  getProcessingDelay(): number {
    return this.processingDelayMs;
  }

  async getStatus(): Promise<ConsumerControlStatus> {
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
    this.logger.info('Disconnecting Analytics Kafka consumer...', {
      groupId: this.config.kafkaGroupId,
    });
    await this.consumer.disconnect();
    this.logger.info('Analytics Kafka consumer disconnected');
  }
}

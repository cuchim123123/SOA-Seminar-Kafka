/* eslint-disable @typescript-eslint/no-explicit-any */
import { Inject, Injectable, OnModuleDestroy, OnModuleInit, Optional } from '@nestjs/common';
import { deserializeEnvelope } from '@lab/contracts';
import { MetricsService, processKafkaRecord, StructuredLogger } from '@lab/observability';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { AppConfigService } from '../../../config/app-config.service';
import {
  ApplySalesEventCommand,
  ApplySalesEventHandler,
} from '../../application/commands/apply-sales-event';
import {
  ConsumerControlStatus,
  ConsumerControlPort,
} from '../../application/ports/consumer-control.port';

@Injectable()
export class KafkaAnalyticsConsumerAdapter
  implements ConsumerControlPort, OnModuleInit, OnModuleDestroy
{
  private readonly logger: StructuredLogger;
  private readonly kafka: KafkaJS.Kafka;
  private readonly consumer: any;
  private readonly topic: string;
  private isPaused = false;
  private processingDelayMs = 0;

  constructor(
    @Inject(AppConfigService)
    private readonly config: AppConfigService,
    @Inject(ApplySalesEventHandler)
    private readonly handler: ApplySalesEventHandler,
    @Optional()
    private readonly metricsService?: MetricsService,
  ) {
    this.logger = new StructuredLogger(KafkaAnalyticsConsumerAdapter.name, {
      serviceName: 'analytics-worker',
      format: this.config.nodeEnv === 'production' ? 'json' : 'pretty',
    });
    this.topic = this.config.purchaseTopic;
    this.kafka = new KafkaJS.Kafka({
      'bootstrap.servers': this.config.kafkaBrokers.join(','),
      kafkaJS: {
        clientId: this.config.kafkaClientId,
        brokers: this.config.kafkaBrokers,
      },
    });
    this.consumer = this.kafka.consumer({
      'enable.auto.commit': false,
      kafkaJS: {
        groupId: this.config.kafkaGroupId,
        fromBeginning: this.config.kafkaFromBeginning,
        autoCommit: false,
      },
    });
  }

  async onModuleInit(): Promise<void> {
    this.logger.info('Connecting Analytics Kafka consumer to brokers...', {
      brokers: this.config.kafkaBrokers,
      groupId: this.config.kafkaGroupId,
      topic: this.topic,
    });
    await this.consumer.connect();
    await (this.consumer as any).subscribe({
      topic: this.topic,
    });

    this.logger.info('Analytics consumer subscribed and started message loop', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });

    await (this.consumer as any).run({
      eachMessage: async ({ topic, partition, message }: any) => {
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
          metricsService: this.metricsService,
          consumerRole: 'analytics',
          transport: 'kafka',
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
        try {
          await this.consumer.commitOffsets([
            {
              topic,
              partition,
              offset: nextOffset,
            },
          ]);
          this.metricsService?.recordAck('analytics', 'kafka', 'success');
        } catch (commitErr: unknown) {
          this.metricsService?.recordAck('analytics', 'kafka', 'failure');
          throw commitErr;
        }
      },
    });
    this.metricsService?.setConsumerState('analytics', 'polling');
  }

  async pause(): Promise<void> {
    this.consumer.pause([{ topic: this.topic }]);
    this.isPaused = true;
    this.metricsService?.setConsumerState('analytics', 'paused');
    this.logger.warn('Analytics Kafka consumer PAUSED by operator request', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });
  }

  async resume(): Promise<void> {
    this.consumer.resume([{ topic: this.topic }]);
    this.isPaused = false;
    this.metricsService?.setConsumerState('analytics', 'polling');
    this.logger.info('Analytics Kafka consumer RESUMED by operator request', {
      topic: this.topic,
      groupId: this.config.kafkaGroupId,
    });
  }

  async pausePartition(partition: number): Promise<void> {
    this.consumer.pause([{ topic: this.topic, partitions: [partition] }]);
    this.logger.warn(`Analytics Kafka consumer PAUSED partition ${partition} by operator request`, {
      topic: this.topic,
      partition,
      groupId: this.config.kafkaGroupId,
    });
  }

  async resumePartition(partition: number): Promise<void> {
    this.consumer.resume([{ topic: this.topic, partitions: [partition] }]);
    this.logger.info(
      `Analytics Kafka consumer RESUMED partition ${partition} by operator request`,
      {
        topic: this.topic,
        partition,
        groupId: this.config.kafkaGroupId,
      },
    );
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
      .filter((p: any) => p.topic === this.topic)
      .flatMap((p: any) => p.partitions ?? []);

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
    this.metricsService?.setConsumerState('analytics', 'stopped');
    this.logger.info('Analytics Kafka consumer disconnected');
  }
}

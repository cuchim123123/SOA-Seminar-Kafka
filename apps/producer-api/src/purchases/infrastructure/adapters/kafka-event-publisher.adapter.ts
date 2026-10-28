import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import {
  getEventPartitionKey,
  IntegrationEventEnvelopeV1,
  serializeEnvelope,
} from '@lab/contracts';
import { StructuredLogger } from '@lab/observability';
import { Kafka, Partitioners, Producer, RecordMetadata } from 'kafkajs';
import { AppConfigService } from '../../../config/app-config.service';
import { EventPublisherPort, PublishResult } from '../../application/ports/event-publisher.port';

@Injectable()
export class KafkaEventPublisherAdapter
  implements EventPublisherPort, OnModuleInit, OnModuleDestroy
{
  private readonly logger: StructuredLogger;
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly topic: string;

  constructor(@Inject(AppConfigService) private readonly config: AppConfigService) {
    this.logger = new StructuredLogger(KafkaEventPublisherAdapter.name, {
      serviceName: 'producer-api',
      format: this.config.nodeEnv === 'production' ? 'json' : 'pretty',
    });
    this.topic = this.config.purchaseTopic;
    this.kafka = new Kafka({
      clientId: this.config.kafkaClientId,
      brokers: this.config.kafkaBrokers,
    });
    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.DefaultPartitioner,
      allowAutoTopicCreation: false,
    });
  }

  async onModuleInit(): Promise<void> {
    this.logger.info('Connecting Kafka producer to brokers...', {
      brokers: this.config.kafkaBrokers,
      topic: this.topic,
    });
    await this.producer.connect();
    this.logger.info('Kafka producer connected successfully');
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.info('Disconnecting Kafka producer...');
    await this.producer.disconnect();
    this.logger.info('Kafka producer disconnected');
  }

  async publish(envelope: IntegrationEventEnvelopeV1): Promise<PublishResult> {
    const partitionKey = getEventPartitionKey(envelope);
    const serialized = serializeEnvelope(envelope);

    const recordMetadata: RecordMetadata[] = await this.producer.send({
      topic: this.topic,
      messages: [
        {
          key: partitionKey,
          value: serialized,
        },
      ],
      acks: -1,
    });

    const firstResult = recordMetadata[0];
    if (!firstResult) {
      throw new Error(
        `Kafka producer sent message to ${this.topic} but received empty record metadata`,
      );
    }

    const offset = firstResult.baseOffset ?? firstResult.offset ?? '0';

    return {
      topic: firstResult.topicName,
      partition: firstResult.partition,
      offset,
      eventId: envelope.eventId,
      correlationId: envelope.correlationId,
    };
  }
}

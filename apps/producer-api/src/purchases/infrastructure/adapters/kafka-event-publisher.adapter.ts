import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Partitioners, Producer, RecordMetadata } from 'kafkajs';
import {
  getEventPartitionKey,
  IntegrationEventEnvelopeV1,
  serializeEnvelope,
} from '@lab/contracts';
import { AppConfigService } from '../../../config/app-config.service';
import { EventPublisherPort, PublishResult } from '../../application/ports/event-publisher.port';

@Injectable()
export class KafkaEventPublisherAdapter
  implements EventPublisherPort, OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(KafkaEventPublisherAdapter.name);
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly topic: string;

  constructor(@Inject(AppConfigService) private readonly config: AppConfigService) {
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
    this.logger.log(
      `Connecting Kafka producer to brokers: ${this.config.kafkaBrokers.join(', ')}...`,
    );
    await this.producer.connect();
    this.logger.log('Kafka producer connected successfully');
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.log('Disconnecting Kafka producer...');
    await this.producer.disconnect();
    this.logger.log('Kafka producer disconnected');
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

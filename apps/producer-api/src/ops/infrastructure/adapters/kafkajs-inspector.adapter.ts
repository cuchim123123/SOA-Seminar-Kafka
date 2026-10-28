import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { StructuredLogger } from '@lab/observability';
import { Admin, Kafka } from 'kafkajs';
import { AppConfigService } from '../../../config/app-config.service';
import {
  KafkaInspectorPort,
  PartitionPosition,
  TopicInspectionResult,
} from '../../application/ports/kafka-inspector.port';

@Injectable()
export class KafkaJsInspectorAdapter implements KafkaInspectorPort, OnModuleInit, OnModuleDestroy {
  private readonly logger: StructuredLogger;
  private readonly kafka: Kafka;
  private readonly admin: Admin;

  constructor(@Inject(AppConfigService) private readonly config: AppConfigService) {
    this.logger = new StructuredLogger(KafkaJsInspectorAdapter.name, {
      serviceName: 'producer-api',
      format: this.config.nodeEnv === 'production' ? 'json' : 'pretty',
    });
    this.kafka = new Kafka({
      clientId: `${this.config.kafkaClientId}-inspector`,
      brokers: this.config.kafkaBrokers,
    });
    this.admin = this.kafka.admin();
  }

  async onModuleInit(): Promise<void> {
    this.logger.info('Connecting KafkaJS admin client for ops inspection...');
    await this.admin.connect();
    this.logger.info('KafkaJS admin client connected');
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.info('Disconnecting KafkaJS admin client...');
    await this.admin.disconnect();
    this.logger.info('KafkaJS admin client disconnected');
  }

  async inspectTopic(topic: string): Promise<TopicInspectionResult> {
    const metadata = await this.admin.fetchTopicMetadata({ topics: [topic] });
    const topicInfo = metadata.topics.find((t) => t.name === topic);

    if (!topicInfo) {
      throw new Error(`Topic '${topic}' not found on Kafka broker cluster`);
    }

    const offsets = await this.admin.fetchTopicOffsets(topic);

    const partitions: PartitionPosition[] = topicInfo.partitions.map((p) => {
      const offsetInfo = offsets.find((o) => o.partition === p.partitionId);
      return {
        topic,
        partition: p.partitionId,
        leader: p.leader,
        replicas: p.replicas,
        isr: p.isr,
        logStartOffset: offsetInfo?.low ?? '0',
        logEndOffset: offsetInfo?.high ?? '0',
      };
    });

    return {
      topic,
      partitionCount: partitions.length,
      partitions,
    };
  }
}

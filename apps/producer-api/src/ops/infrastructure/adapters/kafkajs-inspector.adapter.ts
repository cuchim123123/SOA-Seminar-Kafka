import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Admin, Kafka } from 'kafkajs';
import { AppConfigService } from '../../../config/app-config.service';
import {
  KafkaInspectorPort,
  PartitionPosition,
  TopicInspectionResult,
} from '../../application/ports/kafka-inspector.port';

@Injectable()
export class KafkaJsInspectorAdapter implements KafkaInspectorPort, OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaJsInspectorAdapter.name);
  private readonly kafka: Kafka;
  private readonly admin: Admin;

  constructor(@Inject(AppConfigService) private readonly config: AppConfigService) {
    this.kafka = new Kafka({
      clientId: `${this.config.kafkaClientId}-inspector`,
      brokers: this.config.kafkaBrokers,
    });
    this.admin = this.kafka.admin();
  }

  async onModuleInit(): Promise<void> {
    this.logger.log('Connecting KafkaJS admin client for ops inspection...');
    await this.admin.connect();
    this.logger.log('KafkaJS admin client connected');
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.log('Disconnecting KafkaJS admin client...');
    await this.admin.disconnect();
    this.logger.log('KafkaJS admin client disconnected');
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

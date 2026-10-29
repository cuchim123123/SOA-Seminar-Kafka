import { Inject, Injectable } from '@nestjs/common';
import { StructuredLogger } from '@lab/observability';
import { Admin, Kafka } from 'kafkajs';
import { AppConfigService } from '../../../config/app-config.service';
import {
  TopicBoundaries,
  TopicBoundaryInspectorPort,
} from '../../application/ports/topic-boundary-inspector.port';

@Injectable()
export class KafkaJsTopicBoundaryInspectorAdapter implements TopicBoundaryInspectorPort {
  private readonly logger: StructuredLogger;
  private readonly kafka: Kafka;

  constructor(@Inject(AppConfigService) private readonly config: AppConfigService) {
    this.logger = new StructuredLogger(KafkaJsTopicBoundaryInspectorAdapter.name, {
      serviceName: 'replay-worker',
      format: this.config.nodeEnv === 'production' ? 'json' : 'pretty',
    });
    this.kafka = new Kafka({
      clientId: `${this.config.kafkaClientId}-boundary-inspector`,
      brokers: this.config.kafkaBrokers,
    });
  }

  async getTopicBoundaries(topic: string): Promise<TopicBoundaries> {
    const admin: Admin = this.kafka.admin();
    await admin.connect();
    try {
      this.logger.info('Fetching topic boundaries from Kafka broker...', { topic });
      const offsets = await admin.fetchTopicOffsets(topic);

      const startOffsets: Record<number, string> = {};
      const highWatermarks: Record<number, string> = {};

      for (const offsetInfo of offsets) {
        startOffsets[offsetInfo.partition] = offsetInfo.low ?? '0';
        highWatermarks[offsetInfo.partition] = offsetInfo.high ?? '0';
      }

      this.logger.info('Captured fixed replay boundaries', {
        topic,
        startOffsets,
        highWatermarks,
      });

      return {
        topic,
        startOffsets,
        highWatermarks,
      };
    } finally {
      await admin.disconnect();
    }
  }
}

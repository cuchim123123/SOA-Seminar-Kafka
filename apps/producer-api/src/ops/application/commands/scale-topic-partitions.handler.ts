import { Inject, Injectable } from '@nestjs/common';
import {
  KAFKA_INSPECTOR_PORT,
  KafkaInspectorPort,
  TopicInspectionResult,
} from '../ports/kafka-inspector.port';
import { ScaleTopicPartitionsCommand } from './scale-topic-partitions.command';

@Injectable()
export class ScaleTopicPartitionsHandler {
  constructor(
    @Inject(KAFKA_INSPECTOR_PORT)
    private readonly inspector: KafkaInspectorPort,
  ) {}

  async execute(command: ScaleTopicPartitionsCommand): Promise<TopicInspectionResult> {
    return this.inspector.scaleTopicPartitions(command.data.topic, command.data.newPartitionCount);
  }
}

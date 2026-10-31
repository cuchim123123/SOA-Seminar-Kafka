import { Inject, Injectable } from '@nestjs/common';
import {
  STREAM_INSPECTOR_PORT,
  StreamInspectorPort,
  TopicInspectionResult,
} from '../../ports/stream-inspector.port';
import { ScaleTopicPartitionsCommand } from './scale-topic-partitions.command';

@Injectable()
export class ScaleTopicPartitionsHandler {
  constructor(
    @Inject(STREAM_INSPECTOR_PORT)
    private readonly inspector: StreamInspectorPort,
  ) {}

  async execute(command: ScaleTopicPartitionsCommand): Promise<TopicInspectionResult> {
    return this.inspector.scaleTopicPartitions(command.data.topic, command.data.newPartitionCount);
  }
}

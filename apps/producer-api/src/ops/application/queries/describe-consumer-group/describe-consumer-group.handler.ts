import { Inject, Injectable } from '@nestjs/common';
import {
  ConsumerGroupDescriptionResult,
  STREAM_INSPECTOR_PORT,
  StreamInspectorPort,
} from '../../ports/stream-inspector.port';
import { DescribeConsumerGroupQuery } from './describe-consumer-group.query';

@Injectable()
export class DescribeConsumerGroupHandler {
  constructor(
    @Inject(STREAM_INSPECTOR_PORT)
    private readonly inspector: StreamInspectorPort,
  ) {}

  async execute(query: DescribeConsumerGroupQuery): Promise<ConsumerGroupDescriptionResult> {
    const { groupId, topic } = query.data;
    return this.inspector.describeConsumerGroup(groupId, topic);
  }
}

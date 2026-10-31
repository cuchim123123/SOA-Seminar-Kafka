import { Inject, Injectable } from '@nestjs/common';
import {
  ConsumerGroupLagResult,
  STREAM_INSPECTOR_PORT,
  StreamInspectorPort,
} from '../../ports/stream-inspector.port';
import { GetConsumerGroupLagQuery } from './get-consumer-group-lag.query';

@Injectable()
export class GetConsumerGroupLagHandler {
  constructor(
    @Inject(STREAM_INSPECTOR_PORT)
    private readonly inspector: StreamInspectorPort,
  ) {}

  async execute(query: GetConsumerGroupLagQuery): Promise<ConsumerGroupLagResult> {
    const { groupId, topic } = query.data;
    return this.inspector.inspectConsumerGroupLag(groupId, topic);
  }
}

import { Inject, Injectable } from '@nestjs/common';
import {
  ConsumerGroupLagResult,
  KAFKA_INSPECTOR_PORT,
  KafkaInspectorPort,
} from '../ports/kafka-inspector.port';
import { GetConsumerGroupLagQuery } from './get-consumer-group-lag.query';

@Injectable()
export class GetConsumerGroupLagHandler {
  constructor(
    @Inject(KAFKA_INSPECTOR_PORT)
    private readonly inspector: KafkaInspectorPort,
  ) {}

  async execute(query: GetConsumerGroupLagQuery): Promise<ConsumerGroupLagResult> {
    const { groupId, topic } = query.data;
    return this.inspector.inspectConsumerGroupLag(groupId, topic);
  }
}

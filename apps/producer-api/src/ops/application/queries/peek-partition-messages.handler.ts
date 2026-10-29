import { Inject, Injectable } from '@nestjs/common';
import {
  BrokerMessageRecord,
  KAFKA_INSPECTOR_PORT,
  KafkaInspectorPort,
} from '../ports/kafka-inspector.port';
import { PeekPartitionMessagesQuery } from './peek-partition-messages.query';

@Injectable()
export class PeekPartitionMessagesHandler {
  constructor(
    @Inject(KAFKA_INSPECTOR_PORT)
    private readonly inspector: KafkaInspectorPort,
  ) {}

  async execute(query: PeekPartitionMessagesQuery): Promise<BrokerMessageRecord[]> {
    return this.inspector.peekPartitionMessages(
      query.data.topic,
      query.data.partition,
      query.data.limit,
    );
  }
}

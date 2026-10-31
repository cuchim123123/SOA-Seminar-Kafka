import { Inject, Injectable } from '@nestjs/common';
import {
  BrokerMessageRecord,
  STREAM_INSPECTOR_PORT,
  StreamInspectorPort,
} from '../../ports/stream-inspector.port';
import { PeekPartitionMessagesQuery } from './peek-partition-messages.query';

@Injectable()
export class PeekPartitionMessagesHandler {
  constructor(
    @Inject(STREAM_INSPECTOR_PORT)
    private readonly inspector: StreamInspectorPort,
  ) {}

  async execute(query: PeekPartitionMessagesQuery): Promise<BrokerMessageRecord[]> {
    return this.inspector.peekPartitionMessages(
      query.data.topic,
      query.data.partition,
      query.data.limit,
    );
  }
}

import { describe, expect, it } from 'vitest';
import {
  ConsumerGroupLagResult,
  KafkaInspectorPort,
} from '../../../src/ops/application/ports/kafka-inspector.port';
import { GetConsumerGroupLagHandler } from '../../../src/ops/application/queries/get-consumer-group-lag.handler';
import { GetConsumerGroupLagQuery } from '../../../src/ops/application/queries/get-consumer-group-lag.query';

describe('GetConsumerGroupLagHandler', () => {
  it('delegates consumer group lag inspection to KafkaInspectorPort with UNCOMMITTED handling', async () => {
    const mockLagResult: ConsumerGroupLagResult = {
      groupId: 'loyalty-v1',
      topic: 'purchase.events',
      totalLag: 'UNCOMMITTED',
      partitions: [
        {
          topic: 'purchase.events',
          partition: 0,
          logStartOffset: '0',
          logEndOffset: '15',
          committedOffset: null,
          lag: 'UNCOMMITTED',
          isUncommitted: true,
        },
        {
          topic: 'purchase.events',
          partition: 1,
          logStartOffset: '0',
          logEndOffset: '10',
          committedOffset: '10',
          lag: '0',
          isUncommitted: false,
        },
      ],
    };

    const mockInspector: KafkaInspectorPort = {
      inspectTopic: async () => {
        throw new Error('Not implemented');
      },
      inspectConsumerGroupLag: async (groupId: string, topic?: string) => {
        expect(groupId).toBe('loyalty-v1');
        expect(topic).toBe('purchase.events');
        return mockLagResult;
      },
      scaleTopicPartitions: async () => {
        throw new Error('Not implemented');
      },
      peekPartitionMessages: async () => [],
      publishRawMessage: async () => ({ topic: 'purchase.events', partition: 0, offset: '0' }),
    };

    const handler = new GetConsumerGroupLagHandler(mockInspector);
    const result = await handler.execute(
      new GetConsumerGroupLagQuery({ groupId: 'loyalty-v1', topic: 'purchase.events' }),
    );

    expect(result).toEqual(mockLagResult);
    expect(result.partitions[0].lag).toBe('UNCOMMITTED');
    expect(result.partitions[1].lag).toBe('0');
  });
});

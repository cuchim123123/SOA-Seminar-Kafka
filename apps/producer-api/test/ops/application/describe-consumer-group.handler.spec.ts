import { describe, expect, it } from 'vitest';
import {
  ConsumerGroupDescriptionResult,
  StreamInspectorPort,
} from '../../../src/ops/application/ports/stream-inspector.port';
import { DescribeConsumerGroupHandler } from '../../../src/ops/application/queries/describe-consumer-group/describe-consumer-group.handler';
import { DescribeConsumerGroupQuery } from '../../../src/ops/application/queries/describe-consumer-group/describe-consumer-group.query';

describe('DescribeConsumerGroupHandler', () => {
  it('delegates consumer group description to StreamInspectorPort and returns members and assignments', async () => {
    const mockDescription: ConsumerGroupDescriptionResult = {
      groupId: 'loyalty-v1',
      state: 'Stable',
      protocol: 'RoundRobinAssigner',
      protocolType: 'consumer',
      members: [
        {
          memberId: 'loyalty-worker-primary-123',
          clientId: 'loyalty-worker',
          clientHost: '/127.0.0.1',
          assignedPartitions: [0, 2, 4],
        },
        {
          memberId: 'loyalty-worker-replica-456',
          clientId: 'loyalty-worker-replica-1',
          clientHost: '/127.0.0.1',
          assignedPartitions: [1, 3, 5],
        },
      ],
    };

    const mockInspector: StreamInspectorPort = {
      inspectTopic: async () => {
        throw new Error('Not implemented');
      },
      inspectConsumerGroupLag: async () => {
        throw new Error('Not implemented');
      },
      describeConsumerGroup: async (groupId: string, topic?: string) => {
        expect(groupId).toBe('loyalty-v1');
        expect(topic).toBe('purchase.events');
        return mockDescription;
      },
      scaleTopicPartitions: async () => {
        throw new Error('Not implemented');
      },
      peekPartitionMessages: async () => {
        throw new Error('Not implemented');
      },
      publishRawMessage: async () => {
        throw new Error('Not implemented');
      },
    };

    const handler = new DescribeConsumerGroupHandler(mockInspector);
    const result = await handler.execute(
      new DescribeConsumerGroupQuery({ groupId: 'loyalty-v1', topic: 'purchase.events' }),
    );

    expect(result).toEqual(mockDescription);
    expect(result.members).toHaveLength(2);
    expect(result.members[0].assignedPartitions).toEqual([0, 2, 4]);
    expect(result.members[1].assignedPartitions).toEqual([1, 3, 5]);
  });
});

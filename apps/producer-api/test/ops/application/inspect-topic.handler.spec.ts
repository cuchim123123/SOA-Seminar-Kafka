import { describe, expect, it } from 'vitest';
import {
  KafkaInspectorPort,
  TopicInspectionResult,
} from '../../../src/ops/application/ports/kafka-inspector.port';
import { InspectTopicHandler } from '../../../src/ops/application/queries/inspect-topic.handler';
import { InspectTopicQuery } from '../../../src/ops/application/queries/inspect-topic.query';

describe('InspectTopicHandler', () => {
  it('delegates topic inspection to KafkaInspectorPort', async () => {
    const mockResult: TopicInspectionResult = {
      topic: 'purchase.events',
      partitionCount: 6,
      partitions: [
        {
          topic: 'purchase.events',
          partition: 0,
          leader: 1,
          replicas: [1],
          isr: [1],
          logStartOffset: '0',
          logEndOffset: '10',
        },
      ],
    };

    const mockInspector: KafkaInspectorPort = {
      inspectTopic: async (topic: string) => {
        expect(topic).toBe('purchase.events');
        return mockResult;
      },
      inspectConsumerGroupLag: async () => {
        throw new Error('Not implemented');
      },
      scaleTopicPartitions: async () => {
        throw new Error('Not implemented');
      },
      peekPartitionMessages: async () => [],
      publishRawMessage: async () => ({ topic: 'purchase.events', partition: 0, offset: '0' }),
    };

    const handler = new InspectTopicHandler(mockInspector);
    const result = await handler.execute(new InspectTopicQuery({ topic: 'purchase.events' }));

    expect(result).toEqual(mockResult);
  });
});

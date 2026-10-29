import { describe, expect, it } from 'vitest';
import {
  ConsumerGroupLagResult,
  KafkaInspectorPort,
  TopicInspectionResult,
} from '../../../src/ops/application/ports/kafka-inspector.port';
import { GetConsumerGroupLagHandler } from '../../../src/ops/application/queries/get-consumer-group-lag.handler';
import { InspectTopicHandler } from '../../../src/ops/application/queries/inspect-topic.handler';
import { OpsController } from '../../../src/ops/interfaces/http/ops.controller';

describe('OpsController', () => {
  it('returns topic inspection result from handler', async () => {
    const mockResult: TopicInspectionResult = {
      topic: 'purchase.events',
      partitionCount: 6,
      partitions: [],
    };

    const mockInspector: KafkaInspectorPort = {
      inspectTopic: async () => mockResult,
      inspectConsumerGroupLag: async () => {
        throw new Error('Not implemented');
      },
    };

    const topicHandler = new InspectTopicHandler(mockInspector);
    const lagHandler = new GetConsumerGroupLagHandler(mockInspector);
    const controller = new OpsController(topicHandler, lagHandler);

    const result = await controller.inspectTopic('purchase.events');
    expect(result).toEqual(mockResult);
  });

  it('returns consumer group lag result from handler', async () => {
    const mockLagResult: ConsumerGroupLagResult = {
      groupId: 'analytics-v1',
      topic: 'purchase.events',
      totalLag: '3',
      partitions: [
        {
          topic: 'purchase.events',
          partition: 0,
          logStartOffset: '0',
          logEndOffset: '10',
          committedOffset: '7',
          lag: '3',
          isUncommitted: false,
        },
      ],
    };

    const mockInspector: KafkaInspectorPort = {
      inspectTopic: async () => {
        throw new Error('Not implemented');
      },
      inspectConsumerGroupLag: async () => mockLagResult,
    };

    const topicHandler = new InspectTopicHandler(mockInspector);
    const lagHandler = new GetConsumerGroupLagHandler(mockInspector);
    const controller = new OpsController(topicHandler, lagHandler);

    const result = await controller.getConsumerGroupLag('analytics-v1', 'purchase.events');
    expect(result).toEqual(mockLagResult);
    expect(result.totalLag).toBe('3');
  });
});

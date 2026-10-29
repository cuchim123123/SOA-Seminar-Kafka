import { describe, expect, it } from 'vitest';
import {
  BrokerMessageRecord,
  ConsumerGroupLagResult,
  KafkaInspectorPort,
  TopicInspectionResult,
} from '../../../src/ops/application/ports/kafka-inspector.port';
import { ScaleTopicPartitionsHandler } from '../../../src/ops/application/commands/scale-topic-partitions.handler';
import { GetConsumerGroupLagHandler } from '../../../src/ops/application/queries/get-consumer-group-lag.handler';
import { InspectTopicHandler } from '../../../src/ops/application/queries/inspect-topic.handler';
import { PeekPartitionMessagesHandler } from '../../../src/ops/application/queries/peek-partition-messages.handler';
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
      scaleTopicPartitions: async () => mockResult,
      peekPartitionMessages: async () => [],
    };

    const topicHandler = new InspectTopicHandler(mockInspector);
    const lagHandler = new GetConsumerGroupLagHandler(mockInspector);
    const peekHandler = new PeekPartitionMessagesHandler(mockInspector);
    const scaleHandler = new ScaleTopicPartitionsHandler(mockInspector);
    const controller = new OpsController(topicHandler, lagHandler, peekHandler, scaleHandler);

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
      scaleTopicPartitions: async () => {
        throw new Error('Not implemented');
      },
      peekPartitionMessages: async () => [],
    };

    const topicHandler = new InspectTopicHandler(mockInspector);
    const lagHandler = new GetConsumerGroupLagHandler(mockInspector);
    const peekHandler = new PeekPartitionMessagesHandler(mockInspector);
    const scaleHandler = new ScaleTopicPartitionsHandler(mockInspector);
    const controller = new OpsController(topicHandler, lagHandler, peekHandler, scaleHandler);

    const result = await controller.getConsumerGroupLag('analytics-v1', 'purchase.events');
    expect(result).toEqual(mockLagResult);
    expect(result.totalLag).toBe('3');
  });

  it('scales topic partitions through handler', async () => {
    const mockScaledResult: TopicInspectionResult = {
      topic: 'purchase.events',
      partitionCount: 8,
      partitions: [],
    };

    const mockInspector: KafkaInspectorPort = {
      inspectTopic: async () => mockScaledResult,
      inspectConsumerGroupLag: async () => {
        throw new Error('Not implemented');
      },
      scaleTopicPartitions: async (topic, count) => {
        expect(topic).toBe('purchase.events');
        expect(count).toBe(8);
        return mockScaledResult;
      },
      peekPartitionMessages: async () => [],
    };

    const topicHandler = new InspectTopicHandler(mockInspector);
    const lagHandler = new GetConsumerGroupLagHandler(mockInspector);
    const peekHandler = new PeekPartitionMessagesHandler(mockInspector);
    const scaleHandler = new ScaleTopicPartitionsHandler(mockInspector);
    const controller = new OpsController(topicHandler, lagHandler, peekHandler, scaleHandler);

    const result = await controller.scaleTopicPartitions('purchase.events', 8);
    expect(result.partitionCount).toBe(8);
  });

  it('peeks partition messages through handler', async () => {
    const mockMessages: BrokerMessageRecord[] = [
      {
        topic: 'purchase.events',
        partition: 0,
        offset: '42',
        key: 'U001',
        timestamp: '1700000000',
        payload: { eventId: 'ev-1' },
      },
    ];

    const mockInspector: KafkaInspectorPort = {
      inspectTopic: async () => {
        throw new Error('Not implemented');
      },
      inspectConsumerGroupLag: async () => {
        throw new Error('Not implemented');
      },
      scaleTopicPartitions: async () => {
        throw new Error('Not implemented');
      },
      peekPartitionMessages: async (topic, partition, limit) => {
        expect(topic).toBe('purchase.events');
        expect(partition).toBe(0);
        expect(limit).toBe(5);
        return mockMessages;
      },
    };

    const topicHandler = new InspectTopicHandler(mockInspector);
    const lagHandler = new GetConsumerGroupLagHandler(mockInspector);
    const peekHandler = new PeekPartitionMessagesHandler(mockInspector);
    const scaleHandler = new ScaleTopicPartitionsHandler(mockInspector);
    const controller = new OpsController(topicHandler, lagHandler, peekHandler, scaleHandler);

    const result = await controller.peekPartitionMessages('purchase.events', 0, '5');
    expect(result).toEqual(mockMessages);
    expect(result[0].offset).toBe('42');
  });
});

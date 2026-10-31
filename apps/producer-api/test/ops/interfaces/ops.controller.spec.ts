import { describe, expect, it } from 'vitest';
import {
  BrokerMessageRecord,
  ConsumerGroupDescriptionResult,
  ConsumerGroupLagResult,
  StreamInspectorPort,
  TopicInspectionResult,
} from '../../../src/ops/application/ports/stream-inspector.port';
import { ScaleTopicPartitionsHandler } from '../../../src/ops/application/commands/scale-topic-partitions/scale-topic-partitions.handler';
import { DescribeConsumerGroupHandler } from '../../../src/ops/application/queries/describe-consumer-group/describe-consumer-group.handler';
import { GetConsumerGroupLagHandler } from '../../../src/ops/application/queries/get-consumer-group-lag/get-consumer-group-lag.handler';
import { InspectTopicHandler } from '../../../src/ops/application/queries/inspect-topic/inspect-topic.handler';
import { PeekPartitionMessagesHandler } from '../../../src/ops/application/queries/peek-partition-messages/peek-partition-messages.handler';
import { OpsController } from '../../../src/ops/interfaces/http/ops.controller';
import { OpsDatabaseService } from '../../../src/ops/application/services/ops-database.service';

describe('OpsController', () => {
  const createMockInspector = (overrides?: Partial<StreamInspectorPort>): StreamInspectorPort => ({
    inspectTopic: async () => {
      throw new Error('Not implemented');
    },
    inspectConsumerGroupLag: async () => {
      throw new Error('Not implemented');
    },
    describeConsumerGroup: async () => {
      throw new Error('Not implemented');
    },
    scaleTopicPartitions: async () => {
      throw new Error('Not implemented');
    },
    peekPartitionMessages: async () => [],
    publishRawMessage: async () => ({ topic: 'purchase.events', partition: 0, offset: '1' }),
    ...overrides,
  });

  const createController = (inspector: StreamInspectorPort) => {
    const topicHandler = new InspectTopicHandler(inspector);
    const lagHandler = new GetConsumerGroupLagHandler(inspector);
    const describeHandler = new DescribeConsumerGroupHandler(inspector);
    const peekHandler = new PeekPartitionMessagesHandler(inspector);
    const scaleHandler = new ScaleTopicPartitionsHandler(inspector);
    const mockOpsDbService = new OpsDatabaseService();
    return new OpsController(
      topicHandler,
      lagHandler,
      describeHandler,
      peekHandler,
      scaleHandler,
      inspector,
      mockOpsDbService,
    );
  };

  it('returns topic inspection result from handler', async () => {
    const mockResult: TopicInspectionResult = {
      topic: 'purchase.events',
      partitionCount: 6,
      partitions: [],
    };

    const inspector = createMockInspector({
      inspectTopic: async () => mockResult,
    });
    const controller = createController(inspector);

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

    const inspector = createMockInspector({
      inspectConsumerGroupLag: async () => mockLagResult,
    });
    const controller = createController(inspector);

    const result = await controller.getConsumerGroupLag('analytics-v1', 'purchase.events');
    expect(result).toEqual(mockLagResult);
    expect(result.totalLag).toBe('3');
  });

  it('returns consumer group description result from handler', async () => {
    const mockDescription: ConsumerGroupDescriptionResult = {
      groupId: 'loyalty-v1',
      state: 'Stable',
      protocol: 'RoundRobinAssigner',
      protocolType: 'consumer',
      members: [
        {
          memberId: 'loyalty-worker-primary',
          clientId: 'loyalty-worker',
          clientHost: '/127.0.0.1',
          assignedPartitions: [0, 2, 4],
        },
      ],
    };

    const inspector = createMockInspector({
      describeConsumerGroup: async () => mockDescription,
    });
    const controller = createController(inspector);

    const result = await controller.describeConsumerGroup('loyalty-v1', 'purchase.events');
    expect(result).toEqual(mockDescription);
    expect(result.members[0].assignedPartitions).toEqual([0, 2, 4]);
  });

  it('scales topic partitions through handler', async () => {
    const mockScaledResult: TopicInspectionResult = {
      topic: 'purchase.events',
      partitionCount: 8,
      partitions: [],
    };

    const inspector = createMockInspector({
      scaleTopicPartitions: async (topic, count) => {
        expect(topic).toBe('purchase.events');
        expect(count).toBe(8);
        return mockScaledResult;
      },
    });
    const controller = createController(inspector);

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

    const inspector = createMockInspector({
      peekPartitionMessages: async (topic, partition, limit) => {
        expect(topic).toBe('purchase.events');
        expect(partition).toBe(0);
        expect(limit).toBe(5);
        return mockMessages;
      },
    });
    const controller = createController(inspector);

    const result = await controller.peekPartitionMessages('purchase.events', 0, '5');
    expect(result).toEqual(mockMessages);
    expect(result[0].offset).toBe('42');
  });

  it('injects poison pill and publishes raw messages', async () => {
    const inspector = createMockInspector({
      publishRawMessage: async (t, _k) => ({
        topic: t,
        partition: 2,
        offset: '99',
      }),
    });
    const controller = createController(inspector);

    const poisonRes = await controller.injectPoisonPill({ key: 'poison_custom' });
    expect(poisonRes.status).toBe('POISON_PILL_INJECTED');
    expect(poisonRes.partition).toBe(2);
    expect(poisonRes.key).toBe('poison_custom');

    const rawRes = await controller.publishRaw({ key: 'my_key', payload: 'test' });
    expect(rawRes.status).toBe('PUBLISHED_RAW');
    expect(rawRes.key).toBe('my_key');
  });
});

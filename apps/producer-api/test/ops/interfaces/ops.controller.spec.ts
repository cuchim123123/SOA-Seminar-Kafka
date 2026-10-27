import { describe, expect, it } from 'vitest';
import {
  KafkaInspectorPort,
  TopicInspectionResult,
} from '../../../src/ops/application/ports/kafka-inspector.port';
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
    };

    const handler = new InspectTopicHandler(mockInspector);
    const controller = new OpsController(handler);

    const result = await controller.inspectTopic('purchase.events');
    expect(result).toEqual(mockResult);
  });
});

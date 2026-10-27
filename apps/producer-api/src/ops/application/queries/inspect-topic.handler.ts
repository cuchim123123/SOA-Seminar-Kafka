import { Inject, Injectable } from '@nestjs/common';
import {
  KAFKA_INSPECTOR_PORT,
  KafkaInspectorPort,
  TopicInspectionResult,
} from '../ports/kafka-inspector.port';
import { InspectTopicQuery } from './inspect-topic.query';

@Injectable()
export class InspectTopicHandler {
  constructor(
    @Inject(KAFKA_INSPECTOR_PORT)
    private readonly inspector: KafkaInspectorPort,
  ) {}

  async execute(query: InspectTopicQuery): Promise<TopicInspectionResult> {
    return this.inspector.inspectTopic(query.topic);
  }
}

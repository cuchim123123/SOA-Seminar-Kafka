import { Inject, Injectable } from '@nestjs/common';
import {
  STREAM_INSPECTOR_PORT,
  StreamInspectorPort,
  TopicInspectionResult,
} from '../../ports/stream-inspector.port';
import { InspectTopicQuery } from './inspect-topic.query';

@Injectable()
export class InspectTopicHandler {
  constructor(
    @Inject(STREAM_INSPECTOR_PORT)
    private readonly inspector: StreamInspectorPort,
  ) {}

  async execute(query: InspectTopicQuery): Promise<TopicInspectionResult> {
    return this.inspector.inspectTopic(query.data.topic);
  }
}

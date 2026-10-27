import { Controller, Get, Inject, Param } from '@nestjs/common';
import { TopicInspectionResult } from '../../application/ports/kafka-inspector.port';
import { InspectTopicHandler } from '../../application/queries/inspect-topic.handler';
import { InspectTopicQuery } from '../../application/queries/inspect-topic.query';

@Controller('api/v1/ops')
export class OpsController {
  constructor(
    @Inject(InspectTopicHandler)
    private readonly inspectTopicHandler: InspectTopicHandler,
  ) {}

  @Get('kafka/topics/:topic')
  async inspectTopic(@Param('topic') topic: string): Promise<TopicInspectionResult> {
    return this.inspectTopicHandler.execute(new InspectTopicQuery(topic));
  }
}

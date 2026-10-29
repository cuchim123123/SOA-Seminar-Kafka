import { Controller, Get, Inject, Param, Query } from '@nestjs/common';
import {
  ConsumerGroupLagResult,
  TopicInspectionResult,
} from '../../application/ports/kafka-inspector.port';
import { GetConsumerGroupLagHandler } from '../../application/queries/get-consumer-group-lag.handler';
import { GetConsumerGroupLagQuery } from '../../application/queries/get-consumer-group-lag.query';
import { InspectTopicHandler } from '../../application/queries/inspect-topic.handler';
import { InspectTopicQuery } from '../../application/queries/inspect-topic.query';

@Controller('api/v1/ops')
export class OpsController {
  constructor(
    @Inject(InspectTopicHandler)
    private readonly inspectTopicHandler: InspectTopicHandler,
    @Inject(GetConsumerGroupLagHandler)
    private readonly getConsumerGroupLagHandler: GetConsumerGroupLagHandler,
  ) {}

  @Get('kafka/topics/:topic')
  async inspectTopic(@Param('topic') topic: string): Promise<TopicInspectionResult> {
    return this.inspectTopicHandler.execute(new InspectTopicQuery({ topic }));
  }

  @Get('kafka/groups/:groupId/lag')
  async getConsumerGroupLag(
    @Param('groupId') groupId: string,
    @Query('topic') topic?: string,
  ): Promise<ConsumerGroupLagResult> {
    return this.getConsumerGroupLagHandler.execute(
      new GetConsumerGroupLagQuery({ groupId, topic }),
    );
  }
}

import { Body, Controller, Get, Inject, Param, ParseIntPipe, Post, Query } from '@nestjs/common';
import {
  BrokerMessageRecord,
  ConsumerGroupLagResult,
  TopicInspectionResult,
} from '../../application/ports/kafka-inspector.port';
import { ScaleTopicPartitionsCommand } from '../../application/commands/scale-topic-partitions.command';
import { ScaleTopicPartitionsHandler } from '../../application/commands/scale-topic-partitions.handler';
import { GetConsumerGroupLagHandler } from '../../application/queries/get-consumer-group-lag.handler';
import { GetConsumerGroupLagQuery } from '../../application/queries/get-consumer-group-lag.query';
import { InspectTopicHandler } from '../../application/queries/inspect-topic.handler';
import { InspectTopicQuery } from '../../application/queries/inspect-topic.query';
import { PeekPartitionMessagesHandler } from '../../application/queries/peek-partition-messages.handler';
import { PeekPartitionMessagesQuery } from '../../application/queries/peek-partition-messages.query';

@Controller('api/v1/ops')
export class OpsController {
  constructor(
    @Inject(InspectTopicHandler)
    private readonly inspectTopicHandler: InspectTopicHandler,
    @Inject(GetConsumerGroupLagHandler)
    private readonly getConsumerGroupLagHandler: GetConsumerGroupLagHandler,
    @Inject(PeekPartitionMessagesHandler)
    private readonly peekMessagesHandler: PeekPartitionMessagesHandler,
    @Inject(ScaleTopicPartitionsHandler)
    private readonly scalePartitionsHandler: ScaleTopicPartitionsHandler,
  ) {}

  @Get('kafka/topics/:topic')
  async inspectTopic(@Param('topic') topic: string): Promise<TopicInspectionResult> {
    return this.inspectTopicHandler.execute(new InspectTopicQuery({ topic }));
  }

  @Post('kafka/topics/:topic/partitions')
  async scaleTopicPartitions(
    @Param('topic') topic: string,
    @Body('count', ParseIntPipe) count: number,
  ): Promise<TopicInspectionResult> {
    return this.scalePartitionsHandler.execute(
      new ScaleTopicPartitionsCommand({
        topic,
        newPartitionCount: count,
      }),
    );
  }

  @Get('kafka/topics/:topic/partitions/:partition/messages')
  async peekPartitionMessages(
    @Param('topic') topic: string,
    @Param('partition', ParseIntPipe) partition: number,
    @Query('limit') limitStr?: string,
  ): Promise<BrokerMessageRecord[]> {
    const limit = limitStr ? parseInt(limitStr, 10) : 10;
    return this.peekMessagesHandler.execute(
      new PeekPartitionMessagesQuery({
        topic,
        partition,
        limit,
      }),
    );
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

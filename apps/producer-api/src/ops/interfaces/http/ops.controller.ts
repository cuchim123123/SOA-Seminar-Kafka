import { Body, Controller, Get, Inject, Param, ParseIntPipe, Post, Query } from '@nestjs/common';
import {
  BrokerMessageRecord,
  ConsumerGroupLagResult,
  KAFKA_INSPECTOR_PORT,
  KafkaInspectorPort,
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
    @Inject(KAFKA_INSPECTOR_PORT)
    private readonly kafkaInspector: KafkaInspectorPort,
  ) {}

  @Post('kafka/inject-poison')
  async injectPoisonPill(
    @Body() body: { topic?: string; key?: string },
  ): Promise<{ status: string; topic: string; partition: number; offset: string; key: string }> {
    const topic = body?.topic?.trim() || 'purchase.events';
    const key = body?.key?.trim() || `poison_pill_${Date.now()}`;
    const malformedPayload = `CORRUPTED_POISON_PILL_NON_JSON_{error:true,timestamp:${Date.now()}}`;

    const res = await this.kafkaInspector.publishRawMessage(topic, key, malformedPayload);
    return {
      status: 'POISON_PILL_INJECTED',
      topic: res.topic,
      partition: res.partition,
      offset: res.offset,
      key,
    };
  }

  @Post('kafka/publish-raw')
  async publishRaw(
    @Body() body: { topic?: string; key: string; payload: string },
  ): Promise<{ status: string; topic: string; partition: number; offset: string; key: string }> {
    const topic = body?.topic?.trim() || 'purchase.events';
    const key = body?.key?.trim() || `arbitrary_key_${Date.now()}`;
    const payload =
      typeof body?.payload === 'string' ? body.payload : JSON.stringify(body?.payload ?? {});

    const res = await this.kafkaInspector.publishRawMessage(topic, key, payload);
    return {
      status: 'PUBLISHED_RAW',
      topic: res.topic,
      partition: res.partition,
      offset: res.offset,
      key,
    };
  }

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

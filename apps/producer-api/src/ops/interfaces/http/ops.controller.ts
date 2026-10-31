import { Body, Controller, Get, Inject, Param, ParseIntPipe, Post, Query } from '@nestjs/common';
import { createPurchaseCompletedEnvelope, uuidv7 } from '@lab/contracts';
import {
  BrokerMessageRecord,
  ConsumerGroupDescriptionResult,
  ConsumerGroupLagResult,
  STREAM_INSPECTOR_PORT,
  StreamInspectorPort,
  TopicInspectionResult,
} from '../../application/ports/stream-inspector.port';
import { ScaleTopicPartitionsCommand } from '../../application/commands/scale-topic-partitions/scale-topic-partitions.command';
import { ScaleTopicPartitionsHandler } from '../../application/commands/scale-topic-partitions/scale-topic-partitions.handler';
import { DescribeConsumerGroupHandler } from '../../application/queries/describe-consumer-group/describe-consumer-group.handler';
import { DescribeConsumerGroupQuery } from '../../application/queries/describe-consumer-group/describe-consumer-group.query';
import { GetConsumerGroupLagHandler } from '../../application/queries/get-consumer-group-lag/get-consumer-group-lag.handler';
import { GetConsumerGroupLagQuery } from '../../application/queries/get-consumer-group-lag/get-consumer-group-lag.query';
import { InspectTopicHandler } from '../../application/queries/inspect-topic/inspect-topic.handler';
import { InspectTopicQuery } from '../../application/queries/inspect-topic/inspect-topic.query';
import { PeekPartitionMessagesHandler } from '../../application/queries/peek-partition-messages/peek-partition-messages.handler';
import { PeekPartitionMessagesQuery } from '../../application/queries/peek-partition-messages/peek-partition-messages.query';
import {
  OpsDatabaseService,
  OutboxStatusResult,
  RollbackTestResult,
} from '../../application/services/ops-database.service';

@Controller('api/v1/ops')
export class OpsController {
  constructor(
    @Inject(InspectTopicHandler)
    private readonly inspectTopicHandler: InspectTopicHandler,
    @Inject(GetConsumerGroupLagHandler)
    private readonly getConsumerGroupLagHandler: GetConsumerGroupLagHandler,
    @Inject(DescribeConsumerGroupHandler)
    private readonly describeConsumerGroupHandler: DescribeConsumerGroupHandler,
    @Inject(PeekPartitionMessagesHandler)
    private readonly peekMessagesHandler: PeekPartitionMessagesHandler,
    @Inject(ScaleTopicPartitionsHandler)
    private readonly scalePartitionsHandler: ScaleTopicPartitionsHandler,
    @Inject(STREAM_INSPECTOR_PORT)
    private readonly streamInspector: StreamInspectorPort,
    @Inject(OpsDatabaseService)
    private readonly opsDbService: OpsDatabaseService,
  ) {}

  @Post('kafka/inject-poison')
  async injectPoisonPill(
    @Body() body: { topic?: string; key?: string },
  ): Promise<{ status: string; topic: string; partition: number; offset: string; key: string }> {
    const topic = body?.topic?.trim() || 'purchase.events';
    const key = body?.key?.trim() || `poison_pill_${Date.now()}`;
    const malformedPayload = `CORRUPTED_POISON_PILL_NON_JSON_{error:true,timestamp:${Date.now()}}`;

    const res = await this.streamInspector.publishRawMessage(topic, key, malformedPayload);
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

    const res = await this.streamInspector.publishRawMessage(topic, key, payload);
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

  @Get('kafka/groups/:groupId/description')
  async describeConsumerGroup(
    @Param('groupId') groupId: string,
    @Query('topic') topic?: string,
  ): Promise<ConsumerGroupDescriptionResult> {
    return this.describeConsumerGroupHandler.execute(
      new DescribeConsumerGroupQuery({ groupId, topic }),
    );
  }

  @Get('outbox')
  async getOutboxStatus(): Promise<OutboxStatusResult> {
    return this.opsDbService.getOutboxStatus();
  }

  @Post('outbox/test-rollback')
  async testOutboxRollback(): Promise<RollbackTestResult> {
    return this.opsDbService.testOutboxRollback();
  }

  @Post('kafka/test-duplicate')
  async testConsumerDuplicate(
    @Body()
    body: {
      eventId?: string;
      customerId?: string;
      purchaseId?: string;
      amountCents?: number;
      customerSequence?: number;
      correlationId?: string;
    },
  ): Promise<{
    status: string;
    topic: string;
    partition: number;
    offset: string;
    key: string;
    eventId: string;
    note: string;
  }> {
    const customerId = body?.customerId?.trim() || 'U001';
    const amountCents = body?.amountCents || 4999;
    const customerSequence = body?.customerSequence || 1;
    const eventId = body?.eventId?.trim() || uuidv7();
    const purchaseId = body?.purchaseId?.trim() || `pur_${eventId}`;
    const correlationId = body?.correlationId?.trim() || uuidv7();

    const envelope = createPurchaseCompletedEnvelope({
      eventId,
      customerId,
      purchaseId,
      amountCents,
      customerSequence,
      correlationId,
    });

    const res = await this.streamInspector.publishRawMessage(
      'purchase.events',
      customerId,
      JSON.stringify(envelope),
    );

    return {
      status: 'DUPLICATE_EVENT_INJECTED',
      topic: res.topic,
      partition: res.partition,
      offset: res.offset,
      key: customerId,
      eventId,
      note: 'Duplicate Kafka record published directly to broker partition. Consumer must detect existing eventId in processed_events, record zero incremental points, and commit offset N+1.',
    };
  }
}

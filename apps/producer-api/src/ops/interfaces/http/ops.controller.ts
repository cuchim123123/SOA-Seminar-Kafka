import {
  Body,
  Controller,
  Get,
  Inject,
  Optional,
  Param,
  ParseIntPipe,
  Post,
  Query,
} from '@nestjs/common';
import { createPurchaseCompletedEnvelope, uuidv7 } from '@lab/contracts';
import { PrismaService } from '../../../database/prisma.service';
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
    @Optional()
    @Inject(PrismaService)
    private readonly prisma?: PrismaService,
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
  async getOutboxStatus(): Promise<{
    totalCount: number;
    events: Array<{
      eventId: string;
      aggregateId: string;
      customerSequence: string;
      eventType: string;
      payload: unknown;
      status: string;
      attemptCount: number;
      createdAt: string;
      publishedAt: string | null;
    }>;
    recentIdempotencyKeys: Array<{
      customerId: string;
      idempotencyKey: string;
      requestFingerprint: string;
      purchaseId: string;
    }>;
  }> {
    if (!this.prisma) {
      return { totalCount: 0, events: [], recentIdempotencyKeys: [] };
    }

    const [totalCount, events, recentKeys] = await Promise.all([
      this.prisma.outboxEvent.count(),
      this.prisma.outboxEvent.findMany({
        take: 30,
        orderBy: { createdAt: 'desc' },
      }),
      this.prisma.httpIdempotencyKey.findMany({
        take: 15,
      }),
    ]);

    return {
      totalCount,
      events: events.map((e) => ({
        eventId: e.eventId,
        aggregateId: e.aggregateId,
        customerSequence: e.customerSequence.toString(),
        eventType: e.eventType,
        payload: e.payloadJson,
        status: e.status,
        attemptCount: e.attemptCount,
        createdAt: e.createdAt.toISOString(),
        publishedAt: e.publishedAt?.toISOString() ?? null,
      })),
      recentIdempotencyKeys: recentKeys.map((k) => ({
        customerId: k.customerId,
        idempotencyKey: k.idempotencyKey,
        requestFingerprint: k.requestFingerprint,
        purchaseId: k.purchaseId,
      })),
    };
  }

  @Post('outbox/test-rollback')
  async testOutboxRollback(): Promise<{
    status: string;
    simulatedFailureReason: string;
    testCustomerId: string;
    testPurchaseId: string;
    persistedOutboxRows: number;
    persistedPurchases: number;
    dualWritePrevented: boolean;
    verificationDetails: string;
  }> {
    if (!this.prisma) {
      throw new Error('Database service not available');
    }

    const testCustomerId = `ROLLBACK_TEST_${Date.now()}`;
    const testPurchaseId = `pur_fail_${uuidv7().slice(0, 8)}`;
    const testEventId = uuidv7();

    let caughtError = false;
    try {
      await this.prisma.$transaction(async (tx) => {
        // 1. Upsert customer
        await tx.purchaseCustomer.upsert({
          where: { customerId: testCustomerId },
          create: { customerId: testCustomerId, nextEventSequence: 1n },
          update: {},
        });

        // 2. Insert test purchase
        await tx.purchase.create({
          data: {
            purchaseId: testPurchaseId,
            customerId: testCustomerId,
            amountCents: 9999n,
            status: 'PENDING_CONFIRMATION',
            completionSequence: 1n,
          },
        });

        // 3. Insert test outbox event
        await tx.outboxEvent.create({
          data: {
            eventId: testEventId,
            aggregateId: testCustomerId,
            customerSequence: 1n,
            eventType: 'PurchaseCompleted',
            payloadJson: {
              testRollback: true,
              purchaseId: testPurchaseId,
              amountCents: 9999,
            },
            status: 'PENDING',
          },
        });

        // 4. Intentional transactional failure: simulates server crash / constraint error
        throw new Error('SIMULATED_TRANSACTION_FAILURE_BEFORE_COMMIT');
      });
    } catch {
      caughtError = true;
    }

    // Explicitly query database to prove 0 records were committed
    const [persistedOutboxRows, persistedPurchases] = await Promise.all([
      this.prisma.outboxEvent.count({ where: { aggregateId: testCustomerId } }),
      this.prisma.purchase.count({ where: { purchaseId: testPurchaseId } }),
    ]);

    const dualWritePrevented = caughtError && persistedOutboxRows === 0 && persistedPurchases === 0;

    return {
      status: dualWritePrevented ? 'ROLLED_BACK_ATOMIC_SUCCESS' : 'ROLLBACK_FAILED',
      simulatedFailureReason:
        'Deliberate runtime exception thrown before unit-of-work commit to test transaction atomicity.',
      testCustomerId,
      testPurchaseId,
      persistedOutboxRows,
      persistedPurchases,
      dualWritePrevented,
      verificationDetails: dualWritePrevented
        ? 'Postgres transactional isolation successfully rolled back all changes. Zero outbox events were persisted, ensuring Debezium CDC and Kafka brokers receive NO corrupted or phantom records.'
        : 'Warning: transaction did not roll back cleanly.',
    };
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

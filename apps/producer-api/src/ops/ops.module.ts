import { Module } from '@nestjs/common';
import { STREAM_INSPECTOR_PORT } from './application/ports/stream-inspector.port';
import { ScaleTopicPartitionsHandler } from './application/commands/scale-topic-partitions/scale-topic-partitions.handler';
import { DescribeConsumerGroupHandler } from './application/queries/describe-consumer-group/describe-consumer-group.handler';
import { GetConsumerGroupLagHandler } from './application/queries/get-consumer-group-lag/get-consumer-group-lag.handler';
import { InspectTopicHandler } from './application/queries/inspect-topic/inspect-topic.handler';
import { PeekPartitionMessagesHandler } from './application/queries/peek-partition-messages/peek-partition-messages.handler';
import { KafkaJsInspectorAdapter } from './infrastructure/adapters/kafkajs-inspector.adapter';
import { OpsController } from './interfaces/http/ops.controller';

import { OpsDatabaseService } from './application/services/ops-database.service';

@Module({
  controllers: [OpsController],
  providers: [
    InspectTopicHandler,
    GetConsumerGroupLagHandler,
    DescribeConsumerGroupHandler,
    PeekPartitionMessagesHandler,
    ScaleTopicPartitionsHandler,
    OpsDatabaseService,
    {
      provide: STREAM_INSPECTOR_PORT,
      useClass: KafkaJsInspectorAdapter,
    },
  ],
  exports: [
    InspectTopicHandler,
    GetConsumerGroupLagHandler,
    DescribeConsumerGroupHandler,
    PeekPartitionMessagesHandler,
    ScaleTopicPartitionsHandler,
    OpsDatabaseService,
    STREAM_INSPECTOR_PORT,
  ],
})
export class OpsModule {}

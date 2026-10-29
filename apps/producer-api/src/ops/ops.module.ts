import { Module } from '@nestjs/common';
import { KAFKA_INSPECTOR_PORT } from './application/ports/kafka-inspector.port';
import { ScaleTopicPartitionsHandler } from './application/commands/scale-topic-partitions.handler';
import { GetConsumerGroupLagHandler } from './application/queries/get-consumer-group-lag.handler';
import { InspectTopicHandler } from './application/queries/inspect-topic.handler';
import { PeekPartitionMessagesHandler } from './application/queries/peek-partition-messages.handler';
import { KafkaJsInspectorAdapter } from './infrastructure/adapters/kafkajs-inspector.adapter';
import { OpsController } from './interfaces/http/ops.controller';

@Module({
  controllers: [OpsController],
  providers: [
    InspectTopicHandler,
    GetConsumerGroupLagHandler,
    PeekPartitionMessagesHandler,
    ScaleTopicPartitionsHandler,
    {
      provide: KAFKA_INSPECTOR_PORT,
      useClass: KafkaJsInspectorAdapter,
    },
  ],
  exports: [
    InspectTopicHandler,
    GetConsumerGroupLagHandler,
    PeekPartitionMessagesHandler,
    ScaleTopicPartitionsHandler,
    KAFKA_INSPECTOR_PORT,
  ],
})
export class OpsModule {}

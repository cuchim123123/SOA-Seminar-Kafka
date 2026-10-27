import { Module } from '@nestjs/common';
import { KAFKA_INSPECTOR_PORT } from './application/ports/kafka-inspector.port';
import { InspectTopicHandler } from './application/queries/inspect-topic.handler';
import { KafkaJsInspectorAdapter } from './infrastructure/adapters/kafkajs-inspector.adapter';
import { OpsController } from './interfaces/http/ops.controller';

@Module({
  controllers: [OpsController],
  providers: [
    InspectTopicHandler,
    {
      provide: KAFKA_INSPECTOR_PORT,
      useClass: KafkaJsInspectorAdapter,
    },
  ],
  exports: [InspectTopicHandler, KAFKA_INSPECTOR_PORT],
})
export class OpsModule {}

import { Module } from '@nestjs/common';
import { ApplySalesEventHandler } from './application/commands/apply-sales-event.handler';
import { PauseAnalyticsConsumerHandler } from './application/commands/pause-analytics-consumer.handler';
import { ResumeAnalyticsConsumerHandler } from './application/commands/resume-analytics-consumer.handler';
import { KAFKA_CONSUMER_CONTROL_PORT } from './application/ports/kafka-consumer-control.port';
import { SALES_SUMMARY_READ_DAO_PORT } from './application/ports/sales-summary-read-dao.port';
import { SALES_SUMMARY_REPOSITORY_PORT } from './application/ports/sales-summary-repository.port';
import { GetConsumerControlStatusHandler } from './application/queries/get-consumer-control-status.handler';
import { GetSalesSummaryHandler } from './application/queries/get-sales-summary.handler';
import { InMemorySalesSummaryRepositoryAdapter } from './infrastructure/adapters/in-memory-sales-summary-repository.adapter';
import { KafkaAnalyticsConsumerAdapter } from './infrastructure/adapters/kafka-analytics-consumer.adapter';
import { AnalyticsConsumerControlController } from './interfaces/http/analytics-consumer-control.controller';
import { AnalyticsController } from './interfaces/http/analytics.controller';

@Module({
  controllers: [AnalyticsController, AnalyticsConsumerControlController],
  providers: [
    ApplySalesEventHandler,
    GetSalesSummaryHandler,
    PauseAnalyticsConsumerHandler,
    ResumeAnalyticsConsumerHandler,
    GetConsumerControlStatusHandler,
    InMemorySalesSummaryRepositoryAdapter,
    {
      provide: SALES_SUMMARY_REPOSITORY_PORT,
      useExisting: InMemorySalesSummaryRepositoryAdapter,
    },
    {
      provide: SALES_SUMMARY_READ_DAO_PORT,
      useExisting: InMemorySalesSummaryRepositoryAdapter,
    },
    KafkaAnalyticsConsumerAdapter,
    {
      provide: KAFKA_CONSUMER_CONTROL_PORT,
      useExisting: KafkaAnalyticsConsumerAdapter,
    },
  ],
  exports: [
    ApplySalesEventHandler,
    GetSalesSummaryHandler,
    PauseAnalyticsConsumerHandler,
    ResumeAnalyticsConsumerHandler,
    GetConsumerControlStatusHandler,
    SALES_SUMMARY_REPOSITORY_PORT,
    SALES_SUMMARY_READ_DAO_PORT,
    KAFKA_CONSUMER_CONTROL_PORT,
  ],
})
export class AnalyticsModule {}

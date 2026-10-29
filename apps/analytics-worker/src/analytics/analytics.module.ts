import { Module } from '@nestjs/common';
import { ApplySalesEventHandler } from './application/commands/apply-sales-event.handler';
import { PauseAnalyticsConsumerHandler } from './application/commands/pause-analytics-consumer.handler';
import { ResumeAnalyticsConsumerHandler } from './application/commands/resume-analytics-consumer.handler';
import { ANALYTICS_UNIT_OF_WORK_PORT } from './application/ports/analytics-unit-of-work.port';
import { CONSUMER_CONTROL_PORT } from './application/ports/consumer-control.port';
import { SALES_SUMMARY_READ_DAO_PORT } from './application/ports/sales-summary-read-dao.port';
import { SALES_SUMMARY_REPOSITORY_PORT } from './application/ports/sales-summary-repository.port';
import { GetConsumerControlStatusHandler } from './application/queries/get-consumer-control-status.handler';
import { GetSalesSummaryHandler } from './application/queries/get-sales-summary.handler';
import { KafkaAnalyticsConsumerAdapter } from './infrastructure/adapters/kafka-analytics-consumer.adapter';
import { PostgresAnalyticsUnitOfWorkAdapter } from './infrastructure/adapters/postgres-analytics-unit-of-work.adapter';
import { PostgresSalesSummaryReadDaoAdapter } from './infrastructure/adapters/postgres-sales-summary-read-dao.adapter';
import { PostgresSalesSummaryRepositoryAdapter } from './infrastructure/adapters/postgres-sales-summary-repository.adapter';
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
    PostgresSalesSummaryRepositoryAdapter,
    {
      provide: SALES_SUMMARY_REPOSITORY_PORT,
      useExisting: PostgresSalesSummaryRepositoryAdapter,
    },
    PostgresAnalyticsUnitOfWorkAdapter,
    {
      provide: ANALYTICS_UNIT_OF_WORK_PORT,
      useExisting: PostgresAnalyticsUnitOfWorkAdapter,
    },
    PostgresSalesSummaryReadDaoAdapter,
    {
      provide: SALES_SUMMARY_READ_DAO_PORT,
      useExisting: PostgresSalesSummaryReadDaoAdapter,
    },
    KafkaAnalyticsConsumerAdapter,
    {
      provide: CONSUMER_CONTROL_PORT,
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
    ANALYTICS_UNIT_OF_WORK_PORT,
    SALES_SUMMARY_READ_DAO_PORT,
    CONSUMER_CONTROL_PORT,
  ],
})
export class AnalyticsModule {}

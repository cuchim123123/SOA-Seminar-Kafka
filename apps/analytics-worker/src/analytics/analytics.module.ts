import { EVENT_RECEIPT_READ_PORT } from './application/ports/event-receipt-read.port';
import { GetEventReceiptsHandler } from './application/queries/get-event-receipts.handler';
import { PostgresEventReceiptReadAdapter } from './infrastructure/adapters/postgres-event-receipt-read.adapter';
import { EventReceiptsController } from './interfaces/http/event-receipts.controller';
import { Module } from '@nestjs/common';
import { ApplySalesEventHandler } from './application/commands/apply-sales-event';
import { PauseAnalyticsConsumerHandler } from './application/commands/pause-analytics-consumer';
import { ResumeAnalyticsConsumerHandler } from './application/commands/resume-analytics-consumer';
import { ANALYTICS_UNIT_OF_WORK_PORT } from './application/ports/analytics-unit-of-work.port';
import { CONSUMER_CONTROL_PORT } from './application/ports/consumer-control.port';
import { SALES_SUMMARY_READ_DAO_PORT } from './application/ports/sales-summary-read-dao.port';
import { SALES_SUMMARY_REPOSITORY_PORT } from './application/ports/sales-summary-repository.port';
import { GetConsumerControlStatusHandler } from './application/queries/get-consumer-control-status';
import { GetSalesSummaryHandler } from './application/queries/get-sales-summary';
import { KafkaAnalyticsConsumerAdapter } from './infrastructure/adapters/kafka-analytics-consumer.adapter';
import { PostgresAnalyticsUnitOfWorkAdapter } from './infrastructure/adapters/postgres-analytics-unit-of-work.adapter';
import { PostgresSalesSummaryReadDaoAdapter } from './infrastructure/adapters/postgres-sales-summary-read-dao.adapter';
import { PostgresSalesSummaryRepositoryAdapter } from './infrastructure/adapters/postgres-sales-summary-repository.adapter';
import { AnalyticsConsumerControlController } from './interfaces/http/analytics-consumer-control.controller';
import { AnalyticsController } from './interfaces/http/analytics.controller';

@Module({
  controllers: [EventReceiptsController, AnalyticsController, AnalyticsConsumerControlController],
  providers: [
    GetEventReceiptsHandler,
    { provide: EVENT_RECEIPT_READ_PORT, useClass: PostgresEventReceiptReadAdapter },
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

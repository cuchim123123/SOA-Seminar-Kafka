import { Module } from '@nestjs/common';
import { ApplySalesEventHandler } from './application/commands/apply-sales-event.handler';
import { SALES_SUMMARY_READ_DAO_PORT } from './application/ports/sales-summary-read-dao.port';
import { SALES_SUMMARY_REPOSITORY_PORT } from './application/ports/sales-summary-repository.port';
import { GetSalesSummaryHandler } from './application/queries/get-sales-summary.handler';
import { InMemorySalesSummaryRepositoryAdapter } from './infrastructure/adapters/in-memory-sales-summary-repository.adapter';
import { KafkaAnalyticsConsumerAdapter } from './infrastructure/adapters/kafka-analytics-consumer.adapter';
import { AnalyticsController } from './interfaces/http/analytics.controller';

@Module({
  controllers: [AnalyticsController],
  providers: [
    ApplySalesEventHandler,
    GetSalesSummaryHandler,
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
  ],
  exports: [
    ApplySalesEventHandler,
    GetSalesSummaryHandler,
    SALES_SUMMARY_REPOSITORY_PORT,
    SALES_SUMMARY_READ_DAO_PORT,
  ],
})
export class AnalyticsModule {}

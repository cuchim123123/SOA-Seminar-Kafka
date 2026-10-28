import { Controller, Get, Inject } from '@nestjs/common';
import { GetSalesSummaryHandler } from '../../application/queries/get-sales-summary.handler';
import { GetSalesSummaryQuery } from '../../application/queries/get-sales-summary.query';
import { SalesSummaryResponseDto } from './dto/sales-summary-response.dto';

@Controller('api/v1/analytics')
export class AnalyticsController {
  constructor(
    @Inject(GetSalesSummaryHandler)
    private readonly getSalesSummaryHandler: GetSalesSummaryHandler,
  ) {}

  @Get('summary')
  async getSummary(): Promise<SalesSummaryResponseDto> {
    const summary = await this.getSalesSummaryHandler.execute(new GetSalesSummaryQuery({}));

    return {
      totalRevenueCents: summary.totalRevenueCents,
      totalRevenueDollars: (summary.totalRevenueCents / 100).toFixed(2),
      purchaseCount: summary.purchaseCount,
      uniqueCustomerCount: summary.uniqueCustomerCount,
      lastEventId: summary.lastEventId,
      lastProcessedAt: summary.lastProcessedAt,
    };
  }
}

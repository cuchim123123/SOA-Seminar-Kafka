import { Inject, Injectable } from '@nestjs/common';
import {
  ANALYTICS_UNIT_OF_WORK_PORT,
  AnalyticsUnitOfWorkPort,
} from '../ports/analytics-unit-of-work.port';
import { ApplySalesEventCommand, ApplySalesEventResult } from './apply-sales-event.command';

@Injectable()
export class ApplySalesEventHandler {
  constructor(
    @Inject(ANALYTICS_UNIT_OF_WORK_PORT)
    private readonly unitOfWork: AnalyticsUnitOfWorkPort,
  ) {}

  async execute(command: ApplySalesEventCommand): Promise<ApplySalesEventResult> {
    const { amountCents, customerId, eventId } = command.data;

    return await this.unitOfWork.execute(async (ctx) => {
      // 1. Consumer idempotency check: attempt to insert into processed_events for 'analytics-v1'
      const isNew = await ctx.idempotencyRepository.markProcessedIfNew('analytics-v1', eventId);

      if (!isNew) {
        // Idempotent duplicate event detected! Do not mutate aggregate revenue.
        const summary = await ctx.salesSummaryRepository.get();
        return {
          eventId,
          totalRevenueCents: summary.totalRevenueCents,
          purchaseCount: summary.purchaseCount,
          uniqueCustomerCount: summary.uniqueCustomerCount,
          isDuplicate: true,
        };
      }

      // 2. Fetch locked summary aggregate and apply purchase
      const summary = await ctx.salesSummaryRepository.get();
      summary.applyPurchase(amountCents, customerId, eventId);
      await ctx.salesSummaryRepository.save(summary);

      return {
        eventId,
        totalRevenueCents: summary.totalRevenueCents,
        purchaseCount: summary.purchaseCount,
        uniqueCustomerCount: summary.uniqueCustomerCount,
        isDuplicate: false,
      };
    });
  }
}

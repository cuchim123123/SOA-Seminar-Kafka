import { Inject, Injectable } from '@nestjs/common';
import {
  SALES_SUMMARY_REPOSITORY_PORT,
  SalesSummaryRepositoryPort,
} from '../ports/sales-summary-repository.port';
import { ApplySalesEventCommand, ApplySalesEventResult } from './apply-sales-event.command';

@Injectable()
export class ApplySalesEventHandler {
  constructor(
    @Inject(SALES_SUMMARY_REPOSITORY_PORT)
    private readonly repository: SalesSummaryRepositoryPort,
  ) {}

  async execute(command: ApplySalesEventCommand): Promise<ApplySalesEventResult> {
    const { amountCents, customerId, eventId } = command.data;

    const summary = await this.repository.get();
    summary.applyPurchase(amountCents, customerId, eventId);
    await this.repository.save(summary);

    return {
      eventId,
      totalRevenueCents: summary.totalRevenueCents,
      purchaseCount: summary.purchaseCount,
      uniqueCustomerCount: summary.uniqueCustomerCount,
    };
  }
}

import { describe, expect, it, vi } from 'vitest';
import { ApplySalesEventCommand } from '../../../src/analytics/application/commands/apply-sales-event.command';
import { ApplySalesEventHandler } from '../../../src/analytics/application/commands/apply-sales-event.handler';
import { SalesSummaryRepositoryPort } from '../../../src/analytics/application/ports/sales-summary-repository.port';
import { SalesSummary } from '../../../src/analytics/domain/sales-summary.entity';

describe('ApplySalesEventHandler', () => {
  it('loads summary, applies purchase event, and persists back to repository', async () => {
    const summary = SalesSummary.create();
    const mockRepo: SalesSummaryRepositoryPort = {
      get: vi.fn().mockResolvedValue(summary),
      save: vi.fn().mockResolvedValue(undefined),
    };

    const handler = new ApplySalesEventHandler(mockRepo);
    const command = new ApplySalesEventCommand({
      eventId: 'evt-100',
      customerId: 'CUST-A',
      purchaseId: 'pur-1',
      amountCents: 15000,
      customerSequence: 1,
      correlationId: 'corr-1',
    });

    const result = await handler.execute(command);

    expect(mockRepo.get).toHaveBeenCalledTimes(1);
    expect(mockRepo.save).toHaveBeenCalledTimes(1);
    expect(result).toEqual({
      eventId: 'evt-100',
      totalRevenueCents: 15000,
      purchaseCount: 1,
      uniqueCustomerCount: 1,
    });
  });
});

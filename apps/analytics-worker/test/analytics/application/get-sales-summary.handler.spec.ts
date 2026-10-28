import { describe, expect, it, vi } from 'vitest';
import {
  SalesSummaryReadDaoPort,
  SalesSummaryReadModel,
} from '../../../src/analytics/application/ports/sales-summary-read-dao.port';
import { GetSalesSummaryHandler } from '../../../src/analytics/application/queries/get-sales-summary.handler';
import { GetSalesSummaryQuery } from '../../../src/analytics/application/queries/get-sales-summary.query';

describe('GetSalesSummaryHandler', () => {
  it('penetrates storage directly via Read DAO without touching domain repository', async () => {
    const mockReadModel: SalesSummaryReadModel = {
      totalRevenueCents: 45000,
      purchaseCount: 4,
      uniqueCustomerCount: 2,
      lastEventId: 'evt-4',
      lastProcessedAt: '2026-10-28T12:00:00.000Z',
    };

    const mockReadDao: SalesSummaryReadDaoPort = {
      getSummary: vi.fn().mockResolvedValue(mockReadModel),
    };

    const handler = new GetSalesSummaryHandler(mockReadDao);
    const query = new GetSalesSummaryQuery({});
    const result = await handler.execute(query);

    expect(mockReadDao.getSummary).toHaveBeenCalledTimes(1);
    expect(result).toEqual(mockReadModel);
  });
});

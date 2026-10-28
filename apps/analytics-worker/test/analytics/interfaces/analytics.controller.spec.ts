import { describe, expect, it, vi } from 'vitest';
import { GetSalesSummaryHandler } from '../../../src/analytics/application/queries/get-sales-summary.handler';
import { AnalyticsController } from '../../../src/analytics/interfaces/http/analytics.controller';

describe('AnalyticsController', () => {
  it('returns formatted sales summary DTO with dollars representation', async () => {
    const mockHandler = {
      execute: vi.fn().mockResolvedValue({
        totalRevenueCents: 12550,
        purchaseCount: 3,
        uniqueCustomerCount: 2,
        lastEventId: 'evt-3',
        lastProcessedAt: '2026-10-28T12:00:00.000Z',
      }),
    } as unknown as GetSalesSummaryHandler;

    const controller = new AnalyticsController(mockHandler);
    const result = await controller.getSummary();

    expect(result).toEqual({
      totalRevenueCents: 12550,
      totalRevenueDollars: '125.50',
      purchaseCount: 3,
      uniqueCustomerCount: 2,
      lastEventId: 'evt-3',
      lastProcessedAt: '2026-10-28T12:00:00.000Z',
    });
  });
});

import { describe, expect, it, vi } from 'vitest';
import { ApplySalesEventCommand } from '../../../src/analytics/application/commands/apply-sales-event.command';
import { ApplySalesEventHandler } from '../../../src/analytics/application/commands/apply-sales-event.handler';
import {
  AnalyticsUnitOfWorkContext,
  AnalyticsUnitOfWorkPort,
} from '../../../src/analytics/application/ports/analytics-unit-of-work.port';
import { ConsumerIdempotencyRepositoryPort } from '../../../src/analytics/application/ports/consumer-idempotency-repository.port';
import { SalesSummaryRepositoryPort } from '../../../src/analytics/application/ports/sales-summary-repository.port';
import { SalesSummary } from '../../../src/analytics/domain/sales-summary.entity';

describe('ApplySalesEventHandler', () => {
  function createMockUow(
    mockRepo: SalesSummaryRepositoryPort,
    mockIdempotency: ConsumerIdempotencyRepositoryPort,
  ): AnalyticsUnitOfWorkPort {
    return {
      execute: async <T>(work: (ctx: AnalyticsUnitOfWorkContext) => Promise<T>): Promise<T> => {
        return await work({
          salesSummaryRepository: mockRepo,
          idempotencyRepository: mockIdempotency,
        });
      },
    };
  }

  it('loads summary, applies purchase event, and persists back to repository within UoW', async () => {
    const summary = SalesSummary.create();
    const mockRepo: SalesSummaryRepositoryPort = {
      get: vi.fn().mockResolvedValue(summary),
      save: vi.fn().mockResolvedValue(undefined),
    };
    const mockIdempotency: ConsumerIdempotencyRepositoryPort = {
      markProcessedIfNew: vi.fn().mockResolvedValue(true),
    };

    const handler = new ApplySalesEventHandler(createMockUow(mockRepo, mockIdempotency));
    const command = new ApplySalesEventCommand({
      eventId: 'evt-100',
      customerId: 'CUST-A',
      purchaseId: 'pur-1',
      amountCents: 15000,
      customerSequence: 1,
      correlationId: 'corr-1',
    });

    const result = await handler.execute(command);

    expect(mockIdempotency.markProcessedIfNew).toHaveBeenCalledWith('analytics-v1', 'evt-100');
    expect(mockRepo.get).toHaveBeenCalledTimes(1);
    expect(mockRepo.save).toHaveBeenCalledTimes(1);
    expect(result).toEqual({
      eventId: 'evt-100',
      totalRevenueCents: 15000,
      purchaseCount: 1,
      uniqueCustomerCount: 1,
      isDuplicate: false,
    });
  });

  it('skips duplicate event idempotently without updating revenue or calling save', async () => {
    const summary = SalesSummary.create();
    summary.applyPurchase(15000, 'CUST-A', 'evt-100');

    const mockRepo: SalesSummaryRepositoryPort = {
      get: vi.fn().mockResolvedValue(summary),
      save: vi.fn().mockResolvedValue(undefined),
    };
    const mockIdempotency: ConsumerIdempotencyRepositoryPort = {
      // Simulate duplicate event
      markProcessedIfNew: vi.fn().mockResolvedValue(false),
    };

    const handler = new ApplySalesEventHandler(createMockUow(mockRepo, mockIdempotency));
    const command = new ApplySalesEventCommand({
      eventId: 'evt-100',
      customerId: 'CUST-A',
      purchaseId: 'pur-1',
      amountCents: 15000,
      customerSequence: 1,
      correlationId: 'corr-1',
    });

    const result = await handler.execute(command);

    expect(mockIdempotency.markProcessedIfNew).toHaveBeenCalledWith('analytics-v1', 'evt-100');
    expect(mockRepo.save).not.toHaveBeenCalled();
    expect(result).toEqual({
      eventId: 'evt-100',
      totalRevenueCents: 15000,
      purchaseCount: 1,
      uniqueCustomerCount: 1,
      isDuplicate: true,
    });
  });
});

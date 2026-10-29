import { describe, expect, it, vi } from 'vitest';
import { PrismaClient } from '@prisma/client-analytics';
import { PrismaService } from '../../../src/database/prisma.service';
import { SalesSummary } from '../../../src/analytics/domain/sales-summary.entity';
import { PostgresAnalyticsUnitOfWorkAdapter } from '../../../src/analytics/infrastructure/adapters/postgres-analytics-unit-of-work.adapter';
import { PostgresConsumerIdempotencyRepositoryAdapter } from '../../../src/analytics/infrastructure/adapters/postgres-consumer-idempotency-repository.adapter';
import { PostgresSalesSummaryReadDaoAdapter } from '../../../src/analytics/infrastructure/adapters/postgres-sales-summary-read-dao.adapter';
import { PostgresSalesSummaryRepositoryAdapter } from '../../../src/analytics/infrastructure/adapters/postgres-sales-summary-repository.adapter';

describe('Postgres Analytics Adapters', () => {
  describe('PostgresConsumerIdempotencyRepositoryAdapter', () => {
    it('returns true when insert succeeds (new event)', async () => {
      const mockPrisma = {
        $queryRaw: vi.fn(async () => [{ event_id: 'evt_new_1' }]),
      } as unknown as PrismaClient;

      const adapter = new PostgresConsumerIdempotencyRepositoryAdapter(mockPrisma);
      const isNew = await adapter.markProcessedIfNew('analytics-v1', 'evt_new_1');

      expect(isNew).toBe(true);
      expect(mockPrisma.$queryRaw).toHaveBeenCalled();
    });

    it('returns false when insert results in conflict (duplicate event)', async () => {
      const mockPrisma = {
        $queryRaw: vi.fn(async () => []),
      } as unknown as PrismaClient;

      const adapter = new PostgresConsumerIdempotencyRepositoryAdapter(mockPrisma);
      const isNew = await adapter.markProcessedIfNew('analytics-v1', 'evt_dup_1');

      expect(isNew).toBe(false);
    });
  });

  describe('PostgresSalesSummaryRepositoryAdapter', () => {
    it('get returns reconstituted domain aggregate when record exists', async () => {
      const mockPrisma = {
        $queryRaw: vi.fn(async () => [
          {
            id: 'global',
            total_revenue_cents: 100_000n,
            purchase_count: 5n,
            customer_ids: ['CUST_1', 'CUST_2'],
            last_event_id: 'evt_5',
            last_processed_at: new Date('2026-10-29T10:00:00Z'),
          },
        ]),
      } as unknown as PrismaClient;

      const adapter = new PostgresSalesSummaryRepositoryAdapter(mockPrisma);
      const summary = await adapter.get();

      expect(summary.totalRevenueCents).toBe(100_000);
      expect(summary.purchaseCount).toBe(5);
      expect(summary.uniqueCustomerCount).toBe(2);
      expect(summary.lastEventId).toBe('evt_5');
    });

    it('get initializes aggregate if record does not exist', async () => {
      const mockPrisma = {
        $queryRaw: vi.fn(async () => []),
        analyticsSummary: {
          upsert: vi.fn(async () => ({})),
        },
      } as unknown as PrismaClient;

      const adapter = new PostgresSalesSummaryRepositoryAdapter(mockPrisma);
      const summary = await adapter.get();

      expect(summary.totalRevenueCents).toBe(0);
      expect(summary.purchaseCount).toBe(0);
      expect(mockPrisma.analyticsSummary.upsert).toHaveBeenCalled();
    });

    it('save upserts analytics summary with BigInt conversions', async () => {
      const mockPrisma = {
        analyticsSummary: {
          upsert: vi.fn(async () => ({})),
        },
      } as unknown as PrismaClient;

      const adapter = new PostgresSalesSummaryRepositoryAdapter(mockPrisma);
      const summary = SalesSummary.create();
      summary.applyPurchase(15000, 'CUST_1', 'evt_1');

      await adapter.save(summary);

      expect(mockPrisma.analyticsSummary.upsert).toHaveBeenCalledWith({
        where: { id: 'global' },
        create: {
          id: 'global',
          totalRevenueCents: 15000n,
          purchaseCount: 1n,
          customerIds: ['CUST_1'],
          lastEventId: 'evt_1',
          lastProcessedAt: expect.any(Date),
        },
        update: {
          totalRevenueCents: 15000n,
          purchaseCount: 1n,
          customerIds: ['CUST_1'],
          lastEventId: 'evt_1',
          lastProcessedAt: expect.any(Date),
        },
      });
    });
  });

  describe('PostgresSalesSummaryReadDaoAdapter', () => {
    it('returns flat read model projection directly from DB', async () => {
      const mockPrisma = {
        analyticsSummary: {
          findUnique: vi.fn(async () => ({
            id: 'global',
            totalRevenueCents: 50_000n,
            purchaseCount: 3n,
            customerIds: ['CUST_1', 'CUST_2', 'CUST_3'],
            lastEventId: 'evt_3',
            lastProcessedAt: new Date('2026-10-29T11:00:00Z'),
          })),
        },
      } as unknown as PrismaService;

      const adapter = new PostgresSalesSummaryReadDaoAdapter(mockPrisma);
      const result = await adapter.getSummary();

      expect(result).toEqual({
        totalRevenueCents: 50_000,
        purchaseCount: 3,
        uniqueCustomerCount: 3,
        lastEventId: 'evt_3',
        lastProcessedAt: '2026-10-29T11:00:00.000Z',
      });
    });

    it('returns empty defaults if no summary record found', async () => {
      const mockPrisma = {
        analyticsSummary: {
          findUnique: vi.fn(async () => null),
        },
      } as unknown as PrismaService;

      const adapter = new PostgresSalesSummaryReadDaoAdapter(mockPrisma);
      const result = await adapter.getSummary();

      expect(result).toEqual({
        totalRevenueCents: 0,
        purchaseCount: 0,
        uniqueCustomerCount: 0,
        lastEventId: null,
        lastProcessedAt: null,
      });
    });
  });

  describe('PostgresAnalyticsUnitOfWorkAdapter', () => {
    it('wraps execution inside $transaction', async () => {
      const mockTx = {
        $queryRaw: vi.fn(),
        analyticsSummary: { upsert: vi.fn() },
      };
      const mockPrisma = {
        $transaction: vi.fn(async (cb: (tx: unknown) => Promise<unknown>) => {
          return await cb(mockTx);
        }),
      } as unknown as PrismaService;

      const uow = new PostgresAnalyticsUnitOfWorkAdapter(mockPrisma);
      const res = await uow.execute(async (ctx) => {
        expect(ctx.salesSummaryRepository).toBeDefined();
        expect(ctx.idempotencyRepository).toBeDefined();
        return 'success';
      });

      expect(res).toBe('success');
      expect(mockPrisma.$transaction).toHaveBeenCalled();
    });
  });
});

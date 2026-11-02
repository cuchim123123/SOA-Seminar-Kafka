import { describe, expect, it, vi } from 'vitest';
import { PrismaService } from '../../src/database/prisma.service';
import { PostgresPurchaseHistoryAdapter } from '../../src/purchases/infrastructure/adapters/postgres-purchase-history.adapter';
import {
  purchaseHistoryQuerySchema,
  PurchaseHistoryController,
} from '../../src/purchases/interfaces/http/purchase-history.controller';
import { ListPurchasesHandler } from '../../src/purchases/application/queries/list-purchases.handler';

describe('purchase history', () => {
  it('validates bounded pagination and rejects unexpected query fields', () => {
    expect(purchaseHistoryQuerySchema.parse({})).toEqual({ limit: 25 });
    for (const query of [
      { limit: 101 },
      { limit: 0 },
      { limit: 2.5 },
      { customerId: '' },
      { cursor: '' },
      { sort: 'anything' },
    ]) {
      expect(purchaseHistoryQuerySchema.safeParse(query).success).toBe(false);
    }
  });
  it('dispatches a source-only read, preserves integer precision and distinct invalidation identity', async () => {
    const row = {
      purchaseId: 'p2',
      customerId: 'c',
      amountCents: 9007199254740993n,
      status: 'INVALIDATED',
      createdAt: new Date('2026-11-02T00:00:00Z'),
      completionSequence: 9007199254740993n,
      invalidationSequence: 9007199254740994n,
      invalidationEventId: 'invalidated',
      invalidationReason: 'test',
    };
    const prisma = {
      purchase: { findMany: vi.fn().mockResolvedValue([row, { ...row, purchaseId: 'p1' }]) },
      outboxEvent: {
        findMany: vi
          .fn()
          .mockResolvedValue([
            { aggregateId: 'c', customerSequence: row.completionSequence, eventId: 'completed' },
          ]),
      },
    };
    const adapter = new PostgresPurchaseHistoryAdapter(prisma as unknown as PrismaService);
    const controller = new PurchaseHistoryController(new ListPurchasesHandler(adapter));
    const result = await controller.list({ customerId: 'c', limit: 1 });
    expect(result.nextCursor).toBe('p2');
    expect(result.items).toHaveLength(1);
    expect(result.items[0]).toMatchObject({
      amountCents: '9007199254740993',
      completionSequence: '9007199254740993',
      invalidationSequence: '9007199254740994',
      completionEventId: 'completed',
      invalidationEventId: 'invalidated',
    });
    expect(prisma.purchase.findMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { customerId: 'c' },
        take: 2,
        orderBy: [{ createdAt: 'desc' }, { purchaseId: 'desc' }],
      }),
    );
  });
  it('rejects a cursor belonging to a different customer without querying history', async () => {
    const prisma = {
      purchase: {
        findUnique: vi.fn().mockResolvedValue({ customerId: 'other' }),
        findMany: vi.fn(),
      },
    };
    const adapter = new PostgresPurchaseHistoryAdapter(prisma as unknown as PrismaService);
    await expect(adapter.list({ customerId: 'c', cursor: 'p', limit: 25 })).rejects.toThrow(
      'another customer',
    );
    expect(prisma.purchase.findMany).not.toHaveBeenCalled();
  });
  it('uses a stored-row cursor, skips its anchor and does not invent an event ID', async () => {
    const prisma = {
      purchase: {
        findUnique: vi.fn().mockResolvedValue({ customerId: 'c' }),
        findMany: vi.fn().mockResolvedValue([]),
      },
      outboxEvent: { findMany: vi.fn() },
    };
    const adapter = new PostgresPurchaseHistoryAdapter(prisma as unknown as PrismaService);
    expect(await adapter.list({ customerId: 'c', cursor: 'p', limit: 25 })).toEqual({
      items: [],
      nextCursor: null,
    });
    expect(prisma.purchase.findMany).toHaveBeenCalledWith(
      expect.objectContaining({ cursor: { purchaseId: 'p' }, skip: 1 }),
    );
    expect(prisma.outboxEvent.findMany).not.toHaveBeenCalled();
  });
});

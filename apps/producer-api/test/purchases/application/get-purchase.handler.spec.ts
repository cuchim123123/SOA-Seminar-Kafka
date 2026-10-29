import { describe, expect, it, vi } from 'vitest';
import {
  PurchaseReadDaoPort,
  PurchaseReadModel,
} from '../../../src/purchases/application/ports/purchase-read-dao.port';
import { GetPurchaseHandler } from '../../../src/purchases/application/queries/get-purchase.handler';
import { GetPurchaseQuery } from '../../../src/purchases/application/queries/get-purchase.query';

describe('GetPurchaseHandler', () => {
  it('returns purchase read model when found via read DAO', async () => {
    const fixture: PurchaseReadModel = {
      purchaseId: 'pur_123',
      customerId: 'CUST_A',
      amountCents: 4500,
      status: 'COMPLETED',
      customerSequence: 1,
      createdAt: '2026-10-29T12:00:00.000Z',
      outboxStatus: 'PUBLISHED',
      publishedAt: '2026-10-29T12:00:01.000Z',
      eventId: 'evt_001',
    };

    const mockReadDao: PurchaseReadDaoPort = {
      findById: vi.fn(async () => fixture),
    };

    const handler = new GetPurchaseHandler(mockReadDao);
    const result = await handler.execute(new GetPurchaseQuery({ purchaseId: 'pur_123' }));

    expect(mockReadDao.findById).toHaveBeenCalledWith('pur_123');
    expect(result).toEqual(fixture);
  });

  it('returns null when purchase is not found', async () => {
    const mockReadDao: PurchaseReadDaoPort = {
      findById: vi.fn(async () => null),
    };

    const handler = new GetPurchaseHandler(mockReadDao);
    const result = await handler.execute(new GetPurchaseQuery({ purchaseId: 'pur_missing' }));

    expect(mockReadDao.findById).toHaveBeenCalledWith('pur_missing');
    expect(result).toBeNull();
  });
});

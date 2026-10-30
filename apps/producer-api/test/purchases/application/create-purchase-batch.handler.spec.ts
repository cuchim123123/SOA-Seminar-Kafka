import { describe, expect, it, vi } from 'vitest';
import { CreatePurchaseBatchCommand } from '../../../src/purchases/application/commands/create-purchase-batch.command';
import { CreatePurchaseBatchHandler } from '../../../src/purchases/application/commands/create-purchase-batch.handler';
import { OutboxRecord } from '../../../src/purchases/application/ports/outbox-repository.port';
import {
  UnitOfWorkContext,
  UnitOfWorkPort,
} from '../../../src/purchases/application/ports/unit-of-work.port';
import { Purchase } from '../../../src/purchases/domain/purchase.entity';

describe('CreatePurchaseBatchHandler', () => {
  it('creates batch purchases across default customers with outbox events inside unit of work', async () => {
    const savedPurchases: Purchase[] = [];
    const savedOutboxRecords: OutboxRecord[] = [];

    const mockUnitOfWork: UnitOfWorkPort = {
      execute: async <T>(work: (ctx: UnitOfWorkContext) => Promise<T>): Promise<T> => {
        let seq = 1;
        return work({
          purchaseRepository: {
            save: vi.fn(async (p: Purchase) => {
              savedPurchases.push(p);
            }),
            findById: vi.fn(),
          },
          outboxRepository: {
            save: vi.fn(async (r: OutboxRecord) => {
              savedOutboxRecords.push(r);
            }),
            findPending: vi.fn(),
            markPublished: vi.fn(),
            markFailed: vi.fn(),
          },
          sequenceGenerator: {
            nextSequence: vi.fn(async () => seq++),
          },
          idempotencyRepository: {
            find: vi.fn(),
            save: vi.fn(),
          },
        });
      },
    };

    const handler = new CreatePurchaseBatchHandler(mockUnitOfWork);
    const result = await handler.execute(
      new CreatePurchaseBatchCommand({
        count: 10,
      }),
    );

    expect(result.totalCreated).toBe(10);
    expect(savedPurchases).toHaveLength(10);
    expect(savedOutboxRecords).toHaveLength(10);
    expect(Object.keys(result.customerDistribution).length).toBeGreaterThan(1);
    expect(result.purchases[0].purchaseId).toBeDefined();
    expect(result.purchases[0].customerSequence).toBe(1);
  });

  it('concentrates on single customer when customerId is specified', async () => {
    const savedPurchases: Purchase[] = [];
    const mockUnitOfWork: UnitOfWorkPort = {
      execute: async <T>(work: (ctx: UnitOfWorkContext) => Promise<T>): Promise<T> => {
        let seq = 1;
        return work({
          purchaseRepository: {
            save: vi.fn(async (p: Purchase) => {
              savedPurchases.push(p);
            }),
            findById: vi.fn(),
          },
          outboxRepository: {
            save: vi.fn(),
            findPending: vi.fn(),
            markPublished: vi.fn(),
            markFailed: vi.fn(),
          },
          sequenceGenerator: {
            nextSequence: vi.fn(async () => seq++),
          },
          idempotencyRepository: {
            find: vi.fn(),
            save: vi.fn(),
          },
        });
      },
    };

    const handler = new CreatePurchaseBatchHandler(mockUnitOfWork);
    const result = await handler.execute(
      new CreatePurchaseBatchCommand({
        count: 5,
        customerId: 'cust_hot_spot',
      }),
    );

    expect(result.totalCreated).toBe(5);
    expect(result.customerDistribution['cust_hot_spot']).toBe(5);
    expect(savedPurchases.every((p) => p.customerId === 'cust_hot_spot')).toBe(true);
  });
});

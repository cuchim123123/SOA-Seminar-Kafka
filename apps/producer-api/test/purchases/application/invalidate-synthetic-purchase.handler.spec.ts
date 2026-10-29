import { describe, expect, it, vi } from 'vitest';
import { NotFoundException } from '@nestjs/common';
import { InvalidateSyntheticPurchaseCommand } from '../../../src/purchases/application/commands/invalidate-synthetic-purchase.command';
import { InvalidateSyntheticPurchaseHandler } from '../../../src/purchases/application/commands/invalidate-synthetic-purchase.handler';
import {
  OutboxRecord,
  OutboxRepositoryPort,
} from '../../../src/purchases/application/ports/outbox-repository.port';
import { PurchaseRepositoryPort } from '../../../src/purchases/application/ports/purchase-repository.port';
import { SequenceGeneratorPort } from '../../../src/purchases/application/ports/sequence-generator.port';
import {
  UnitOfWorkContext,
  UnitOfWorkPort,
} from '../../../src/purchases/application/ports/unit-of-work.port';
import { Purchase } from '../../../src/purchases/domain/purchase.entity';

describe('InvalidateSyntheticPurchaseHandler with Unit of Work and Outbox', () => {
  it('atomically invalidates purchase, advances customer sequence, and appends PurchaseInvalidated outbox record', async () => {
    const existingPurchase = Purchase.create({
      purchaseId: 'pur_test_101',
      customerId: 'CUST_U001',
      amountCents: 12000,
      customerSequence: 1,
      status: 'COMPLETED',
    });

    let savedPurchase: Purchase | undefined;
    let savedOutboxRecord: OutboxRecord | undefined;

    const mockPurchaseRepo: PurchaseRepositoryPort = {
      save: vi.fn(async (p: Purchase): Promise<void> => {
        savedPurchase = p;
      }),
      findById: vi.fn(async (id: string): Promise<Purchase | null> => {
        if (id === 'pur_test_101') return existingPurchase;
        return null;
      }),
    };

    const mockOutboxRepo: OutboxRepositoryPort = {
      save: vi.fn(async (record: OutboxRecord): Promise<void> => {
        savedOutboxRecord = record;
      }),
      findPending: vi.fn(),
      markPublished: vi.fn(),
      markFailed: vi.fn(),
    };

    const mockSequenceGenerator: SequenceGeneratorPort = {
      nextSequence: vi.fn(async (_customerId: string): Promise<number> => 4),
    };

    const mockIdempotencyRepo = {
      find: vi.fn(async () => null),
      save: vi.fn(async () => {}),
    };

    const executeSpy = vi.fn();
    const mockUnitOfWork: UnitOfWorkPort = {
      execute: async <T>(work: (ctx: UnitOfWorkContext) => Promise<T>): Promise<T> => {
        executeSpy();
        return work({
          purchaseRepository: mockPurchaseRepo,
          outboxRepository: mockOutboxRepo,
          sequenceGenerator: mockSequenceGenerator,
          idempotencyRepository: mockIdempotencyRepo,
        });
      },
    };

    const handler = new InvalidateSyntheticPurchaseHandler(mockUnitOfWork);

    const command = new InvalidateSyntheticPurchaseCommand({
      purchaseId: 'pur_test_101',
      reason: 'FRAUD_CONFIRMED',
      correlationId: '0192d478-f7ef-7b33-912a-3652d3a95d77',
    });

    const result = await handler.execute(command);

    const UUIDV7_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

    // Unit of work invoked
    expect(executeSpy).toHaveBeenCalledTimes(1);
    expect(mockPurchaseRepo.findById).toHaveBeenCalledWith('pur_test_101', { forUpdate: true });
    expect(mockSequenceGenerator.nextSequence).toHaveBeenCalledWith('CUST_U001');

    // Return value
    expect(result.purchaseId).toBe('pur_test_101');
    expect(result.customerId).toBe('CUST_U001');
    expect(result.status).toBe('INVALIDATED');
    expect(result.customerSequence).toBe(4);
    expect(result.reason).toBe('FRAUD_CONFIRMED');
    expect(result.isAlreadyInvalidated).toBe(false);
    expect(result.eventId).toMatch(UUIDV7_REGEX);

    // Purchase entity state updated and saved
    expect(mockPurchaseRepo.save).toHaveBeenCalledTimes(1);
    expect(savedPurchase).toBeDefined();
    expect(savedPurchase?.status).toBe('INVALIDATED');
    expect(savedPurchase?.purchaseId).toBe('pur_test_101');
    expect(savedPurchase?.invalidationSequence).toBe(4);
    expect(savedPurchase?.invalidationEventId).toBe(result.eventId);
    expect(savedPurchase?.invalidationReason).toBe('FRAUD_CONFIRMED');

    // Outbox record appended
    expect(mockOutboxRepo.save).toHaveBeenCalledTimes(1);
    expect(savedOutboxRecord).toBeDefined();
    expect(savedOutboxRecord?.eventId).toBe(result.eventId);
    expect(savedOutboxRecord?.aggregateId).toBe('CUST_U001');
    expect(savedOutboxRecord?.customerSequence).toBe(4);
    expect(savedOutboxRecord?.status).toBe('PENDING');
    expect(savedOutboxRecord?.envelope.eventType).toBe('PurchaseInvalidated');
    expect(savedOutboxRecord?.envelope.aggregateId).toBe('CUST_U001');
    expect(savedOutboxRecord?.envelope.correlationId).toBe('0192d478-f7ef-7b33-912a-3652d3a95d77');
    expect(savedOutboxRecord?.envelope.payload).toEqual({
      purchaseId: 'pur_test_101',
      customerId: 'CUST_U001',
      reason: 'FRAUD_CONFIRMED',
    });
  });

  it('idempotently returns existing invalidation sequence and event ID when purchase is already INVALIDATED', async () => {
    const alreadyInvalidatedPurchase = Purchase.create({
      purchaseId: 'pur_already_inv',
      customerId: 'CUST_U001',
      amountCents: 5000,
      customerSequence: 1, // original completion sequence was 1
      status: 'INVALIDATED',
      invalidationSequence: 4, // invalidation assigned sequence 4
      invalidationEventId: '0192d478-f7ef-7b33-912a-3652d3a95d77',
      invalidationReason: 'FRAUD_CONFIRMED',
    });

    const mockPurchaseRepo: PurchaseRepositoryPort = {
      save: vi.fn(),
      findById: vi.fn(async () => alreadyInvalidatedPurchase),
    };

    const mockOutboxRepo: OutboxRepositoryPort = {
      save: vi.fn(),
      findPending: vi.fn(),
      markPublished: vi.fn(),
      markFailed: vi.fn(),
    };

    const mockSequenceGenerator: SequenceGeneratorPort = {
      nextSequence: vi.fn(),
    };

    const mockIdempotencyRepo = {
      find: vi.fn(async () => null),
      save: vi.fn(async () => {}),
    };

    const mockUnitOfWork: UnitOfWorkPort = {
      execute: async <T>(work: (ctx: UnitOfWorkContext) => Promise<T>): Promise<T> => {
        return work({
          purchaseRepository: mockPurchaseRepo,
          outboxRepository: mockOutboxRepo,
          sequenceGenerator: mockSequenceGenerator,
          idempotencyRepository: mockIdempotencyRepo,
        });
      },
    };

    const handler = new InvalidateSyntheticPurchaseHandler(mockUnitOfWork);

    const result = await handler.execute(
      new InvalidateSyntheticPurchaseCommand({
        purchaseId: 'pur_already_inv',
        reason: 'DEMO_INVALIDATION',
      }),
    );

    expect(result.purchaseId).toBe('pur_already_inv');
    expect(result.status).toBe('INVALIDATED');
    expect(result.isAlreadyInvalidated).toBe(true);
    expect(result.customerSequence).toBe(4); // Must return the invalidation sequence (4), NOT completion sequence (1)
    expect(result.eventId).toBe('0192d478-f7ef-7b33-912a-3652d3a95d77'); // Must return the original invalidation event ID
    expect(result.reason).toBe('FRAUD_CONFIRMED');
    expect(mockPurchaseRepo.findById).toHaveBeenCalledWith('pur_already_inv', { forUpdate: true });
    expect(mockSequenceGenerator.nextSequence).not.toHaveBeenCalled();
    expect(mockPurchaseRepo.save).not.toHaveBeenCalled();
    expect(mockOutboxRepo.save).not.toHaveBeenCalled();
  });

  it('throws NotFoundException when purchase does not exist', async () => {
    const mockPurchaseRepo: PurchaseRepositoryPort = {
      save: vi.fn(),
      findById: vi.fn(async () => null),
    };

    const mockOutboxRepo: OutboxRepositoryPort = {
      save: vi.fn(),
      findPending: vi.fn(),
      markPublished: vi.fn(),
      markFailed: vi.fn(),
    };

    const mockSequenceGenerator: SequenceGeneratorPort = {
      nextSequence: vi.fn(),
    };

    const mockIdempotencyRepo = {
      find: vi.fn(async () => null),
      save: vi.fn(async () => {}),
    };

    const mockUnitOfWork: UnitOfWorkPort = {
      execute: async <T>(work: (ctx: UnitOfWorkContext) => Promise<T>): Promise<T> => {
        return work({
          purchaseRepository: mockPurchaseRepo,
          outboxRepository: mockOutboxRepo,
          sequenceGenerator: mockSequenceGenerator,
          idempotencyRepository: mockIdempotencyRepo,
        });
      },
    };

    const handler = new InvalidateSyntheticPurchaseHandler(mockUnitOfWork);

    await expect(
      handler.execute(
        new InvalidateSyntheticPurchaseCommand({
          purchaseId: 'non_existent_id',
        }),
      ),
    ).rejects.toThrow(NotFoundException);
  });
});

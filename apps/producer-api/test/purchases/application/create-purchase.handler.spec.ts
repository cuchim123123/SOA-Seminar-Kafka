import { describe, expect, it, vi } from 'vitest';
import { CreatePurchaseCommand } from '../../../src/purchases/application/commands/create-purchase.command';
import { CreatePurchaseHandler } from '../../../src/purchases/application/commands/create-purchase.handler';
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

describe('CreatePurchaseHandler with Unit of Work and Outbox', () => {
  it('atomically persists purchase and outbox record within a Unit of Work transaction', async () => {
    let savedPurchase: Purchase | undefined;
    let savedOutboxRecord: OutboxRecord | undefined;

    const mockPurchaseRepo: PurchaseRepositoryPort = {
      save: vi.fn(async (purchase: Purchase): Promise<void> => {
        savedPurchase = purchase;
      }),
      findById: vi.fn(),
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
      nextSequence: vi.fn(async (_customerId: string): Promise<number> => 1),
    };

    const executeSpy = vi.fn();
    const mockUnitOfWork: UnitOfWorkPort = {
      execute: async <T>(work: (ctx: UnitOfWorkContext) => Promise<T>): Promise<T> => {
        executeSpy();
        return work({
          purchaseRepository: mockPurchaseRepo,
          outboxRepository: mockOutboxRepo,
          sequenceGenerator: mockSequenceGenerator,
        });
      },
    };

    const handler = new CreatePurchaseHandler(mockUnitOfWork);

    const command = new CreatePurchaseCommand('CUST_U001', 3500);
    const result = await handler.execute(command);

    const UUIDV7_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

    // Unit of work called
    expect(executeSpy).toHaveBeenCalledTimes(1);
    expect(mockSequenceGenerator.nextSequence).toHaveBeenCalledWith('CUST_U001');

    // Return value shape (pure command result, zero Kafka transport metadata)
    expect(result.customerId).toBe('CUST_U001');
    expect(result.amountCents).toBe(3500);
    expect(result.customerSequence).toBe(1);
    expect(result.purchaseId).toMatch(
      /^pur_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
    expect(result.eventId).toMatch(UUIDV7_REGEX);
    expect(result.correlationId).toMatch(UUIDV7_REGEX);

    // Purchase entity persisted in UoW
    expect(mockPurchaseRepo.save).toHaveBeenCalledTimes(1);
    expect(savedPurchase).toBeDefined();
    expect(savedPurchase?.purchaseId).toBe(result.purchaseId);
    expect(savedPurchase?.customerId).toBe('CUST_U001');
    expect(savedPurchase?.amountCents).toBe(3500);
    expect(savedPurchase?.customerSequence).toBe(1);

    // Outbox record appended in UoW
    expect(mockOutboxRepo.save).toHaveBeenCalledTimes(1);
    expect(savedOutboxRecord).toBeDefined();
    expect(savedOutboxRecord?.eventId).toBe(result.eventId);
    expect(savedOutboxRecord?.aggregateId).toBe('CUST_U001');
    expect(savedOutboxRecord?.customerSequence).toBe(1);
    expect(savedOutboxRecord?.status).toBe('PENDING');
    expect(savedOutboxRecord?.envelope.schemaVersion).toBe(1);
    expect(savedOutboxRecord?.envelope.eventType).toBe('PurchaseCompleted');
    expect(savedOutboxRecord?.envelope.aggregateId).toBe('CUST_U001');
    expect(savedOutboxRecord?.envelope.payload).toEqual({
      purchaseId: result.purchaseId,
      customerId: 'CUST_U001',
      amountCents: 3500,
    });
  });

  it('preserves provided correlationId and idempotencyKey across outbox envelope and result', async () => {
    const fixedCorrelationId = '01a18192-9c94-7d7b-952b-02265a63bac4';
    const idempotencyKey = 'client-idempotency-key-001';

    let savedOutboxRecord: OutboxRecord | undefined;

    const mockPurchaseRepo: PurchaseRepositoryPort = {
      save: vi.fn(),
      findById: vi.fn(),
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
      nextSequence: vi.fn(async () => 3),
    };

    const mockUnitOfWork: UnitOfWorkPort = {
      execute: async <T>(work: (ctx: UnitOfWorkContext) => Promise<T>): Promise<T> => {
        return work({
          purchaseRepository: mockPurchaseRepo,
          outboxRepository: mockOutboxRepo,
          sequenceGenerator: mockSequenceGenerator,
        });
      },
    };

    const handler = new CreatePurchaseHandler(mockUnitOfWork);

    const command = new CreatePurchaseCommand(
      'CUST_U002',
      1200,
      fixedCorrelationId,
      idempotencyKey,
    );
    const result = await handler.execute(command);

    expect(result.correlationId).toBe(fixedCorrelationId);
    expect(result.idempotencyKey).toBe(idempotencyKey);
    expect(result.customerSequence).toBe(3);
    expect(savedOutboxRecord?.envelope.correlationId).toBe(fixedCorrelationId);
  });
});

import { describe, expect, it, vi } from 'vitest';
import { ApplyPurchaseEventCommand } from '../../../src/loyalty/application/commands/apply-purchase-event.command';
import { ApplyPurchaseEventHandler } from '../../../src/loyalty/application/commands/apply-purchase-event.handler';
import { ConsumerIdempotencyRepositoryPort } from '../../../src/loyalty/application/ports/consumer-idempotency-repository.port';
import { LoyaltyAccountRepositoryPort } from '../../../src/loyalty/application/ports/loyalty-account-repository.port';
import {
  LoyaltyUnitOfWorkContext,
  LoyaltyUnitOfWorkPort,
} from '../../../src/loyalty/application/ports/loyalty-unit-of-work.port';
import { LoyaltyAccount } from '../../../src/loyalty/domain/loyalty-account.entity';

describe('ApplyPurchaseEventHandler', () => {
  function createMockUow(
    mockRepo: LoyaltyAccountRepositoryPort,
    mockIdempotency: ConsumerIdempotencyRepositoryPort,
  ): LoyaltyUnitOfWorkPort {
    return {
      execute: async <T>(work: (ctx: LoyaltyUnitOfWorkContext) => Promise<T>): Promise<T> => {
        return await work({
          loyaltyRepository: mockRepo,
          idempotencyRepository: mockIdempotency,
        });
      },
    };
  }

  it('loads existing account, validates sequence, calculates points, and persists updated state', async () => {
    const existingAccount = LoyaltyAccount.create('U001');
    existingAccount.applyPurchase(12_000, 1); // 12 pts, Silver, sequence 1

    const mockRepo: LoyaltyAccountRepositoryPort = {
      findByCustomerId: vi.fn(async () => existingAccount),
      save: vi.fn(async () => {}),
    };
    const mockIdempotency: ConsumerIdempotencyRepositoryPort = {
      markProcessedIfNew: vi.fn(async () => true),
    };

    const handler = new ApplyPurchaseEventHandler(createMockUow(mockRepo, mockIdempotency));
    const command = new ApplyPurchaseEventCommand({
      eventId: 'evt_002',
      customerId: 'U001',
      purchaseId: 'pur_002',
      amountCents: 20_000,
      customerSequence: 2,
      correlationId: 'corr_002',
    });

    const result = await handler.execute(command);

    expect(mockIdempotency.markProcessedIfNew).toHaveBeenCalledWith('loyalty-v1', 'evt_002');
    expect(mockRepo.findByCustomerId).toHaveBeenCalledWith('U001');
    expect(mockRepo.save).toHaveBeenCalledWith(existingAccount);

    expect(result.pointsEarned).toBe(40);
    expect(result.currentTier).toBe('GOLD');
    expect(result.totalPoints).toBe(52);
    expect(result.isDuplicate).toBe(false);
  });

  it('creates new account on first purchase for a customer', async () => {
    let savedAccount: LoyaltyAccount | undefined;
    const mockRepo: LoyaltyAccountRepositoryPort = {
      findByCustomerId: vi.fn(async () => null),
      save: vi.fn(async (account: LoyaltyAccount) => {
        savedAccount = account;
      }),
    };
    const mockIdempotency: ConsumerIdempotencyRepositoryPort = {
      markProcessedIfNew: vi.fn(async () => true),
    };

    const handler = new ApplyPurchaseEventHandler(createMockUow(mockRepo, mockIdempotency));
    const command = new ApplyPurchaseEventCommand({
      eventId: 'evt_001',
      customerId: 'U002',
      purchaseId: 'pur_001',
      amountCents: 5_000,
      customerSequence: 1,
      correlationId: 'corr_001',
    });

    const result = await handler.execute(command);

    expect(result.customerId).toBe('U002');
    expect(result.pointsEarned).toBe(5);
    expect(result.currentTier).toBe('BRONZE');
    expect(result.totalPoints).toBe(5);
    expect(result.isDuplicate).toBe(false);
    expect(savedAccount).toBeDefined();
    expect(savedAccount?.customerId).toBe('U002');
  });

  it('skips duplicate event idempotently without updating state or points', async () => {
    const existingAccount = LoyaltyAccount.reconstitute({
      customerId: 'U001',
      tier: 'SILVER',
      totalEligibleSpendCents: 12_000,
      pointsBalance: 12,
      lastCustomerSequence: 1,
      correctionRequired: false,
    });

    const mockRepo: LoyaltyAccountRepositoryPort = {
      findByCustomerId: vi.fn(async () => existingAccount),
      save: vi.fn(async () => {}),
    };
    const mockIdempotency: ConsumerIdempotencyRepositoryPort = {
      // Simulate duplicate: event already exists in processed_events
      markProcessedIfNew: vi.fn(async () => false),
    };

    const handler = new ApplyPurchaseEventHandler(createMockUow(mockRepo, mockIdempotency));
    const command = new ApplyPurchaseEventCommand({
      eventId: 'evt_001', // duplicate
      customerId: 'U001',
      purchaseId: 'pur_001',
      amountCents: 12_000,
      customerSequence: 1,
      correlationId: 'corr_001',
    });

    const result = await handler.execute(command);

    expect(result.isDuplicate).toBe(true);
    expect(result.pointsEarned).toBe(0);
    expect(result.totalPoints).toBe(12);
    expect(mockRepo.save).not.toHaveBeenCalled();
  });

  it('throws sequence gap error on out-of-order event', async () => {
    const existingAccount = LoyaltyAccount.create('U001');
    existingAccount.applyPurchase(10_000, 1); // sequence 1

    const mockRepo: LoyaltyAccountRepositoryPort = {
      findByCustomerId: vi.fn(async () => existingAccount),
      save: vi.fn(async () => {}),
    };
    const mockIdempotency: ConsumerIdempotencyRepositoryPort = {
      markProcessedIfNew: vi.fn(async () => true),
    };

    const handler = new ApplyPurchaseEventHandler(createMockUow(mockRepo, mockIdempotency));
    const command = new ApplyPurchaseEventCommand({
      eventId: 'evt_003',
      customerId: 'U001',
      purchaseId: 'pur_003',
      amountCents: 10_000,
      customerSequence: 3, // gap: expected 2
      correlationId: 'corr_003',
    });

    await expect(handler.execute(command)).rejects.toThrowError(
      /Sequence gap or out-of-order event for customer U001: expected 2, received 3/,
    );
    expect(mockRepo.save).not.toHaveBeenCalled();
  });
});

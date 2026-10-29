import { describe, expect, it, vi } from 'vitest';
import { ApplyPurchaseInvalidationCommand } from '../../../src/loyalty/application/commands/apply-purchase-invalidation.command';
import { ApplyPurchaseInvalidationHandler } from '../../../src/loyalty/application/commands/apply-purchase-invalidation.handler';
import { ConsumerIdempotencyRepositoryPort } from '../../../src/loyalty/application/ports/consumer-idempotency-repository.port';
import { LoyaltyAccountRepositoryPort } from '../../../src/loyalty/application/ports/loyalty-account-repository.port';
import {
  LoyaltyUnitOfWorkContext,
  LoyaltyUnitOfWorkPort,
} from '../../../src/loyalty/application/ports/loyalty-unit-of-work.port';
import { LoyaltyAccount } from '../../../src/loyalty/domain/loyalty-account.entity';

describe('ApplyPurchaseInvalidationHandler', () => {
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

  it('marks correction_required = true and updates sequence without mutating points balance', async () => {
    const existingAccount = LoyaltyAccount.reconstitute({
      customerId: 'U001',
      tier: 'GOLD',
      totalEligibleSpendCents: 42_000,
      pointsBalance: 82,
      lastCustomerSequence: 3,
      correctionRequired: false,
    });

    const mockRepo: LoyaltyAccountRepositoryPort = {
      findByCustomerId: vi.fn(async () => existingAccount),
      save: vi.fn(async () => {}),
    };
    const mockIdempotency: ConsumerIdempotencyRepositoryPort = {
      markProcessedIfNew: vi.fn(async () => true),
    };

    const handler = new ApplyPurchaseInvalidationHandler(createMockUow(mockRepo, mockIdempotency));
    const command = new ApplyPurchaseInvalidationCommand({
      eventId: 'evt_inv_001',
      customerId: 'U001',
      purchaseId: 'P001',
      customerSequence: 4,
      correlationId: 'corr_inv_001',
      reason: 'Refund',
    });

    const result = await handler.execute(command);

    expect(mockIdempotency.markProcessedIfNew).toHaveBeenCalledWith('loyalty-v1', 'evt_inv_001');
    expect(mockRepo.findByCustomerId).toHaveBeenCalledWith('U001');
    expect(mockRepo.save).toHaveBeenCalledWith(existingAccount);

    expect(result.correctionRequired).toBe(true);
    expect(result.lastCustomerSequence).toBe(4);
    expect(result.isDuplicate).toBe(false);

    // Points balance and tier remain intact in the live projection
    expect(existingAccount.pointsBalance).toBe(82);
    expect(existingAccount.tier).toBe('GOLD');
    expect(existingAccount.correctionRequired).toBe(true);
  });

  it('handles duplicate invalidation event idempotently', async () => {
    const existingAccount = LoyaltyAccount.reconstitute({
      customerId: 'U001',
      tier: 'GOLD',
      totalEligibleSpendCents: 42_000,
      pointsBalance: 82,
      lastCustomerSequence: 4,
      correctionRequired: true,
    });

    const mockRepo: LoyaltyAccountRepositoryPort = {
      findByCustomerId: vi.fn(async () => existingAccount),
      save: vi.fn(async () => {}),
    };
    const mockIdempotency: ConsumerIdempotencyRepositoryPort = {
      markProcessedIfNew: vi.fn(async () => false),
    };

    const handler = new ApplyPurchaseInvalidationHandler(createMockUow(mockRepo, mockIdempotency));
    const command = new ApplyPurchaseInvalidationCommand({
      eventId: 'evt_inv_001',
      customerId: 'U001',
      purchaseId: 'P001',
      customerSequence: 4,
      correlationId: 'corr_inv_001',
      reason: 'Refund',
    });

    const result = await handler.execute(command);

    expect(result.isDuplicate).toBe(true);
    expect(result.correctionRequired).toBe(true);
    expect(mockRepo.save).not.toHaveBeenCalled();
  });

  it('rejects out-of-order sequence on invalidation', async () => {
    const existingAccount = LoyaltyAccount.reconstitute({
      customerId: 'U001',
      tier: 'GOLD',
      totalEligibleSpendCents: 42_000,
      pointsBalance: 82,
      lastCustomerSequence: 1,
      correctionRequired: false,
    });

    const mockRepo: LoyaltyAccountRepositoryPort = {
      findByCustomerId: vi.fn(async () => existingAccount),
      save: vi.fn(async () => {}),
    };
    const mockIdempotency: ConsumerIdempotencyRepositoryPort = {
      markProcessedIfNew: vi.fn(async () => true),
    };

    const handler = new ApplyPurchaseInvalidationHandler(createMockUow(mockRepo, mockIdempotency));
    const command = new ApplyPurchaseInvalidationCommand({
      eventId: 'evt_inv_001',
      customerId: 'U001',
      purchaseId: 'P001',
      customerSequence: 4, // expected 2
      correlationId: 'corr_inv_001',
      reason: 'Refund',
    });

    await expect(handler.execute(command)).rejects.toThrowError(
      /Sequence gap or out-of-order event for customer U001: expected 2, received 4/,
    );
    expect(mockRepo.save).not.toHaveBeenCalled();
  });
});

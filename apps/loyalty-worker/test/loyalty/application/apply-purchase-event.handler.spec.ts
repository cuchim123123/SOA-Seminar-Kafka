import { describe, expect, it, vi } from 'vitest';
import { ApplyPurchaseEventCommand } from '../../../src/loyalty/application/commands/apply-purchase-event.command';
import { ApplyPurchaseEventHandler } from '../../../src/loyalty/application/commands/apply-purchase-event.handler';
import { LoyaltyAccountRepositoryPort } from '../../../src/loyalty/application/ports/loyalty-account-repository.port';
import { LoyaltyAccount } from '../../../src/loyalty/domain/loyalty-account.entity';

describe('ApplyPurchaseEventHandler', () => {
  it('loads existing account, calculates points, and persists updated state', async () => {
    const existingAccount = LoyaltyAccount.initializeNew('U001');
    existingAccount.applyPurchase(12_000); // 12 pts, Silver

    const mockRepo: LoyaltyAccountRepositoryPort = {
      findByCustomerId: vi.fn(async () => existingAccount),
      save: vi.fn(async () => {}),
    };

    const handler = new ApplyPurchaseEventHandler(mockRepo);
    const command = new ApplyPurchaseEventCommand(
      'evt_002',
      'U001',
      'pur_002',
      20_000,
      2,
      'corr_002',
      'purchase.events',
      0,
      '15',
    );

    const result = await handler.execute(command);

    expect(mockRepo.findByCustomerId).toHaveBeenCalledWith('U001');
    expect(mockRepo.save).toHaveBeenCalledWith(existingAccount);

    expect(result.pointsEarned).toBe(40);
    expect(result.currentTier).toBe('GOLD');
    expect(result.totalPoints).toBe(52);
    expect(result.kafkaMetadata).toEqual({
      topic: 'purchase.events',
      partition: 0,
      offset: '15',
    });
  });

  it('creates new account on first purchase for a customer', async () => {
    let savedAccount: LoyaltyAccount | undefined;
    const mockRepo: LoyaltyAccountRepositoryPort = {
      findByCustomerId: vi.fn(async () => null),
      save: vi.fn(async (account: LoyaltyAccount) => {
        savedAccount = account;
      }),
    };

    const handler = new ApplyPurchaseEventHandler(mockRepo);
    const command = new ApplyPurchaseEventCommand(
      'evt_001',
      'U002',
      'pur_001',
      5_000,
      1,
      'corr_001',
      'purchase.events',
      0,
      '1',
    );

    const result = await handler.execute(command);

    expect(result.customerId).toBe('U002');
    expect(result.pointsEarned).toBe(5);
    expect(result.currentTier).toBe('BRONZE');
    expect(result.totalPoints).toBe(5);
    expect(savedAccount).toBeDefined();
    expect(savedAccount?.customerId).toBe('U002');
  });
});

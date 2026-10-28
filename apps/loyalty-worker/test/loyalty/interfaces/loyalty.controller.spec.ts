import { NotFoundException } from '@nestjs/common';
import { describe, expect, it } from 'vitest';
import { GetLoyaltyAccountHandler } from '../../../src/loyalty/application/queries/get-loyalty-account.handler';
import { LoyaltyAccount } from '../../../src/loyalty/domain/loyalty-account.entity';
import { InMemoryLoyaltyAccountRepositoryAdapter } from '../../../src/loyalty/infrastructure/adapters/in-memory-loyalty-account-repository.adapter';
import { LoyaltyController } from '../../../src/loyalty/interfaces/http/loyalty.controller';

describe('LoyaltyController', () => {
  it('throws NotFoundException when customer does not exist', async () => {
    const repository = new InMemoryLoyaltyAccountRepositoryAdapter();
    const handler = new GetLoyaltyAccountHandler(repository);
    const controller = new LoyaltyController(handler);

    await expect(controller.getLoyaltyAccount('UNKNOWN')).rejects.toThrow(NotFoundException);
  });

  it('returns loyalty account DTO when customer exists', async () => {
    const repository = new InMemoryLoyaltyAccountRepositoryAdapter();
    const account = LoyaltyAccount.create('U001');
    account.applyPurchase(12_000);
    await repository.save(account);

    const handler = new GetLoyaltyAccountHandler(repository);
    const controller = new LoyaltyController(handler);

    const result = await controller.getLoyaltyAccount('U001');
    expect(result).toEqual({
      customerId: 'U001',
      tier: 'SILVER',
      totalEligibleSpendCents: 12_000,
      pointsBalance: 12,
    });
  });
});

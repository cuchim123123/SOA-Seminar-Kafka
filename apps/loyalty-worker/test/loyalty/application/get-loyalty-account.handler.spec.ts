import { describe, expect, it } from 'vitest';
import { LoyaltyAccount } from '../../../src/loyalty/domain/loyalty-account.model';
import { GetLoyaltyAccountHandler } from '../../../src/loyalty/application/queries/get-loyalty-account.handler';
import { GetLoyaltyAccountQuery } from '../../../src/loyalty/application/queries/get-loyalty-account.query';
import { InMemoryLoyaltyAccountRepositoryAdapter } from '../../../src/loyalty/infrastructure/adapters/in-memory-loyalty-account-repository.adapter';

describe('GetLoyaltyAccountHandler', () => {
  it('returns null when account does not exist', async () => {
    const repository = new InMemoryLoyaltyAccountRepositoryAdapter();
    const handler = new GetLoyaltyAccountHandler(repository);

    const result = await handler.execute(new GetLoyaltyAccountQuery('NON_EXISTENT'));

    expect(result).toBeNull();
  });

  it('returns account details when account exists', async () => {
    const repository = new InMemoryLoyaltyAccountRepositoryAdapter();
    const account = LoyaltyAccount.initializeNew('U001');
    account.applyPurchase(12_000); // 12 points, SILVER
    await repository.save(account);

    const handler = new GetLoyaltyAccountHandler(repository);
    const result = await handler.execute(new GetLoyaltyAccountQuery('U001'));

    expect(result).not.toBeNull();
    expect(result?.customerId).toBe('U001');
    expect(result?.tier).toBe('SILVER');
    expect(result?.totalEligibleSpendCents).toBe(12_000);
    expect(result?.pointsBalance).toBe(12);
  });
});

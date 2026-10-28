import { describe, expect, it } from 'vitest';
import { LoyaltyAccount } from '../../../src/loyalty/domain/loyalty-account.entity';
import { InMemoryLoyaltyAccountRepositoryAdapter } from '../../../src/loyalty/infrastructure/adapters/in-memory-loyalty-account-repository.adapter';

describe('InMemoryLoyaltyAccountRepositoryAdapter', () => {
  it('saves and retrieves loyalty account by customerId', async () => {
    const adapter = new InMemoryLoyaltyAccountRepositoryAdapter();

    expect(await adapter.findByCustomerId('CUST_99')).toBeNull();

    const account = LoyaltyAccount.initializeNew('CUST_99');
    account.applyPurchase(15_000);
    await adapter.save(account);

    const retrieved = await adapter.findByCustomerId('CUST_99');
    expect(retrieved).not.toBeNull();
    expect(retrieved?.customerId).toBe('CUST_99');
    expect(retrieved?.pointsBalance).toBe(15);
    expect(retrieved?.tier).toBe('SILVER');
    expect(retrieved?.totalEligibleSpendCents).toBe(15_000);
  });
});

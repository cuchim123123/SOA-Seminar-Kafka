import { Injectable } from '@nestjs/common';
import { LoyaltyAccountRepositoryPort } from '../../application/ports/loyalty-account-repository.port';
import { LoyaltyAccount } from '../../domain/loyalty-account.model';

/**
 * In-memory loyalty account repository for Phase 1-3.
 *
 * NOTE - PHASE 1 ARCHITECTURAL LIMITATION:
 * Single-instance non-durable in-memory state tracking for simulated fixtures.
 * In Phase 4, this is replaced by durable PostgreSQL repository with transactional outbox and row locking.
 */
@Injectable()
export class InMemoryLoyaltyAccountRepositoryAdapter implements LoyaltyAccountRepositoryPort {
  private readonly accounts = new Map<string, LoyaltyAccount>();

  async findByCustomerId(customerId: string): Promise<LoyaltyAccount | null> {
    const found = this.accounts.get(customerId);
    if (!found) {
      return Promise.resolve(null);
    }
    // Return a reconstituted clone to prevent accidental outside mutation
    return Promise.resolve(
      LoyaltyAccount.reconstitute({
        customerId: found.customerId,
        tier: found.tier,
        totalEligibleSpendCents: found.totalEligibleSpendCents,
        pointsBalance: found.pointsBalance,
      }),
    );
  }

  async save(account: LoyaltyAccount): Promise<void> {
    this.accounts.set(account.customerId, account);
    return Promise.resolve();
  }
}

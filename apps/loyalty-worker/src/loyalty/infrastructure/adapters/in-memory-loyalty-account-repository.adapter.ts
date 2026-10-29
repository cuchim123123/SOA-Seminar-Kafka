import { Injectable } from '@nestjs/common';
import {
  LoyaltyAccountReadDaoPort,
  LoyaltyAccountReadModel,
} from '../../application/ports/loyalty-account-read-dao.port';
import { LoyaltyAccountRepositoryPort } from '../../application/ports/loyalty-account-repository.port';
import { LoyaltyAccount } from '../../domain/loyalty-account.entity';

/**
 * In-memory persistence adapter satisfying both:
 * 1. LoyaltyAccountRepositoryPort (Write Model / Commands): returns rich domain aggregates.
 * 2. LoyaltyAccountReadDaoPort (Read Model / Queries): penetrates directly to return flat projection DTOs.
 */
@Injectable()
export class InMemoryLoyaltyAccountRepositoryAdapter
  implements LoyaltyAccountRepositoryPort, LoyaltyAccountReadDaoPort
{
  private readonly accounts = new Map<string, LoyaltyAccount>();

  // --- Read DAO implementation (Queries) ---
  async findById(customerId: string): Promise<LoyaltyAccountReadModel | null> {
    const found = this.accounts.get(customerId);
    if (!found) {
      return null;
    }
    return {
      customerId: found.customerId,
      tier: found.tier,
      totalEligibleSpendCents: found.totalEligibleSpendCents,
      pointsBalance: found.pointsBalance,
    };
  }

  // --- Domain Repository implementation (Commands) ---
  async findByCustomerId(customerId: string): Promise<LoyaltyAccount | null> {
    const found = this.accounts.get(customerId);
    if (!found) {
      return null;
    }
    return LoyaltyAccount.reconstitute({
      customerId: found.customerId,
      tier: found.tier,
      totalEligibleSpendCents: found.totalEligibleSpendCents,
      pointsBalance: found.pointsBalance,
      lastCustomerSequence: found.lastCustomerSequence,
      correctionRequired: found.correctionRequired,
    });
  }

  async save(account: LoyaltyAccount): Promise<void> {
    this.accounts.set(account.customerId, account);
  }
}

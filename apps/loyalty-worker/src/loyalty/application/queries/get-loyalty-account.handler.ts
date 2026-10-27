import { Inject, Injectable } from '@nestjs/common';
import {
  LOYALTY_ACCOUNT_REPOSITORY_PORT,
  LoyaltyAccountRepositoryPort,
} from '../ports/loyalty-account-repository.port';
import { GetLoyaltyAccountQuery } from './get-loyalty-account.query';
import { GetLoyaltyAccountResult } from './get-loyalty-account.result';

@Injectable()
export class GetLoyaltyAccountHandler {
  constructor(
    @Inject(LOYALTY_ACCOUNT_REPOSITORY_PORT)
    private readonly repository: LoyaltyAccountRepositoryPort,
  ) {}

  async execute(query: GetLoyaltyAccountQuery): Promise<GetLoyaltyAccountResult | null> {
    const account = await this.repository.findByCustomerId(query.customerId);
    if (!account) {
      return null;
    }

    return {
      customerId: account.customerId,
      tier: account.tier,
      totalEligibleSpendCents: account.totalEligibleSpendCents,
      pointsBalance: account.pointsBalance,
    };
  }
}

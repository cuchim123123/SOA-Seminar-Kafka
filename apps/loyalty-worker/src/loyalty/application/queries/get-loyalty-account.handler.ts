import { Inject, Injectable } from '@nestjs/common';
import {
  LOYALTY_ACCOUNT_READ_DAO_PORT,
  LoyaltyAccountReadDaoPort,
} from '../ports/loyalty-account-read-dao.port';
import { GetLoyaltyAccountQuery } from './get-loyalty-account.query';
import { GetLoyaltyAccountResult } from './get-loyalty-account.result';

@Injectable()
export class GetLoyaltyAccountHandler {
  constructor(
    @Inject(LOYALTY_ACCOUNT_READ_DAO_PORT)
    private readonly readDao: LoyaltyAccountReadDaoPort,
  ) {}

  async execute(query: GetLoyaltyAccountQuery): Promise<GetLoyaltyAccountResult | null> {
    const account = await this.readDao.findById(query.data.customerId);
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

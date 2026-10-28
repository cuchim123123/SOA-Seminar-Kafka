import { Tier } from '../../domain/loyalty-policy';

export interface LoyaltyAccountReadModel {
  readonly customerId: string;
  readonly tier: Tier;
  readonly totalEligibleSpendCents: number;
  readonly pointsBalance: number;
}

export const LOYALTY_ACCOUNT_READ_DAO_PORT = Symbol('LOYALTY_ACCOUNT_READ_DAO_PORT');

/**
 * Read-side DAO / Query Port for the Loyalty bounded context.
 * Bypasses domain aggregates and repositories to fetch flat projection models directly.
 */
export interface LoyaltyAccountReadDaoPort {
  findById(customerId: string): Promise<LoyaltyAccountReadModel | null>;
}

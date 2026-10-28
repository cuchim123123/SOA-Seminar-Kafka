import { LoyaltyAccount } from '../../domain/loyalty-account.entity';

export const LOYALTY_ACCOUNT_REPOSITORY_PORT = Symbol('LOYALTY_ACCOUNT_REPOSITORY_PORT');

export interface LoyaltyAccountRepositoryPort {
  findByCustomerId(customerId: string): Promise<LoyaltyAccount | null>;
  save(account: LoyaltyAccount): Promise<void>;
}

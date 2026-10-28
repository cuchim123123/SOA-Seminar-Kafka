import { Purchase } from '../../domain/purchase.entity';

export const PURCHASE_REPOSITORY_PORT = Symbol('PURCHASE_REPOSITORY_PORT');

export interface PurchaseRepositoryPort {
  save(purchase: Purchase): Promise<void>;
  findById(purchaseId: string): Promise<Purchase | null>;
}

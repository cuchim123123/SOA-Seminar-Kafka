import { Purchase } from '../../domain/purchase.entity';

export const PURCHASE_REPOSITORY_PORT = Symbol('PURCHASE_REPOSITORY_PORT');

export interface FindPurchaseOptions {
  readonly forUpdate?: boolean;
}

export interface PurchaseRepositoryPort {
  save(purchase: Purchase): Promise<void>;
  findById(purchaseId: string, options?: FindPurchaseOptions): Promise<Purchase | null>;
}

import { Injectable } from '@nestjs/common';
import { Purchase } from '../../domain/purchase.entity';
import { PurchaseRepositoryPort } from '../../application/ports/purchase-repository.port';

@Injectable()
export class InMemoryPurchaseRepositoryAdapter implements PurchaseRepositoryPort {
  private readonly purchases = new Map<string, Purchase>();

  async save(purchase: Purchase): Promise<void> {
    this.purchases.set(purchase.purchaseId, purchase);
  }

  async findById(purchaseId: string): Promise<Purchase | null> {
    return this.purchases.get(purchaseId) ?? null;
  }

  clear(): void {
    this.purchases.clear();
  }
}

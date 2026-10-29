export interface PurchaseReadModel {
  readonly purchaseId: string;
  readonly customerId: string;
  readonly amountCents: number;
  readonly status: 'COMPLETED' | 'INVALIDATED';
  readonly customerSequence: number;
  readonly createdAt: string;
  readonly outboxStatus: 'PENDING' | 'PUBLISHED' | 'FAILED' | 'UNKNOWN';
  readonly publishedAt: string | null;
  readonly eventId: string | null;
}

export const PURCHASE_READ_DAO_PORT = Symbol('PURCHASE_READ_DAO_PORT');

export interface PurchaseReadDaoPort {
  findById(purchaseId: string): Promise<PurchaseReadModel | null>;
}

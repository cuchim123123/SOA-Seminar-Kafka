export interface PurchaseHistoryItem {
  purchaseId: string;
  customerId: string;
  amountCents: string;
  status: string;
  createdAt: string;
  completionSequence: string;
  completionEventId: string | null;
  invalidationSequence: string | null;
  invalidationEventId: string | null;
  invalidationReason: string | null;
}

export interface PurchaseHistoryQuery {
  purchaseId?: string;
  customerId?: string;
  cursor?: string;
  limit: number;
}

export interface PurchaseHistoryPage {
  items: PurchaseHistoryItem[];
  nextCursor: string | null;
}

export const PURCHASE_HISTORY_PORT = Symbol('PURCHASE_HISTORY_PORT');
export interface PurchaseHistoryPort {
  list(query: PurchaseHistoryQuery): Promise<PurchaseHistoryPage>;
}

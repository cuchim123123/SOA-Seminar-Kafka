import { request } from './http';

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

export interface PurchaseDetail {
  purchaseId: string;
  customerId: string;
  amountCents: number;
  status: 'COMPLETED' | 'INVALIDATED';
  customerSequence: number;
  createdAt: string;
  outboxStatus: 'ENQUEUED' | 'UNKNOWN';
  publishedAt: string | null;
  eventId: string | null;
}

export const businessApi = {
  getPurchase(purchaseId: string, signal?: AbortSignal) {
    return request<PurchaseDetail>(
      `/api/producer/api/v1/purchases/${encodeURIComponent(purchaseId)}`,
      { signal },
    );
  },
  listPurchases(
    query: { purchaseId?: string; customerId?: string; cursor?: string; limit?: number },
    signal?: AbortSignal,
  ) {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(query)) {
      if (value !== undefined) params.set(key, String(value));
    }
    return request<{ items: PurchaseHistoryItem[]; nextCursor: string | null }>(
      `/api/producer/api/v1/purchases?${params}`,
      { signal },
    );
  },
  getReceipts(context: 'loyalty' | 'analytics', eventIds: string[], signal?: AbortSignal) {
    const query = new URLSearchParams({ eventIds: eventIds.join(',') });
    return request<{ receipts: Array<{ eventId: string; processedAt: string }> }>(
      `/api/${context}/api/v1/${context}/event-receipts?${query}`,
      { signal },
    );
  },
};

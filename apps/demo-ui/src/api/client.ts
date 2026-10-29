export interface PartitionInfo {
  partition: number;
  leader: number;
  replicas: number[];
  isr: number[];
  logStartOffset: string;
  highWatermark: string;
  messageCount: string;
}

export interface TopicPartitionMetadata {
  topic: string;
  partitionCount: number;
  partitions: PartitionInfo[];
}

export interface PartitionLag {
  partition: number;
  logEndOffset: string;
  committedOffset: string | null;
  lag: string;
}

export interface ConsumerGroupLag {
  groupId: string;
  topic: string;
  partitions: PartitionLag[];
  totalLag: string;
}

export interface LoyaltyAccountDto {
  customerId: string;
  pointsBalance: number;
  tier: 'BRONZE' | 'SILVER' | 'GOLD';
  totalEligibleSpendCents: number;
  lastCustomerSequence: number;
  correctionRequired: boolean;
}

export interface SalesSummaryDto {
  totalRevenueCents: number;
  totalPurchasesCount: number;
  lastUpdated: string | null;
  processedEventCount: number;
}

export interface ConsumerStatusDto {
  status: 'RUNNING' | 'PAUSED';
  isPaused: boolean;
  groupId: string;
  topic: string;
}

export interface PurchaseCreatedResponse {
  purchaseId: string;
  customerId: string;
  status: string;
  customerSequence: number;
  eventId: string;
}

export interface PurchaseInvalidatedResponse {
  purchaseId: string;
  customerId: string;
  status: string;
  customerSequence: number;
  eventId: string;
  reason: string;
  isAlreadyInvalidated: boolean;
}

export interface ReplayRunDto {
  id: string;
  groupId: string;
  topic: string;
  status: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED';
  startOffsets: Record<number, string>;
  highWatermarks: Record<number, string>;
  scannedCount: number;
  stagingCount?: number;
  errorMessage?: string | null;
  startedAt?: string | null;
  completedAt?: string | null;
}

export interface CustomerComparisonDto {
  customerId: string;
  runId: string;
  live: {
    points: number;
    tier: string;
    eligibleSpendCents: number;
    correctionRequired: boolean;
  } | null;
  staged: {
    points: number;
    tier: string;
    eligibleSpendCents: number;
    scannedEventCount: number;
  } | null;
  discrepancy: {
    pointsDifference: number;
    spendDifferenceCents: number;
    tierChanged: boolean;
  };
}

export interface ServiceHealthStatus {
  producer: boolean;
  loyalty: boolean;
  analytics: boolean;
  publisher: boolean;
  replay: boolean;
}

async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });

  if (!res.ok) {
    let errorMsg = `Request failed: ${res.status} ${res.statusText}`;
    try {
      const errObj = await res.json();
      errorMsg = errObj.message || errObj.error || errorMsg;
    } catch {
      // Ignore json parse error
    }
    throw new Error(errorMsg);
  }

  return res.json() as Promise<T>;
}

export const api = {
  // Producer API
  createPurchase: async (
    customerId: string,
    amountCents: number,
  ): Promise<PurchaseCreatedResponse> => {
    return request<PurchaseCreatedResponse>('/api/producer/api/v1/purchases', {
      method: 'POST',
      body: JSON.stringify({ customerId, amountCents }),
    });
  },

  invalidatePurchase: async (
    purchaseId: string,
    reason: string = 'Item returned or cancelled',
  ): Promise<PurchaseInvalidatedResponse> => {
    return request<PurchaseInvalidatedResponse>(
      `/api/producer/api/v1/purchases/${purchaseId}/invalidation`,
      {
        method: 'POST',
        body: JSON.stringify({ reason }),
      },
    );
  },

  getPartitions: async (): Promise<TopicPartitionMetadata> => {
    return request<TopicPartitionMetadata>('/api/producer/api/v1/ops/partitions');
  },

  getConsumerLag: async (groupId: string): Promise<ConsumerGroupLag> => {
    return request<ConsumerGroupLag>(`/api/producer/api/v1/ops/groups/${groupId}/lag`);
  },

  // Loyalty API
  getLoyaltyAccount: async (customerId: string): Promise<LoyaltyAccountDto | null> => {
    try {
      return await request<LoyaltyAccountDto>(`/api/loyalty/api/v1/loyalty/accounts/${customerId}`);
    } catch {
      return null;
    }
  },

  // Analytics API
  getAnalyticsSummary: async (): Promise<SalesSummaryDto> => {
    return request<SalesSummaryDto>('/api/analytics/api/v1/analytics/summary');
  },

  getConsumerStatus: async (): Promise<ConsumerStatusDto> => {
    return request<ConsumerStatusDto>('/api/analytics/api/v1/analytics/consumer/status');
  },

  pauseAnalytics: async (): Promise<{ status: string }> => {
    return request<{ status: string }>('/api/analytics/api/v1/analytics/consumer/pause', {
      method: 'POST',
    });
  },

  resumeAnalytics: async (): Promise<{ status: string }> => {
    return request<{ status: string }>('/api/analytics/api/v1/analytics/consumer/resume', {
      method: 'POST',
    });
  },

  // Replay API
  startReplay: async (params?: {
    runId?: string;
    topic?: string;
    requiredStartOffsets?: Record<string, string>;
  }): Promise<ReplayRunDto> => {
    return request<ReplayRunDto>('/api/replay/api/v1/replay/runs', {
      method: 'POST',
      body: JSON.stringify(params || {}),
    });
  },

  getReplayRun: async (runId: string): Promise<ReplayRunDto> => {
    return request<ReplayRunDto>(`/api/replay/api/v1/replay/runs/${runId}`);
  },

  getComparison: async (runId: string, customerId: string): Promise<CustomerComparisonDto> => {
    return request<CustomerComparisonDto>(
      `/api/replay/api/v1/replay/runs/${runId}/comparison/${customerId}`,
    );
  },

  // Health checks
  checkServicesHealth: async (): Promise<ServiceHealthStatus> => {
    const check = async (path: string): Promise<boolean> => {
      try {
        const res = await fetch(path);
        return res.ok;
      } catch {
        return false;
      }
    };

    const [producer, loyalty, analytics, publisher, replay] = await Promise.all([
      check('/health/producer'),
      check('/health/loyalty'),
      check('/health/analytics'),
      check('/health/publisher'),
      check('/health/replay'),
    ]);

    return { producer, loyalty, analytics, publisher, replay };
  },
};

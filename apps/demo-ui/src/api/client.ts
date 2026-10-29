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
  logStartOffset?: string;
  logEndOffset?: string;
  committedOffset: string | null;
  lag: string;
  isUncommitted?: boolean;
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
  tier: 'BRONZE' | 'SILVER' | 'GOLD' | string;
  totalEligibleSpendCents: number;
  lastCustomerSequence?: number;
  correctionRequired?: boolean;
}

export interface SalesSummaryDto {
  totalRevenueCents: number;
  totalPurchasesCount: number;
  lastUpdated: string | null;
  processedEventCount: number;
  uniqueCustomerCount?: number;
}

export interface ConsumerStatusDto {
  isPaused: boolean;
  groupId: string;
  topic: string;
  pausedPartitions?: number[];
}

export interface PurchaseCreatedResponse {
  purchaseId: string;
  customerId: string;
  status: string;
  customerSequence?: number;
  eventId: string;
  correlationId?: string;
}

export interface PurchaseInvalidatedResponse {
  purchaseId: string;
  customerId: string;
  status: string;
  customerSequence: number;
  eventId: string;
  reason?: string;
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
    lastCustomerSequence?: number;
    correctionRequired: boolean;
  } | null;
  staged: {
    points: number;
    tier: string;
    eligibleSpendCents: number;
    lastCustomerSequence?: number;
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
  // 1. Producer API
  createPurchase: async (
    customerId: string,
    amountCents: number,
  ): Promise<PurchaseCreatedResponse> => {
    const raw = await request<{
      purchaseId: string;
      requestStatus: string;
      eventId: string;
      correlationId: string;
    }>('/api/producer/api/v1/purchases', {
      method: 'POST',
      body: JSON.stringify({ customerId, amountCents }),
    });

    return {
      purchaseId: raw.purchaseId,
      customerId,
      status: raw.requestStatus,
      eventId: raw.eventId,
      correlationId: raw.correlationId,
    };
  },

  invalidatePurchase: async (
    purchaseId: string,
    reason: string = 'Customer return requested',
  ): Promise<PurchaseInvalidatedResponse> => {
    return request<PurchaseInvalidatedResponse>(
      `/api/producer/api/v1/purchases/${purchaseId}/invalidate`,
      {
        method: 'POST',
        body: JSON.stringify({ reason }),
      },
    );
  },

  getPartitions: async (topic: string = 'purchase.events'): Promise<TopicPartitionMetadata> => {
    const raw = await request<{
      topic: string;
      partitionCount: number;
      partitions: Array<{
        topic: string;
        partition: number;
        leader: number;
        replicas: number[];
        isr: number[];
        logStartOffset: string;
        logEndOffset: string;
      }>;
    }>(`/api/producer/api/v1/ops/kafka/topics/${topic}`);

    return {
      topic: raw.topic,
      partitionCount: raw.partitionCount,
      partitions: raw.partitions.map((p) => {
        const start = BigInt(p.logStartOffset || '0');
        const end = BigInt(p.logEndOffset || '0');
        const count = end >= start ? (end - start).toString() : '0';
        return {
          partition: p.partition,
          leader: p.leader,
          replicas: p.replicas,
          isr: p.isr,
          logStartOffset: p.logStartOffset,
          highWatermark: p.logEndOffset,
          messageCount: count,
        };
      }),
    };
  },

  getConsumerLag: async (
    groupId: string,
    topic: string = 'purchase.events',
  ): Promise<ConsumerGroupLag> => {
    return request<ConsumerGroupLag>(
      `/api/producer/api/v1/ops/kafka/groups/${groupId}/lag?topic=${topic}`,
    );
  },

  // 2. Loyalty API
  getLoyaltyAccount: async (customerId: string): Promise<LoyaltyAccountDto | null> => {
    try {
      return await request<LoyaltyAccountDto>(`/api/loyalty/api/v1/loyalty/${customerId}`);
    } catch {
      return null;
    }
  },

  // 3. Analytics API
  getAnalyticsSummary: async (): Promise<SalesSummaryDto> => {
    const raw = await request<{
      totalRevenueCents: number;
      purchaseCount: number;
      uniqueCustomerCount: number;
      lastProcessedAt: string | null;
    }>('/api/analytics/api/v1/analytics/summary');

    return {
      totalRevenueCents: raw.totalRevenueCents,
      totalPurchasesCount: raw.purchaseCount,
      lastUpdated: raw.lastProcessedAt,
      processedEventCount: raw.purchaseCount,
      uniqueCustomerCount: raw.uniqueCustomerCount,
    };
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

  // 4. Replay API
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

  // 5. Cluster Health Checks
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

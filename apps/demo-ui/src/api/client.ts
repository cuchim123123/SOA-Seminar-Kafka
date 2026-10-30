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
  processingDelayMs?: number;
}

export interface PurchaseBatchResponse {
  totalCreated: number;
  durationMs: number;
  customerDistribution: Record<string, number>;
  purchases: Array<{
    purchaseId: string;
    customerId: string;
    amountCents: number;
    customerSequence: number;
    eventId: string;
  }>;
}

export interface PurchaseCreatedResponse {
  purchaseId: string;
  customerId: string;
  status: string;
  customerSequence?: number;
  eventId: string;
  correlationId?: string;
  idempotencyKey?: string;
  amountCents?: number;
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
  status: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'CUTOVER_COMPLETED' | 'FAILED';
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

export interface StagingCustomerRecord {
  id: string;
  runId: string;
  customerId: string;
  points: number;
  tier: string;
  eligibleSpendCents: number;
  scannedEventCount: number;
  lastCustomerSequence: number;
  createdAt: string;
}

export interface CutoverResult {
  runId: string;
  status: 'CUTOVER_COMPLETED';
  appliedCustomersCount: number;
  cutoverAt: string;
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

export interface BrokerMessageRecord {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  timestamp: string;
  payload: Record<string, unknown> | string | null;
}

export const api = {
  // 1. Producer API
  createPurchase: async (
    customerId: string,
    amountCents: number,
    idempotencyKey?: string,
  ): Promise<PurchaseCreatedResponse> => {
    const raw = await request<{
      purchaseId: string;
      requestStatus: string;
      eventId: string;
      correlationId: string;
      idempotencyKey?: string;
    }>('/api/producer/api/v1/purchases', {
      method: 'POST',
      body: JSON.stringify({ customerId, amountCents, idempotencyKey }),
    });

    return {
      purchaseId: raw.purchaseId,
      customerId,
      status: raw.requestStatus,
      eventId: raw.eventId,
      correlationId: raw.correlationId,
      idempotencyKey: idempotencyKey || raw.idempotencyKey,
      amountCents,
    };
  },

  injectPoisonPill: async (params?: {
    topic?: string;
    key?: string;
  }): Promise<{
    status: string;
    topic: string;
    partition: number;
    offset: string;
    key: string;
  }> => {
    return request('/api/producer/api/v1/ops/kafka/inject-poison', {
      method: 'POST',
      body: JSON.stringify(params || {}),
    });
  },

  publishRawMessage: async (params: {
    topic?: string;
    key?: string;
    payload: string;
  }): Promise<{
    status: string;
    topic: string;
    partition: number;
    offset: string;
    key: string;
  }> => {
    return request('/api/producer/api/v1/ops/kafka/publish-raw', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  },

  createPurchaseBatch: async (params: {
    count: number;
    customerId?: string;
    minAmountCents?: number;
    maxAmountCents?: number;
  }): Promise<PurchaseBatchResponse> => {
    return request<PurchaseBatchResponse>('/api/producer/api/v1/purchases/batch', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  },

  invalidatePurchase: async (
    purchaseId: string,
    reason: string = 'DEMO_INVALIDATION',
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

  scaleTopicPartitions: async (
    topic: string = 'purchase.events',
    count: number,
  ): Promise<TopicPartitionMetadata> => {
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
    }>(`/api/producer/api/v1/ops/kafka/topics/${topic}/partitions`, {
      method: 'POST',
      body: JSON.stringify({ count }),
    });

    return {
      topic: raw.topic,
      partitionCount: raw.partitionCount,
      partitions: raw.partitions.map((p) => {
        const start = BigInt(p.logStartOffset || '0');
        const end = BigInt(p.logEndOffset || '0');
        const msgCount = end >= start ? (end - start).toString() : '0';
        return {
          partition: p.partition,
          leader: p.leader,
          replicas: p.replicas,
          isr: p.isr,
          logStartOffset: p.logStartOffset,
          highWatermark: p.logEndOffset,
          messageCount: msgCount,
        };
      }),
    };
  },

  peekPartitionMessages: async (
    topic: string = 'purchase.events',
    partition: number,
    limit: number = 10,
  ): Promise<BrokerMessageRecord[]> => {
    return request<BrokerMessageRecord[]>(
      `/api/producer/api/v1/ops/kafka/topics/${topic}/partitions/${partition}/messages?limit=${limit}`,
    );
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

  getLoyaltyConsumerStatus: async (): Promise<ConsumerStatusDto> => {
    return request<ConsumerStatusDto>('/api/loyalty/api/v1/loyalty/consumer/status');
  },

  pauseLoyalty: async (): Promise<{ status: string }> => {
    return request<{ status: string }>('/api/loyalty/api/v1/loyalty/consumer/pause', {
      method: 'POST',
    });
  },

  resumeLoyalty: async (): Promise<{ status: string }> => {
    return request<{ status: string }>('/api/loyalty/api/v1/loyalty/consumer/resume', {
      method: 'POST',
    });
  },

  setLoyaltyConsumerDelay: async (
    delayMs: number,
  ): Promise<{ status: string; processingDelayMs: number }> => {
    return request<{ status: string; processingDelayMs: number }>(
      '/api/loyalty/api/v1/loyalty/consumer/delay',
      {
        method: 'POST',
        body: JSON.stringify({ delayMs }),
      },
    );
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

  setAnalyticsConsumerDelay: async (
    delayMs: number,
  ): Promise<{ status: string; processingDelayMs: number }> => {
    return request<{ status: string; processingDelayMs: number }>(
      '/api/analytics/api/v1/analytics/consumer/delay',
      {
        method: 'POST',
        body: JSON.stringify({ delayMs }),
      },
    );
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

  getStagingRecords: async (runId: string): Promise<StagingCustomerRecord[]> => {
    return request<StagingCustomerRecord[]>(`/api/replay/api/v1/replay/runs/${runId}/staging`);
  },

  cutoverReplayRun: async (runId: string): Promise<CutoverResult> => {
    return request<CutoverResult>(`/api/replay/api/v1/replay/runs/${runId}/cutover`, {
      method: 'POST',
    });
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

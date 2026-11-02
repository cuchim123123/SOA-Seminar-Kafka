import { ApiError, request } from './http';

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
  lastCustomerSequence?: number | string;
  correctionRequired?: boolean;
}

export interface SalesSummaryDto {
  totalRevenueCents: number;
  totalPurchasesCount: number;
  lastUpdated: string | null;
  processedEventCount: number;
  uniqueCustomerCount?: number;
}

export interface LoyaltyConsumerInstanceInfoDto {
  instanceId: string;
  clientId: string;
  isPrimary: boolean;
  status: 'RUNNING' | 'PAUSED' | 'KILLED' | 'STOPPED';
  assignedPartitions: number[];
}

export interface ConsumerStatusDto {
  isPaused: boolean;
  groupId: string;
  topic: string;
  pausedPartitions?: number[];
  processingDelayMs?: number;
  instanceCount?: number;
  instances?: LoyaltyConsumerInstanceInfoDto[];
  totalRebalanceCount?: number;
}

export interface ConsumerGroupMemberDto {
  memberId: string;
  clientId: string;
  clientHost: string;
  assignedPartitions: number[];
}

export interface ConsumerGroupDescriptionDto {
  groupId: string;
  state: 'Stable' | 'PreparingRebalance' | 'CompletingRebalance' | 'Dead' | 'Empty' | string;
  protocol: string;
  protocolType: string;
  members: ConsumerGroupMemberDto[];
}

export interface RebalanceEventLogDto {
  id: string;
  timestamp: string;
  eventType: 'REBALANCING' | 'GROUP_JOIN' | 'MEMBER_JOINED' | 'MEMBER_LEFT' | 'MEMBER_KILLED';
  memberId: string;
  clientId: string;
  groupGenerationId?: number;
  assignedPartitions?: number[];
  durationMs?: number;
  details: string;
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
    lastCustomerSequence?: number | string;
    correctionRequired: boolean;
  } | null;
  staged: {
    points: number;
    tier: string;
    eligibleSpendCents: number;
    lastCustomerSequence?: number | string;
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
  cdc: boolean;
  replay: boolean;
}

export interface OutboxEventDto {
  eventId: string;
  aggregateId: string;
  customerSequence: string;
  eventType: string;
  payload: Record<string, unknown> | string | unknown;
  status: string;
  attemptCount: number;
  createdAt: string;
  publishedAt: string | null;
}

export interface HttpIdempotencyKeyDto {
  customerId: string;
  idempotencyKey: string;
  requestFingerprint: string;
  purchaseId: string;
}

export interface OutboxStatusDto {
  totalCount: number;
  events: OutboxEventDto[];
  recentIdempotencyKeys: HttpIdempotencyKeyDto[];
}

export interface RollbackTestResultDto {
  status: string;
  simulatedFailureReason: string;
  testCustomerId: string;
  testPurchaseId: string;
  persistedOutboxRows: number;
  persistedPurchases: number;
  dualWritePrevented: boolean;
  verificationDetails: string;
}

export interface ProcessedEventDto {
  consumerName: string;
  eventId: string;
  processedAt: string;
}

export interface ProcessedEventsDto {
  count: number;
  events: ProcessedEventDto[];
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
      return await request<LoyaltyAccountDto>(
        `/api/loyalty/api/v1/loyalty/${encodeURIComponent(customerId)}`,
      );
    } catch (error) {
      if (error instanceof ApiError && error.status === 404) return null;
      throw error;
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

  pauseLoyaltyPartition: async (
    partition: number,
  ): Promise<{ status: string; partition: number; isPaused: boolean }> => {
    return request<{ status: string; partition: number; isPaused: boolean }>(
      `/api/loyalty/api/v1/loyalty/consumer/partitions/${partition}/pause`,
      {
        method: 'POST',
      },
    );
  },

  resumeLoyaltyPartition: async (
    partition: number,
  ): Promise<{ status: string; partition: number; isPaused: boolean }> => {
    return request<{ status: string; partition: number; isPaused: boolean }>(
      `/api/loyalty/api/v1/loyalty/consumer/partitions/${partition}/resume`,
      {
        method: 'POST',
      },
    );
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

  scaleLoyaltyConsumer: async (
    targetInstances: number,
  ): Promise<{
    status: string;
    targetCount: number;
    instances: LoyaltyConsumerInstanceInfoDto[];
  }> => {
    return request<{
      status: string;
      targetCount: number;
      instances: LoyaltyConsumerInstanceInfoDto[];
    }>('/api/loyalty/api/v1/loyalty/consumer/scale', {
      method: 'POST',
      body: JSON.stringify({ targetInstances }),
    });
  },

  killLoyaltyReplica: async (
    instanceId?: string,
  ): Promise<{ status: string; killedInstanceId: string; remainingCount: number }> => {
    return request<{ status: string; killedInstanceId: string; remainingCount: number }>(
      '/api/loyalty/api/v1/loyalty/consumer/kill-replica',
      {
        method: 'POST',
        body: JSON.stringify({ instanceId }),
      },
    );
  },

  getLoyaltyRebalanceHistory: async (): Promise<RebalanceEventLogDto[]> => {
    return request<RebalanceEventLogDto[]>(
      '/api/loyalty/api/v1/loyalty/consumer/rebalance-history',
    );
  },

  describeConsumerGroup: async (
    groupId: string,
    topic: string = 'purchase.events',
  ): Promise<ConsumerGroupDescriptionDto> => {
    return request<ConsumerGroupDescriptionDto>(
      `/api/producer/api/v1/ops/kafka/groups/${groupId}/description?topic=${topic}`,
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

  pauseAnalyticsPartition: async (
    partition: number,
  ): Promise<{ status: string; partition: number; isPaused: boolean }> => {
    return request<{ status: string; partition: number; isPaused: boolean }>(
      `/api/analytics/api/v1/analytics/consumer/partitions/${partition}/pause`,
      {
        method: 'POST',
      },
    );
  },

  resumeAnalyticsPartition: async (
    partition: number,
  ): Promise<{ status: string; partition: number; isPaused: boolean }> => {
    return request<{ status: string; partition: number; isPaused: boolean }>(
      `/api/analytics/api/v1/analytics/consumer/partitions/${partition}/resume`,
      {
        method: 'POST',
      },
    );
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
    simulateRetentionFailure?: boolean;
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

  // CDC status is read-only; RUNNING does not assert delivery completion.
  checkCdc: async (): Promise<void> => {
    const status = await request<{ connector: { state: string }; tasks: { state: string }[] }>(
      '/api/cdc/status',
    );
    if (
      status.connector.state !== 'RUNNING' ||
      status.tasks.length === 0 ||
      status.tasks.some((task) => task.state !== 'RUNNING')
    ) {
      throw new Error('Debezium connector or source task is not running');
    }
  },

  // Real delivery barrier: resolves only after the live loyalty consumer has applied
  // every event through minLastSequence. Consumers only ever receive what Debezium
  // published, so reaching this state proves the source rows reached Kafka. This is
  // NOT a connector status check; it observes processed projection state instead.
  waitForLoyaltyProcessed: async (
    customerId: string,
    minLastSequence: number,
    timeoutMs: number = 30000,
  ): Promise<LoyaltyAccountDto> => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const account = await api.getLoyaltyAccount(customerId).catch(() => null);
      if (
        account &&
        account.lastCustomerSequence !== undefined &&
        BigInt(account.lastCustomerSequence) >= BigInt(minLastSequence) &&
        account.correctionRequired === true
      ) {
        return account;
      }
      await new Promise((resolve) => setTimeout(resolve, 300));
    }
    throw new Error(
      `Loyalty consumer did not apply event sequence ${minLastSequence} for ${customerId} within ${timeoutMs}ms (CDC delivery stalled?)`,
    );
  },

  // 5. Cluster Health Checks
  checkServicesHealth: async (): Promise<ServiceHealthStatus> => {
    const httpCheck = async (path: string): Promise<boolean> => {
      try {
        const res = await fetch(path);
        return res.ok;
      } catch {
        return false;
      }
    };

    const checkCdcRunning = async (): Promise<boolean> => {
      try {
        const status = await request<{ connector: { state: string }; tasks: { state: string }[] }>(
          '/api/cdc/status',
        );
        return (
          status.connector.state === 'RUNNING' &&
          status.tasks.length > 0 &&
          status.tasks.every((task) => task.state === 'RUNNING')
        );
      } catch {
        return false;
      }
    };

    const [producer, loyalty, analytics, replay, cdc] = await Promise.all([
      httpCheck('/health/producer'),
      httpCheck('/health/loyalty'),
      httpCheck('/health/analytics'),
      httpCheck('/health/replay'),
      checkCdcRunning(),
    ]);

    return { producer, loyalty, analytics, replay, cdc };
  },

  // 6. Outbox & CDC Inspection
  getOutboxStatus: async (): Promise<OutboxStatusDto> => {
    return request<OutboxStatusDto>('/api/producer/api/v1/ops/outbox');
  },

  testOutboxRollback: async (): Promise<RollbackTestResultDto> => {
    return request<RollbackTestResultDto>('/api/producer/api/v1/ops/outbox/test-rollback', {
      method: 'POST',
    });
  },

  // 7. Stream Idempotency & Deduplication
  testConsumerDuplicate: async (params: {
    eventId?: string;
    customerId?: string;
    purchaseId?: string;
    amountCents?: number;
    customerSequence?: number;
  }): Promise<{
    status: string;
    topic: string;
    partition: number;
    offset: string;
    key: string;
    eventId: string;
    note: string;
  }> => {
    return request('/api/producer/api/v1/ops/kafka/test-duplicate', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  },

  getProcessedEvents: async (): Promise<ProcessedEventsDto> => {
    return request<ProcessedEventsDto>('/api/loyalty/api/v1/loyalty/processed-events');
  },
};

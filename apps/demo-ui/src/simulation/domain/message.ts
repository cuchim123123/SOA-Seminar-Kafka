export interface SimMessagePayload {
  purchaseId: string;
  customerId: string;
  amountCents: number;
  itemDescription?: string;
  metadata?: Record<string, unknown>;
}

export interface SimMessage {
  id: string;
  key: string;
  payload: SimMessagePayload;
  createdAtVirtualMs: number;
  timestampMs?: number;
  topic?: string;
  partition?: number;
  offset?: number;
  status: 'CREATED' | 'IN_FLIGHT' | 'APPENDED' | 'PROCESSING' | 'COMMITTED' | 'FAILED';
  stepHistory: Array<{
    step: string;
    virtualTimeMs: number;
    nodeId: string;
    details?: string;
  }>;
}

let messageSeq = 0;

export function createSimMessage(
  key: string = 'U001',
  amountCents: number = 4999,
  virtualTimeMs: number = 0,
  options?: {
    purchaseId?: string;
    itemDescription?: string;
    targetFailedConsumerIds?: string[];
    targetFailedGroupIds?: string[];
  },
): SimMessage {
  messageSeq += 1;
  const purchaseId = options?.purchaseId || `sim-order-${String(messageSeq).padStart(4, '0')}`;

  return {
    id: `msg-${Date.now()}-${messageSeq}`,
    key,
    payload: {
      purchaseId,
      customerId: key,
      amountCents,
      itemDescription: options?.itemDescription || `Simulated Retail Order #${messageSeq}`,
      metadata: {
        ...(options?.targetFailedConsumerIds
          ? { targetFailedConsumerIds: options.targetFailedConsumerIds }
          : {}),
        ...(options?.targetFailedGroupIds
          ? { targetFailedGroupIds: options.targetFailedGroupIds }
          : {}),
      },
    },
    createdAtVirtualMs: virtualTimeMs,
    status: 'CREATED',
    stepHistory: [],
  };
}

export function resetMessageSequence(): void {
  messageSeq = 0;
}

import type { SimMessage } from './message';

export type SimEventType =
  | 'DISPATCH_MESSAGE'
  | 'MESSAGE_IN_FLIGHT'
  | 'APPEND_PARTITION'
  | 'POLL_RECORD'
  | 'START_PROCESSING'
  | 'COMPLETE_PROCESSING'
  | 'WRITE_DATABASE'
  | 'WRITE_OUTBOX'
  | 'CDC_RELAY_STREAM'
  | 'REBALANCE_GROUP'
  | 'LEADER_FAILOVER'
  | 'CRASH_BROKER'
  | 'CRASH_CONSUMER'
  | 'REVIVE_CONSUMER'
  | 'ROUTE_DLQ'
  | 'CONSUMER_ERROR'
  | 'IDEMPOTENCY_SKIP'
  | 'IDEMPOTENCY_DISABLED_HAZARD';

export interface SimEvent {
  id: string;
  seq: number; // Monotonic sequence for deterministic tie-breaking
  scheduledTime: number; // Virtual simulation timestamp in ms
  type: SimEventType;
  sourceNodeId: string;
  targetNodeId?: string;
  edgeId?: string;
  message?: SimMessage;
  description: string;
  metadata?: Record<string, unknown>;
}

let eventCounter = 0;

export function createSimEvent(
  scheduledTime: number,
  type: SimEventType,
  sourceNodeId: string,
  description: string,
  options?: {
    targetNodeId?: string;
    edgeId?: string;
    message?: SimMessage;
    metadata?: Record<string, unknown>;
  },
): SimEvent {
  eventCounter += 1;
  return {
    id: `ev-${scheduledTime}-${eventCounter}`,
    seq: eventCounter,
    scheduledTime,
    type,
    sourceNodeId,
    targetNodeId: options?.targetNodeId,
    edgeId: options?.edgeId,
    message: options?.message,
    description,
    metadata: options?.metadata,
  };
}

export function resetEventCounter(): void {
  eventCounter = 0;
}

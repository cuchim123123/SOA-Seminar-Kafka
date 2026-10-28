import { uuidv7 } from 'uuidv7';
import {
  PurchaseCompletedEnvelopeSchemaV1,
  PurchaseCompletedEnvelopeV1,
  PurchaseInvalidatedEnvelopeSchemaV1,
  PurchaseInvalidatedEnvelopeV1,
  PurchaseInvalidatedReasonV1,
} from './envelope.v1';

export interface CreatePurchaseCompletedInput {
  customerId: string;
  amountCents: number;
  customerSequence: number;
  eventId?: string;
  purchaseId?: string;
  correlationId?: string;
  occurredAt?: string;
}

/**
 * Safe factory to construct a PurchaseCompleted envelope.
 * Eliminates inconsistency risks by accepting customerId once and synchronizing aggregateId and payload.
 * Generates RFC 9562 UUIDv7 by default for eventId, correlationId, and purchaseId if not explicitly provided.
 */
export function createPurchaseCompletedEnvelope(
  input: CreatePurchaseCompletedInput,
): PurchaseCompletedEnvelopeV1 {
  const eventId = input.eventId ?? uuidv7();
  const correlationId = input.correlationId ?? uuidv7();
  const purchaseId = input.purchaseId ?? `pur_${uuidv7()}`;
  const occurredAt = input.occurredAt ?? new Date().toISOString();

  return PurchaseCompletedEnvelopeSchemaV1.parse({
    schemaVersion: 1,
    eventId,
    eventType: 'PurchaseCompleted',
    aggregateType: 'CustomerPurchaseStream',
    aggregateId: input.customerId,
    customerSequence: input.customerSequence,
    occurredAt,
    correlationId,
    payload: {
      purchaseId,
      customerId: input.customerId,
      amountCents: input.amountCents,
    },
  });
}

export interface CreatePurchaseInvalidatedInput {
  customerId: string;
  purchaseId: string;
  reason: PurchaseInvalidatedReasonV1;
  customerSequence: number;
  eventId?: string;
  correlationId?: string;
  occurredAt?: string;
}

/**
 * Safe factory to construct a PurchaseInvalidated envelope.
 * Eliminates inconsistency risks by accepting customerId once and synchronizing aggregateId and payload.
 * Generates RFC 9562 UUIDv7 by default for eventId and correlationId if not explicitly provided.
 */
export function createPurchaseInvalidatedEnvelope(
  input: CreatePurchaseInvalidatedInput,
): PurchaseInvalidatedEnvelopeV1 {
  const eventId = input.eventId ?? uuidv7();
  const correlationId = input.correlationId ?? uuidv7();
  const occurredAt = input.occurredAt ?? new Date().toISOString();

  return PurchaseInvalidatedEnvelopeSchemaV1.parse({
    schemaVersion: 1,
    eventId,
    eventType: 'PurchaseInvalidated',
    aggregateType: 'CustomerPurchaseStream',
    aggregateId: input.customerId,
    customerSequence: input.customerSequence,
    occurredAt,
    correlationId,
    payload: {
      purchaseId: input.purchaseId,
      customerId: input.customerId,
      reason: input.reason,
    },
  });
}

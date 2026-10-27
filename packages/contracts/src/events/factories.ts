import {
  PurchaseCompletedEnvelopeSchemaV1,
  PurchaseCompletedEnvelopeV1,
  PurchaseInvalidatedEnvelopeSchemaV1,
  PurchaseInvalidatedEnvelopeV1,
  PurchaseInvalidatedReasonV1,
} from './envelope.v1';

export interface CreatePurchaseCompletedInput {
  eventId: string;
  customerId: string;
  purchaseId: string;
  amountCents: number;
  customerSequence: number;
  correlationId: string;
  occurredAt?: string;
}

/**
 * Safe factory to construct a PurchaseCompleted envelope.
 * Eliminates inconsistency risks by accepting customerId once and synchronizing aggregateId and payload.
 */
export function createPurchaseCompletedEnvelope(
  input: CreatePurchaseCompletedInput,
): PurchaseCompletedEnvelopeV1 {
  const occurredAt = input.occurredAt ?? new Date().toISOString();

  return PurchaseCompletedEnvelopeSchemaV1.parse({
    schemaVersion: 1,
    eventId: input.eventId,
    eventType: 'PurchaseCompleted',
    aggregateType: 'CustomerPurchaseStream',
    aggregateId: input.customerId,
    customerSequence: input.customerSequence,
    occurredAt,
    correlationId: input.correlationId,
    payload: {
      purchaseId: input.purchaseId,
      customerId: input.customerId,
      amountCents: input.amountCents,
    },
  });
}

export interface CreatePurchaseInvalidatedInput {
  eventId: string;
  customerId: string;
  purchaseId: string;
  reason: PurchaseInvalidatedReasonV1;
  customerSequence: number;
  correlationId: string;
  occurredAt?: string;
}

/**
 * Safe factory to construct a PurchaseInvalidated envelope.
 * Eliminates inconsistency risks by accepting customerId once and synchronizing aggregateId and payload.
 */
export function createPurchaseInvalidatedEnvelope(
  input: CreatePurchaseInvalidatedInput,
): PurchaseInvalidatedEnvelopeV1 {
  const occurredAt = input.occurredAt ?? new Date().toISOString();

  return PurchaseInvalidatedEnvelopeSchemaV1.parse({
    schemaVersion: 1,
    eventId: input.eventId,
    eventType: 'PurchaseInvalidated',
    aggregateType: 'CustomerPurchaseStream',
    aggregateId: input.customerId,
    customerSequence: input.customerSequence,
    occurredAt,
    correlationId: input.correlationId,
    payload: {
      purchaseId: input.purchaseId,
      customerId: input.customerId,
      reason: input.reason,
    },
  });
}

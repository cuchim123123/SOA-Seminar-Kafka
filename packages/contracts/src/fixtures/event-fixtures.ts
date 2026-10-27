import {
  PurchaseCompletedEnvelopeV1,
  PurchaseCompletedPayloadV1,
  PurchaseInvalidatedEnvelopeV1,
  PurchaseInvalidatedPayloadV1,
} from '../events/envelope.v1';

export type PurchaseCompletedFixtureOverrides = Partial<
  Omit<PurchaseCompletedEnvelopeV1, 'payload'>
> & {
  payload?: Partial<PurchaseCompletedPayloadV1>;
};

export function createPurchaseCompletedFixture(
  overrides: PurchaseCompletedFixtureOverrides = {},
): PurchaseCompletedEnvelopeV1 {
  const { payload: payloadOverrides, ...envelopeOverrides } = overrides;

  // Single authoritative customerId resolved once to ensure aggregateId === payload.customerId
  const customerId = envelopeOverrides.aggregateId ?? payloadOverrides?.customerId ?? 'U001';

  return {
    schemaVersion: 1,
    eventId: 'evt-0001',
    eventType: 'PurchaseCompleted',
    aggregateType: 'CustomerPurchaseStream',
    customerSequence: 1,
    occurredAt: '2026-10-27T07:00:00.000Z',
    correlationId: 'req-0001',
    ...envelopeOverrides,
    aggregateId: customerId,
    payload: {
      purchaseId: 'P001',
      amountCents: 12000,
      ...payloadOverrides,
      customerId, // strictly synchronized with aggregateId
    },
  };
}

export type PurchaseInvalidatedFixtureOverrides = Partial<
  Omit<PurchaseInvalidatedEnvelopeV1, 'payload'>
> & {
  payload?: Partial<PurchaseInvalidatedPayloadV1>;
};

export function createPurchaseInvalidatedFixture(
  overrides: PurchaseInvalidatedFixtureOverrides = {},
): PurchaseInvalidatedEnvelopeV1 {
  const { payload: payloadOverrides, ...envelopeOverrides } = overrides;

  // Single authoritative customerId resolved once to ensure aggregateId === payload.customerId
  const customerId = envelopeOverrides.aggregateId ?? payloadOverrides?.customerId ?? 'U001';

  return {
    schemaVersion: 1,
    eventId: 'evt-0004',
    eventType: 'PurchaseInvalidated',
    aggregateType: 'CustomerPurchaseStream',
    customerSequence: 4,
    occurredAt: '2026-10-27T07:15:00.000Z',
    correlationId: 'req-0004',
    ...envelopeOverrides,
    aggregateId: customerId,
    payload: {
      purchaseId: 'P001',
      reason: 'DEMO_INVALIDATION',
      ...payloadOverrides,
      customerId, // strictly synchronized with aggregateId
    },
  };
}

export const DEMO_U001_EVENT_SERIES = [
  createPurchaseCompletedFixture({
    eventId: 'evt-0001',
    customerSequence: 1,
    occurredAt: '2026-10-27T07:00:00.000Z',
    correlationId: 'req-0001',
    payload: { purchaseId: 'P001', amountCents: 12000 },
  }),
  createPurchaseCompletedFixture({
    eventId: 'evt-0002',
    customerSequence: 2,
    occurredAt: '2026-10-27T07:05:00.000Z',
    correlationId: 'req-0002',
    payload: { purchaseId: 'P002', amountCents: 20000 },
  }),
  createPurchaseCompletedFixture({
    eventId: 'evt-0003',
    customerSequence: 3,
    occurredAt: '2026-10-27T07:10:00.000Z',
    correlationId: 'req-0003',
    payload: { purchaseId: 'P003', amountCents: 10000 },
  }),
  createPurchaseInvalidatedFixture({
    eventId: 'evt-0004',
    customerSequence: 4,
    occurredAt: '2026-10-27T07:15:00.000Z',
    correlationId: 'req-0004',
    payload: { purchaseId: 'P001', reason: 'DEMO_INVALIDATION' },
  }),
] as const;

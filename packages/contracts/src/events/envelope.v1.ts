import { z } from 'zod';

export type PurchaseEventType = 'PurchaseCompleted' | 'PurchaseInvalidated';

export const PurchaseCompletedPayloadSchemaV1 = z.object({
  purchaseId: z.string().min(1, 'purchaseId must not be empty'),
  customerId: z.string().min(1, 'customerId must not be empty'),
  amountCents: z
    .number()
    .int('amountCents must be an integer')
    .positive('amountCents must be greater than zero')
    .max(Number.MAX_SAFE_INTEGER, 'amountCents exceeds MAX_SAFE_INTEGER'),
});

export type PurchaseCompletedPayloadV1 = z.infer<typeof PurchaseCompletedPayloadSchemaV1>;

export const PurchaseInvalidatedReasonSchemaV1 = z.enum(['FRAUD_CONFIRMED', 'DEMO_INVALIDATION']);
export type PurchaseInvalidatedReasonV1 = z.infer<typeof PurchaseInvalidatedReasonSchemaV1>;

export const PurchaseInvalidatedPayloadSchemaV1 = z.object({
  purchaseId: z.string().min(1, 'purchaseId must not be empty'),
  customerId: z.string().min(1, 'customerId must not be empty'),
  reason: PurchaseInvalidatedReasonSchemaV1,
});

export type PurchaseInvalidatedPayloadV1 = z.infer<typeof PurchaseInvalidatedPayloadSchemaV1>;

const BaseEnvelopeSchemaV1 = z.object({
  schemaVersion: z.literal(1),
  eventId: z.string().min(1, 'eventId must not be empty'),
  aggregateType: z.literal('CustomerPurchaseStream'),
  aggregateId: z.string().min(1, 'aggregateId must not be empty'),
  customerSequence: z
    .number()
    .int('customerSequence must be an integer')
    .min(1, 'customerSequence must be >= 1'),
  occurredAt: z.string().datetime({ message: 'occurredAt must be a valid ISO 8601 UTC timestamp' }),
  correlationId: z.string().min(1, 'correlationId must not be empty'),
});

export const BasePurchaseCompletedEnvelopeSchemaV1 = BaseEnvelopeSchemaV1.extend({
  eventType: z.literal('PurchaseCompleted'),
  payload: PurchaseCompletedPayloadSchemaV1,
});

export const BasePurchaseInvalidatedEnvelopeSchemaV1 = BaseEnvelopeSchemaV1.extend({
  eventType: z.literal('PurchaseInvalidated'),
  payload: PurchaseInvalidatedPayloadSchemaV1,
});

export const PurchaseCompletedEnvelopeSchemaV1 = BasePurchaseCompletedEnvelopeSchemaV1.refine(
  (env) => env.aggregateId === env.payload.customerId,
  {
    message: 'aggregateId must equal payload.customerId',
    path: ['aggregateId'],
  },
);

export type PurchaseCompletedEnvelopeV1 = z.infer<typeof BasePurchaseCompletedEnvelopeSchemaV1>;

export const PurchaseInvalidatedEnvelopeSchemaV1 = BasePurchaseInvalidatedEnvelopeSchemaV1.refine(
  (env) => env.aggregateId === env.payload.customerId,
  {
    message: 'aggregateId must equal payload.customerId',
    path: ['aggregateId'],
  },
);

export type PurchaseInvalidatedEnvelopeV1 = z.infer<typeof BasePurchaseInvalidatedEnvelopeSchemaV1>;

export const RawIntegrationEventEnvelopeSchemaV1 = z.discriminatedUnion('eventType', [
  BasePurchaseCompletedEnvelopeSchemaV1,
  BasePurchaseInvalidatedEnvelopeSchemaV1,
]);

export const IntegrationEventEnvelopeSchemaV1 = RawIntegrationEventEnvelopeSchemaV1.refine(
  (env) => env.aggregateId === env.payload.customerId,
  {
    message: 'aggregateId must equal payload.customerId',
    path: ['aggregateId'],
  },
);

export type IntegrationEventEnvelopeV1 = z.infer<typeof RawIntegrationEventEnvelopeSchemaV1>;

export interface GenericIntegrationEventEnvelopeV1<TPayload> {
  schemaVersion: 1;
  eventId: string;
  eventType: PurchaseEventType;
  aggregateType: 'CustomerPurchaseStream';
  aggregateId: string;
  customerSequence: number;
  occurredAt: string;
  correlationId: string;
  payload: TPayload;
}

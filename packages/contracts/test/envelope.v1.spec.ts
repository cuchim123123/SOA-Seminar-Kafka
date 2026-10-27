import { describe, expect, it } from 'vitest';
import {
  ContractValidationError,
  DEMO_U001_EVENT_SERIES,
  createPurchaseCompletedEnvelope,
  createPurchaseCompletedFixture,
  createPurchaseInvalidatedEnvelope,
  createPurchaseInvalidatedFixture,
  deserializeEnvelope,
  getEventCustomerId,
  getEventPartitionKey,
  isIntegrationEventEnvelopeV1,
  serializeEnvelope,
} from '../src';

describe('IntegrationEventEnvelopeV1 Contracts & Validation', () => {
  describe('Positive Validation & Serialization', () => {
    it('accepts valid PurchaseCompleted envelope', () => {
      const fixture = createPurchaseCompletedFixture();
      expect(isIntegrationEventEnvelopeV1(fixture)).toBe(true);

      const serialized = serializeEnvelope(fixture);
      expect(typeof serialized).toBe('string');

      const deserialized = deserializeEnvelope(serialized);
      expect(deserialized).toEqual(fixture);
    });

    it('accepts valid PurchaseInvalidated envelope', () => {
      const fixture = createPurchaseInvalidatedFixture();
      expect(isIntegrationEventEnvelopeV1(fixture)).toBe(true);

      const serialized = serializeEnvelope(fixture);
      const deserialized = deserializeEnvelope(serialized);
      expect(deserialized).toEqual(fixture);
    });

    it('deserializes Buffer input correctly', () => {
      const fixture = createPurchaseCompletedFixture();
      const buffer = Buffer.from(JSON.stringify(fixture), 'utf-8');

      const deserialized = deserializeEnvelope(buffer);
      expect(deserialized).toEqual(fixture);
    });

    it('validates the entire canonical DEMO_U001_EVENT_SERIES', () => {
      expect(DEMO_U001_EVENT_SERIES).toHaveLength(4);

      DEMO_U001_EVENT_SERIES.forEach((event, idx) => {
        expect(isIntegrationEventEnvelopeV1(event)).toBe(true);
        expect(event.customerSequence).toBe(idx + 1);
        expect(event.aggregateId).toBe('U001');
        expect(getEventPartitionKey(event)).toBe('U001');
        expect(getEventCustomerId(event)).toBe('U001');
      });

      expect(DEMO_U001_EVENT_SERIES[0].eventType).toBe('PurchaseCompleted');
      expect(DEMO_U001_EVENT_SERIES[1].eventType).toBe('PurchaseCompleted');
      expect(DEMO_U001_EVENT_SERIES[2].eventType).toBe('PurchaseCompleted');
      expect(DEMO_U001_EVENT_SERIES[3].eventType).toBe('PurchaseInvalidated');
    });
  });

  describe('AggregateId Architecture & Single-Source-of-Truth Enforcement', () => {
    it('constructs consistent PurchaseCompleted envelope from single customerId', () => {
      const envelope = createPurchaseCompletedEnvelope({
        eventId: 'evt-custom-01',
        customerId: 'U002',
        purchaseId: 'P100',
        amountCents: 15000,
        customerSequence: 1,
        correlationId: 'corr-01',
      });

      expect(envelope.aggregateId).toBe('U002');
      expect(envelope.payload.customerId).toBe('U002');
      expect(getEventPartitionKey(envelope)).toBe('U002');
      expect(getEventCustomerId(envelope)).toBe('U002');
      expect(isIntegrationEventEnvelopeV1(envelope)).toBe(true);
    });

    it('constructs consistent PurchaseInvalidated envelope from single customerId', () => {
      const envelope = createPurchaseInvalidatedEnvelope({
        eventId: 'evt-custom-02',
        customerId: 'U002',
        purchaseId: 'P100',
        reason: 'DEMO_INVALIDATION',
        customerSequence: 2,
        correlationId: 'corr-02',
      });

      expect(envelope.aggregateId).toBe('U002');
      expect(envelope.payload.customerId).toBe('U002');
      expect(getEventPartitionKey(envelope)).toBe('U002');
      expect(isIntegrationEventEnvelopeV1(envelope)).toBe(true);
    });

    it('fixture builder synchronizes aggregateId and payload.customerId on aggregateId override', () => {
      const fixture = createPurchaseCompletedFixture({
        aggregateId: 'U999',
      });

      expect(fixture.aggregateId).toBe('U999');
      expect(fixture.payload.customerId).toBe('U999');
      expect(isIntegrationEventEnvelopeV1(fixture)).toBe(true);
    });

    it('fixture builder synchronizes aggregateId and payload.customerId on payload override', () => {
      const fixture = createPurchaseCompletedFixture({
        payload: {
          purchaseId: 'P999',
          amountCents: 50000,
        },
      });

      expect(fixture.aggregateId).toBe('U001');
      expect(fixture.payload.customerId).toBe('U001');
      expect(fixture.payload.purchaseId).toBe('P999');
      expect(fixture.payload.amountCents).toBe(50000);
      expect(isIntegrationEventEnvelopeV1(fixture)).toBe(true);
    });
  });

  describe('Negative Validation & Invariants', () => {
    it('rejects unsupported schemaVersion', () => {
      const raw = {
        ...createPurchaseCompletedFixture(),
        schemaVersion: 2,
      };

      expect(isIntegrationEventEnvelopeV1(raw)).toBe(false);
      expect(() => deserializeEnvelope(raw)).toThrow(ContractValidationError);
    });

    it('rejects aggregateId mismatch where aggregateId != payload.customerId', () => {
      const raw = {
        ...createPurchaseCompletedFixture(),
        aggregateId: 'U999',
        payload: {
          purchaseId: 'P001',
          customerId: 'U001',
          amountCents: 12000,
        },
      };

      expect(isIntegrationEventEnvelopeV1(raw)).toBe(false);
      expect(() => deserializeEnvelope(raw)).toThrow(ContractValidationError);
      expect(() => deserializeEnvelope(raw)).toThrowError(
        /aggregateId must equal payload\.customerId/,
      );
    });

    it('rejects invalid amountCents (zero, negative, float, non-number, overflow)', () => {
      const base = createPurchaseCompletedFixture();

      // Zero
      expect(() =>
        deserializeEnvelope({
          ...base,
          payload: { ...base.payload, amountCents: 0 },
        }),
      ).toThrow(ContractValidationError);

      // Negative
      expect(() =>
        deserializeEnvelope({
          ...base,
          payload: { ...base.payload, amountCents: -500 },
        }),
      ).toThrow(ContractValidationError);

      // Float / fractional cents
      expect(() =>
        deserializeEnvelope({
          ...base,
          payload: { ...base.payload, amountCents: 120.5 },
        }),
      ).toThrow(ContractValidationError);

      // String cents
      expect(() =>
        deserializeEnvelope({
          ...base,
          payload: { ...base.payload, amountCents: '12000' as unknown as number },
        }),
      ).toThrow(ContractValidationError);

      // Exceeds MAX_SAFE_INTEGER
      expect(() =>
        deserializeEnvelope({
          ...base,
          payload: { ...base.payload, amountCents: Number.MAX_SAFE_INTEGER + 1000 },
        }),
      ).toThrow(ContractValidationError);
    });

    it('rejects invalid customerSequence (zero, negative, float)', () => {
      const base = createPurchaseCompletedFixture();

      expect(() =>
        deserializeEnvelope({
          ...base,
          customerSequence: 0,
        }),
      ).toThrow(ContractValidationError);

      expect(() =>
        deserializeEnvelope({
          ...base,
          customerSequence: -1,
        }),
      ).toThrow(ContractValidationError);

      expect(() =>
        deserializeEnvelope({
          ...base,
          customerSequence: 1.5,
        }),
      ).toThrow(ContractValidationError);
    });

    it('rejects invalid occurredAt timestamps', () => {
      const base = createPurchaseCompletedFixture();

      expect(() =>
        deserializeEnvelope({
          ...base,
          occurredAt: 'not-a-timestamp',
        }),
      ).toThrow(ContractValidationError);
    });

    it('rejects invalid invalidation reason', () => {
      const base = createPurchaseInvalidatedFixture();

      expect(() =>
        deserializeEnvelope({
          ...base,
          payload: {
            ...base.payload,
            reason: 'SOME_RANDOM_REASON' as unknown as 'FRAUD_CONFIRMED',
          },
        }),
      ).toThrow(ContractValidationError);
    });

    it('rejects malformed raw JSON strings', () => {
      expect(() => deserializeEnvelope('{ invalid json')).toThrow(ContractValidationError);
    });
  });
});

import { describe, expect, it } from 'vitest';
import {
  ReplayCustomerCorrector,
  ReplayInputEvent,
} from '../../../src/replay/domain/replay-customer-corrector';

describe('ReplayCustomerCorrector (P5-03)', () => {
  it('correctly recalculates points from 82 to 40 when P001 ($120) is invalidated', () => {
    const events: ReplayInputEvent[] = [
      {
        eventType: 'PurchaseCompleted',
        aggregateId: 'U001',
        customerSequence: 1,
        payload: {
          customerId: 'U001',
          purchaseId: 'P001',
          amountCents: 12000,
        },
      },
      {
        eventType: 'PurchaseCompleted',
        aggregateId: 'U001',
        customerSequence: 2,
        payload: {
          customerId: 'U001',
          purchaseId: 'P002',
          amountCents: 20000,
        },
      },
      {
        eventType: 'PurchaseCompleted',
        aggregateId: 'U001',
        customerSequence: 3,
        payload: {
          customerId: 'U001',
          purchaseId: 'P003',
          amountCents: 10000,
        },
      },
      {
        eventType: 'PurchaseInvalidated',
        aggregateId: 'U001',
        customerSequence: 4,
        payload: {
          customerId: 'U001',
          purchaseId: 'P001',
          reason: 'DEMO_INVALIDATION',
        },
      },
    ];

    const result = ReplayCustomerCorrector.executeTwoPassCorrection({
      runId: 'run-p5-03',
      events,
    });

    expect(result.invalidatedPurchaseIds.has('P001')).toBe(true);
    expect(result.stagingRecords).toHaveLength(1);

    const record = result.stagingRecords[0];
    expect(record.customerId).toBe('U001');
    expect(record.points).toBe(40n); // 40 points, NOT 70!
    expect(record.tier).toBe('GOLD');
    expect(record.eligibleSpendCents).toBe(30000n);
    expect(record.scannedEventCount).toBe(4);
    expect(record.lastCustomerSequence).toBe(4n);
  });

  it('calculates full baseline 82 points when no purchases are invalidated', () => {
    const events: ReplayInputEvent[] = [
      {
        eventType: 'PurchaseCompleted',
        aggregateId: 'U001',
        customerSequence: 1,
        payload: {
          customerId: 'U001',
          purchaseId: 'P001',
          amountCents: 12000,
        },
      },
      {
        eventType: 'PurchaseCompleted',
        aggregateId: 'U001',
        customerSequence: 2,
        payload: {
          customerId: 'U001',
          purchaseId: 'P002',
          amountCents: 20000,
        },
      },
      {
        eventType: 'PurchaseCompleted',
        aggregateId: 'U001',
        customerSequence: 3,
        payload: {
          customerId: 'U001',
          purchaseId: 'P003',
          amountCents: 10000,
        },
      },
    ];

    const result = ReplayCustomerCorrector.executeTwoPassCorrection({
      runId: 'run-baseline',
      events,
    });

    expect(result.invalidatedPurchaseIds.size).toBe(0);
    const record = result.stagingRecords[0];
    expect(record.customerId).toBe('U001');
    expect(record.points).toBe(82n); // 12 + 40 + 30 = 82
    expect(record.tier).toBe('GOLD');
    expect(record.eligibleSpendCents).toBe(42000n);
  });

  it('isolates calculations between different customers', () => {
    const events: ReplayInputEvent[] = [
      {
        eventType: 'PurchaseCompleted',
        aggregateId: 'CUST_A',
        customerSequence: 1,
        payload: {
          customerId: 'CUST_A',
          purchaseId: 'PA_1',
          amountCents: 10000,
        },
      },
      {
        eventType: 'PurchaseCompleted',
        aggregateId: 'CUST_B',
        customerSequence: 1,
        payload: {
          customerId: 'CUST_B',
          purchaseId: 'PB_1',
          amountCents: 10000,
        },
      },
      {
        eventType: 'PurchaseInvalidated',
        aggregateId: 'CUST_A',
        customerSequence: 2,
        payload: {
          customerId: 'CUST_A',
          purchaseId: 'PA_1',
        },
      },
    ];

    const result = ReplayCustomerCorrector.executeTwoPassCorrection({
      runId: 'run-multi',
      events,
    });

    expect(result.stagingRecords).toHaveLength(2);
    const custA = result.stagingRecords.find((r) => r.customerId === 'CUST_A');
    const custB = result.stagingRecords.find((r) => r.customerId === 'CUST_B');

    expect(custA?.points).toBe(0n); // PA_1 was invalidated
    expect(custB?.points).toBe(10n); // PB_1 is valid (10,000 / 1,000 * 1 = 10)
  });
});

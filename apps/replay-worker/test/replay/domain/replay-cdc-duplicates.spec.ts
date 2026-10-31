import { describe, expect, it } from 'vitest';
import {
  ReplayCustomerCorrector,
  ReplayInputEvent,
} from '../../../src/replay/domain/replay-customer-corrector';

describe('CDC redelivery during replay', () => {
  const event: ReplayInputEvent = {
    eventId: 'stable-event',
    eventType: 'PurchaseCompleted',
    aggregateId: 'U001',
    customerSequence: 1,
    payload: { customerId: 'U001', purchaseId: 'P001', amountCents: 12000 },
  };
  it('applies a repeated event once, including tier-dependent points', () => {
    const result = ReplayCustomerCorrector.executeTwoPassCorrection({
      runId: 'test',
      events: [event, event],
    });
    expect(result.stagingRecords[0].points).toBe(12n);
    expect(result.stagingRecords[0].eligibleSpendCents).toBe(12000n);
  });
  it('rejects conflicting reuse of an event identity', () => {
    expect(() =>
      ReplayCustomerCorrector.executeTwoPassCorrection({
        runId: 'test',
        events: [event, { ...event, customerSequence: 2 }],
      }),
    ).toThrow('Conflicting replay event identity');
  });
});

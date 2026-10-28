import { describe, expect, it } from 'vitest';
import { Purchase } from '../../../src/purchases/domain/purchase.entity';

describe('Purchase Domain Entity', () => {
  it('creates a valid purchase when all invariants are satisfied', () => {
    const purchase = Purchase.create({
      purchaseId: 'pur_123',
      customerId: 'cust_abc',
      amountCents: 2500,
      customerSequence: 1,
    });

    expect(purchase.purchaseId).toBe('pur_123');
    expect(purchase.customerId).toBe('cust_abc');
    expect(purchase.amountCents).toBe(2500);
    expect(purchase.customerSequence).toBe(1);
  });

  it('rejects empty customerId', () => {
    expect(() =>
      Purchase.create({
        purchaseId: 'pur_123',
        customerId: '',
        amountCents: 1000,
        customerSequence: 1,
      }),
    ).toThrowError(/customerId must not be empty/);

    expect(() =>
      Purchase.create({
        purchaseId: 'pur_123',
        customerId: '   ',
        amountCents: 1000,
        customerSequence: 1,
      }),
    ).toThrowError(/customerId must not be empty/);
  });

  it('rejects empty purchaseId', () => {
    expect(() =>
      Purchase.create({
        purchaseId: '',
        customerId: 'cust_abc',
        amountCents: 1000,
        customerSequence: 1,
      }),
    ).toThrowError(/purchaseId must not be empty/);
  });

  it('rejects non-positive or non-integer amountCents', () => {
    expect(() =>
      Purchase.create({
        purchaseId: 'pur_123',
        customerId: 'cust_abc',
        amountCents: 0,
        customerSequence: 1,
      }),
    ).toThrowError(/amountCents must be a positive integer/);

    expect(() =>
      Purchase.create({
        purchaseId: 'pur_123',
        customerId: 'cust_abc',
        amountCents: -500,
        customerSequence: 1,
      }),
    ).toThrowError(/amountCents must be a positive integer/);

    expect(() =>
      Purchase.create({
        purchaseId: 'pur_123',
        customerId: 'cust_abc',
        amountCents: 19.99,
        customerSequence: 1,
      }),
    ).toThrowError(/amountCents must be a positive integer/);
  });

  it('rejects non-positive or non-integer customerSequence', () => {
    expect(() =>
      Purchase.create({
        purchaseId: 'pur_123',
        customerId: 'cust_abc',
        amountCents: 1000,
        customerSequence: 0,
      }),
    ).toThrowError(/customerSequence must be a positive integer/);

    expect(() =>
      Purchase.create({
        purchaseId: 'pur_123',
        customerId: 'cust_abc',
        amountCents: 1000,
        customerSequence: -1,
      }),
    ).toThrowError(/customerSequence must be a positive integer/);
  });
});

import { describe, expect, it } from 'vitest';
import { SalesSummary } from '../../../src/analytics/domain/sales-summary.entity';

describe('SalesSummary Entity', () => {
  it('creates a fresh sales summary with zero initial values', () => {
    const summary = SalesSummary.create();

    expect(summary.totalRevenueCents).toBe(0);
    expect(summary.purchaseCount).toBe(0);
    expect(summary.uniqueCustomerCount).toBe(0);
    expect(summary.lastEventId).toBeNull();
    expect(summary.lastProcessedAt).toBeNull();
  });

  it('correctly aggregates multiple purchases for same and distinct customers', () => {
    const summary = SalesSummary.create();

    summary.applyPurchase(10000, 'CUST-001', 'evt-1');
    expect(summary.totalRevenueCents).toBe(10000);
    expect(summary.purchaseCount).toBe(1);
    expect(summary.uniqueCustomerCount).toBe(1);
    expect(summary.lastEventId).toBe('evt-1');
    expect(summary.lastProcessedAt).toBeInstanceOf(Date);

    // Second purchase from same customer
    summary.applyPurchase(5000, 'CUST-001', 'evt-2');
    expect(summary.totalRevenueCents).toBe(15000);
    expect(summary.purchaseCount).toBe(2);
    expect(summary.uniqueCustomerCount).toBe(1);
    expect(summary.lastEventId).toBe('evt-2');

    // Third purchase from distinct customer
    summary.applyPurchase(25000, 'CUST-002', 'evt-3');
    expect(summary.totalRevenueCents).toBe(40000);
    expect(summary.purchaseCount).toBe(3);
    expect(summary.uniqueCustomerCount).toBe(2);
    expect(summary.lastEventId).toBe('evt-3');
  });

  it('rejects invalid purchase amounts and empty inputs', () => {
    const summary = SalesSummary.create();

    expect(() => summary.applyPurchase(0, 'CUST-001', 'evt-1')).toThrow(
      'Invalid purchase amount: 0. Must be a positive integer.',
    );
    expect(() => summary.applyPurchase(-500, 'CUST-001', 'evt-1')).toThrow(
      'Invalid purchase amount: -500. Must be a positive integer.',
    );
    expect(() => summary.applyPurchase(12.34, 'CUST-001', 'evt-1')).toThrow(
      'Invalid purchase amount: 12.34. Must be a positive integer.',
    );
    expect(() => summary.applyPurchase(1000, '', 'evt-1')).toThrow(
      'customerId must be a non-empty string',
    );
    expect(() => summary.applyPurchase(1000, 'CUST-001', '')).toThrow(
      'eventId must be a non-empty string',
    );
  });

  it('reconstitutes an existing sales summary correctly', () => {
    const pastDate = new Date('2026-01-01T00:00:00Z');
    const summary = SalesSummary.reconstitute({
      totalRevenueCents: 50000,
      purchaseCount: 5,
      uniqueCustomerIds: new Set(['CUST-1', 'CUST-2']),
      lastEventId: 'evt-old',
      lastProcessedAt: pastDate,
    });

    expect(summary.totalRevenueCents).toBe(50000);
    expect(summary.purchaseCount).toBe(5);
    expect(summary.uniqueCustomerCount).toBe(2);
    expect(summary.lastEventId).toBe('evt-old');
    expect(summary.lastProcessedAt).toEqual(pastDate);
  });
});

import { describe, expect, it } from 'vitest';
import { parseEventReceiptIds } from '../src/queries/event-receipts.v1';

describe('receipt wire query', () => {
  it('bounds and validates before deduplicating', () => {
    expect(parseEventReceiptIds(' a, b,a ')).toMatchObject({ success: true, data: ['a', 'b'] });
    for (const input of [
      undefined,
      ['a'],
      '',
      'a,,b',
      'x'.repeat(129),
      Array(101).fill('a').join(','),
    ]) {
      expect(parseEventReceiptIds(input).success).toBe(false);
    }
  });
});

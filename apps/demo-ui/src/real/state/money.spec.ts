import { expect, it } from 'vitest';
import { parseAmountCents } from './money';
it('converts decimal amounts without floating point rounding', () => {
  expect(parseAmountCents('0.29')).toBe(29);
  expect(parseAmountCents('100.1')).toBe(10010);
  for (const value of ['0', '-1', '1.001', '1e3', 'Infinity', '9007199254740992'])
    expect(() => parseAmountCents(value)).toThrow();
});

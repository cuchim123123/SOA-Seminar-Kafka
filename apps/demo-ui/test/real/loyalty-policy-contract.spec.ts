import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { expect, it } from 'vitest';
import { RealApplication } from '../../src/real/RealApplication';
import { parseRoute } from '../../src/real/state/routes';
import {
  pointsForPurchase,
  tierForEligibleSpend,
} from '../../../loyalty-worker/src/loyalty/domain/loyalty-policy';

it('keeps presenter tier and points explanations consistent with the actual pure policy', () => {
  const html = renderToStaticMarkup(
    createElement(RealApplication, { route: parseRoute('#real/loyalty') }),
  );
  expect(html).toContain('Bronze: below 10,000 cents; Silver: from 10,000; Gold: from 30,000.');
  expect(tierForEligibleSpend(9999)).toBe('BRONZE');
  expect(tierForEligibleSpend(10000)).toBe('SILVER');
  expect(tierForEligibleSpend(29999)).toBe('SILVER');
  expect(tierForEligibleSpend(30000)).toBe('GOLD');
  expect(html).toContain('floor(purchase cents / 1,000)');
  expect(
    ['BRONZE', 'SILVER', 'GOLD'].map((tier) =>
      pointsForPurchase(1999, tier as 'BRONZE' | 'SILVER' | 'GOLD'),
    ),
  ).toEqual([1, 2, 3]);
});

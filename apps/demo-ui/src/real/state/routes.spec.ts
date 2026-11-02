import { describe, expect, it } from 'vitest';
import { parseRoute, routeHash } from './routes';

describe('workspace routes', () => {
  it('preserves legacy entry points and safely handles unknown pages', () => {
    for (const page of ['simulation', 'benchmark', 'guide']) {
      expect(parseRoute(`#${page}`)).toMatchObject({ page, real: false });
    }
    expect(parseRoute('#real/unknown')).toMatchObject({ page: 'simulation', real: false });
    expect(parseRoute('')).toMatchObject({ page: 'simulation', real: false });
  });

  it('round-trips encoded selection context across workspace navigation', () => {
    const route = {
      page: 'overview' as const,
      real: true,
      customerId: 'customer /?&é',
      purchaseId: 'purchase#1',
      runId: 'run+2',
    };
    expect(parseRoute(routeHash(route))).toEqual(route);
    const simulation = { ...route, page: 'simulation' as const, real: false };
    expect(parseRoute(routeHash(simulation))).toEqual(simulation);
  });
});

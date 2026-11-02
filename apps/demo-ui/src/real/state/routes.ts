export const realPages = [
  'overview',
  'purchases',
  'loyalty',
  'analytics',
  'events',
  'consumers',
  'recovery',
] as const;
export type RealPage = (typeof realPages)[number];
export interface WorkspaceRoute {
  page: 'simulation' | 'benchmark' | 'guide' | RealPage;
  real: boolean;
  customerId: string;
  purchaseId: string;
  runId: string;
}
export function parseRoute(hash: string): WorkspaceRoute {
  const [path, query = ''] = hash.replace(/^#/, '').split('?');
  const params = new URLSearchParams(query);
  const candidate = path.replace(/^real\//, '') as RealPage;
  const real = path.startsWith('real/') && realPages.includes(candidate);
  const page = real
    ? candidate
    : ['simulation', 'benchmark', 'guide'].includes(path)
      ? (path as 'simulation' | 'benchmark' | 'guide')
      : 'simulation';
  return {
    page,
    real,
    customerId: params.get('customerId') ?? '',
    purchaseId: params.get('purchaseId') ?? '',
    runId: params.get('runId') ?? '',
  };
}
export function routeHash(route: WorkspaceRoute): string {
  const query = new URLSearchParams();
  for (const key of ['customerId', 'purchaseId', 'runId'] as const)
    if (route[key]) query.set(key, route[key]);
  return `#${route.real ? 'real/' : ''}${route.page}${query.size ? `?${query}` : ''}`;
}

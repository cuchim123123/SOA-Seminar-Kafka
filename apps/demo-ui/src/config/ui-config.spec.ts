import { expect, it } from 'vitest';
import { uiConfigSchema } from './ui-config';
it('validates public links and bounded request timeout', () => {
  expect(uiConfigSchema.parse({}).VITE_GRAFANA_URL).toBe('http://localhost:3005');
  for (const value of ['javascript:alert(1)', 'ftp://host', 'http://user:password@host'])
    expect(uiConfigSchema.safeParse({ VITE_KAFBAT_URL: value }).success).toBe(false);
  expect(uiConfigSchema.safeParse({ VITE_HTTP_TIMEOUT_MS: 0 }).success).toBe(false);
});

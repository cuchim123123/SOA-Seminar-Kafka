import { z } from 'zod';

const publicUrl = z
  .string()
  .url()
  .refine((value) => {
    const url = new URL(value);
    return ['http:', 'https:'].includes(url.protocol) && !url.username && !url.password;
  }, 'Use an HTTP(S) URL without credentials');

export const uiConfigSchema = z.object({
  VITE_KAFBAT_URL: publicUrl.default('http://localhost:8080'),
  VITE_GRAFANA_URL: publicUrl.default('http://localhost:3005'),
  VITE_HTTP_TIMEOUT_MS: z.coerce.number().int().min(1000).max(600000).default(120000),
});

/** Public build-time settings only. Never expose database or broker credentials. */
export const uiConfig = uiConfigSchema.parse(import.meta.env ?? {});

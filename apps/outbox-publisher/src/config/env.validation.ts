import { z } from 'zod';

export const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z
    .string()
    .default('3003')
    .transform((val) => parseInt(val, 10))
    .pipe(z.number().int().min(1).max(65535)),
  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  KAFKA_CLIENT_ID: z.string().min(1).default('outbox-publisher-client'),
  PURCHASE_TOPIC: z.string().min(1).default('purchase.events'),
  DATABASE_URL: z
    .string()
    .min(1)
    .default('postgresql://postgres:postgres@localhost:5432/purchases_db?schema=public'),
  PURCHASES_DATABASE_URL: z.string().optional(),
  POLL_INTERVAL_MS: z
    .string()
    .default('250')
    .transform((val) => parseInt(val, 10))
    .pipe(z.number().int().min(50).max(60000)),
  BATCH_SIZE: z
    .string()
    .default('100')
    .transform((val) => parseInt(val, 10))
    .pipe(z.number().int().min(1).max(1000)),
  INITIAL_BACKOFF_MS: z
    .string()
    .default('250')
    .transform((val) => parseInt(val, 10))
    .pipe(z.number().int().min(50).max(10000)),
  MAX_BACKOFF_MS: z
    .string()
    .default('5000')
    .transform((val) => parseInt(val, 10))
    .pipe(z.number().int().min(100).max(60000)),
  FAULT_INJECTION_POINT: z.enum(['NONE', 'AFTER_KAFKA_ACK_BEFORE_OUTBOX_MARK']).default('NONE'),
  FAULT_TRIGGER_EVENT_ID: z.string().optional(),
});

export type EnvConfig = z.infer<typeof envSchema>;

export function validateEnv(env: Record<string, unknown> = process.env): EnvConfig {
  const result = envSchema.safeParse(env);
  if (!result.success) {
    const formatted = result.error.format();
    throw new Error(`Environment validation failed: ${JSON.stringify(formatted, null, 2)}`);
  }
  return result.data;
}

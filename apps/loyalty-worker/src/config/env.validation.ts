import { z } from 'zod';

const booleanFlagSchema = z.union([z.boolean(), z.string()]).transform((val) => {
  if (typeof val === 'boolean') return val;
  const lower = val.trim().toLowerCase();
  if (lower === 'true' || lower === '1') return true;
  if (lower === 'false' || lower === '0') return false;
  return false;
});

export const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z
    .string()
    .default('3001')
    .transform((val) => parseInt(val, 10))
    .pipe(z.number().int().min(1).max(65535)),
  LOYALTY_WORKER_PORT: z
    .string()
    .optional()
    .transform((val) => (val ? parseInt(val, 10) : undefined))
    .pipe(z.number().int().min(1).max(65535).optional()),
  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  KAFKA_CLIENT_ID: z.string().min(1).default('loyalty-worker'),
  KAFKA_GROUP_ID: z.string().min(1).default('loyalty-v1'),
  PURCHASE_TOPIC: z.string().min(1).default('purchase.events'),
  DATABASE_URL: z
    .string()
    .min(1)
    .default('postgresql://postgres:postgres@localhost:5433/loyalty_db?schema=public'),
  LOYALTY_DATABASE_URL: z.string().optional(),
  KAFKA_FROM_BEGINNING: booleanFlagSchema.default(true),
  FAULT_INJECTION_POINT: z.enum(['NONE', 'AFTER_DB_COMMIT_BEFORE_OFFSET_COMMIT']).default('NONE'),
  FAULT_TRIGGER_EVENT_ID: z.string().optional(),
});

export type EnvConfig = z.infer<typeof envSchema>;

export function validateEnv(env: Record<string, unknown> = process.env): EnvConfig {
  const result = envSchema.safeParse(env);
  if (!result.success) {
    const formatted = result.error.format();
    throw new Error(
      `Environment validation failed for loyalty-worker: ${JSON.stringify(formatted, null, 2)}`,
    );
  }
  return result.data;
}

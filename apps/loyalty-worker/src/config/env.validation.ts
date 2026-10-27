import { z } from 'zod';

export const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z
    .string()
    .default('3001')
    .transform((val) => parseInt(val, 10))
    .pipe(z.number().int().min(1).max(65535)),
  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  KAFKA_CLIENT_ID: z.string().min(1).default('loyalty-worker'),
  KAFKA_GROUP_ID: z.string().min(1).default('loyalty-v1'),
  PURCHASE_TOPIC: z.string().min(1).default('purchase.events'),
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

import { z } from 'zod';

export const createPurchaseBatchSchema = z.object({
  count: z.number().int().min(1).max(500).default(10),
  customerId: z.string().trim().min(1).max(128).optional(),
  minAmountCents: z.number().int().min(100).max(10000000).optional(),
  maxAmountCents: z.number().int().min(100).max(10000000).optional(),
  correlationId: z.string().uuid().optional(),
});

export type CreatePurchaseBatchDto = z.infer<typeof createPurchaseBatchSchema>;

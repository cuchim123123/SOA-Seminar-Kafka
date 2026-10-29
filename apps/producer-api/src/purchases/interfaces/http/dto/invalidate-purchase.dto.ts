import { z } from 'zod';
import type { PurchaseInvalidatedReasonV1 } from '@lab/contracts';

export const invalidatePurchaseSchema = z.object({
  reason: z
    .string()
    .optional()
    .transform((val): PurchaseInvalidatedReasonV1 => {
      if (!val) return 'DEMO_INVALIDATION';
      const upper = val.toUpperCase().trim();
      if (upper === 'FRAUD_CONFIRMED' || upper.includes('FRAUD')) return 'FRAUD_CONFIRMED';
      return 'DEMO_INVALIDATION';
    })
    .default('DEMO_INVALIDATION'),
  correlationId: z.string().uuid().optional(),
});

export type InvalidatePurchaseDto = z.infer<typeof invalidatePurchaseSchema>;

import { z } from 'zod';
import { PurchaseInvalidatedReasonSchemaV1 } from '@lab/contracts';

export const invalidatePurchaseSchema = z.object({
  reason: PurchaseInvalidatedReasonSchemaV1.default('DEMO_INVALIDATION').optional(),
  correlationId: z.string().uuid().optional(),
});

export type InvalidatePurchaseDto = z.infer<typeof invalidatePurchaseSchema>;

import { z } from 'zod';

export const createPurchaseSchema = z.object({
  customerId: z.string().trim().min(1, 'customerId must not be empty'),
  amountCents: z.number().int().positive('amountCents must be a positive integer'),
  correlationId: z.string().uuid().optional(),
  idempotencyKey: z.string().trim().min(1).optional(),
});

export type CreatePurchaseDto = z.infer<typeof createPurchaseSchema>;

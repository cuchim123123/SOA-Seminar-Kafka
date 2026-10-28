import { z } from 'zod';

export const purchasePropsSchema = z.object({
  purchaseId: z
    .string({ required_error: 'Purchase purchaseId must not be empty' })
    .trim()
    .min(1, 'Purchase purchaseId must not be empty'),
  customerId: z
    .string({ required_error: 'Purchase customerId must not be empty' })
    .trim()
    .min(1, 'Purchase customerId must not be empty'),
  amountCents: z
    .number({ required_error: 'Purchase amountCents must be a positive integer' })
    .int('Purchase amountCents must be a positive integer')
    .positive('Purchase amountCents must be a positive integer'),
  customerSequence: z
    .number({ required_error: 'Purchase customerSequence must be a positive integer' })
    .int('Purchase customerSequence must be a positive integer')
    .positive('Purchase customerSequence must be a positive integer'),
});

export type PurchaseProps = z.infer<typeof purchasePropsSchema>;

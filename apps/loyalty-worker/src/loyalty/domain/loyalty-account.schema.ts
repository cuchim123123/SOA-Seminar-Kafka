import { z } from 'zod';

export const customerIdSchema = z
  .string({ required_error: 'Loyalty account customerId must not be empty' })
  .trim()
  .min(1, 'Loyalty account customerId must not be empty');

export const tierEnumSchema = z.enum(['BRONZE', 'SILVER', 'GOLD']);

export const loyaltyAccountPropsSchema = z.object({
  customerId: customerIdSchema,
  tier: tierEnumSchema,
  totalEligibleSpendCents: z
    .number({ required_error: 'Total eligible spend must be a non-negative integer' })
    .int('Total eligible spend must be an integer')
    .nonnegative('Total eligible spend must be non-negative'),
  pointsBalance: z
    .number({ required_error: 'Points balance must be a non-negative integer' })
    .int('Points balance must be an integer')
    .nonnegative('Points balance must be non-negative'),
});

export type LoyaltyAccountProps = z.infer<typeof loyaltyAccountPropsSchema>;

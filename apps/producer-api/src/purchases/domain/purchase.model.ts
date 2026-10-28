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

export class Purchase {
  private constructor(private readonly props: PurchaseProps) {}

  public static create(rawProps: PurchaseProps): Purchase {
    const parseResult = purchasePropsSchema.safeParse(rawProps);
    if (!parseResult.success) {
      const issue = parseResult.error.errors[0];
      throw new Error(issue?.message ?? 'Invalid purchase properties');
    }
    return new Purchase(parseResult.data);
  }

  get purchaseId(): string {
    return this.props.purchaseId;
  }

  get customerId(): string {
    return this.props.customerId;
  }

  get amountCents(): number {
    return this.props.amountCents;
  }

  get customerSequence(): number {
    return this.props.customerSequence;
  }
}

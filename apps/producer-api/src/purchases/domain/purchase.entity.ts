import { PurchaseProps, purchasePropsSchema } from './purchase.schema';

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

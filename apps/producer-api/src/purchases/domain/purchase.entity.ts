export interface PurchaseProps {
  readonly purchaseId: string;
  readonly customerId: string;
  readonly amountCents: number;
  readonly customerSequence: number;
}

function assertNonEmpty(value: string | undefined | null, field: string): string {
  if (!value || value.trim().length === 0) {
    throw new Error(`Purchase ${field} must not be empty`);
  }
  return value.trim();
}

function assertPositiveInteger(value: number | undefined | null, field: string): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value <= 0) {
    throw new Error(`Purchase ${field} must be a positive integer`);
  }
  return value;
}

export class Purchase {
  private constructor(private readonly props: PurchaseProps) {}

  public static create(props: PurchaseProps): Purchase {
    return new Purchase({
      purchaseId: assertNonEmpty(props.purchaseId, 'purchaseId'),
      customerId: assertNonEmpty(props.customerId, 'customerId'),
      amountCents: assertPositiveInteger(props.amountCents, 'amountCents'),
      customerSequence: assertPositiveInteger(props.customerSequence, 'customerSequence'),
    });
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

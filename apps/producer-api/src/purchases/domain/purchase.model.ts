export interface PurchaseProps {
  readonly purchaseId: string;
  readonly customerId: string;
  readonly amountCents: number;
  readonly customerSequence: number;
}

export class Purchase {
  private constructor(private readonly props: PurchaseProps) {}

  public static create(props: PurchaseProps): Purchase {
    if (!props.customerId || props.customerId.trim().length === 0) {
      throw new Error('Purchase customerId must not be empty');
    }
    if (!props.purchaseId || props.purchaseId.trim().length === 0) {
      throw new Error('Purchase purchaseId must not be empty');
    }
    if (!Number.isInteger(props.amountCents) || props.amountCents <= 0) {
      throw new Error('Purchase amountCents must be a positive integer');
    }
    if (!Number.isInteger(props.customerSequence) || props.customerSequence <= 0) {
      throw new Error('Purchase customerSequence must be a positive integer');
    }
    return new Purchase(props);
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

export type PurchaseStatus = 'COMPLETED' | 'INVALIDATED';

export interface InvalidationDetails {
  readonly sequence: number;
  readonly eventId: string;
  readonly reason: string;
}

export interface PurchaseProps {
  readonly purchaseId: string;
  readonly customerId: string;
  readonly amountCents: number;
  readonly customerSequence: number;
  readonly status?: PurchaseStatus;
  readonly invalidationSequence?: number;
  readonly invalidationEventId?: string;
  readonly invalidationReason?: string;
  readonly createdAt?: string;
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
  private constructor(
    private readonly props: PurchaseProps & {
      readonly status: PurchaseStatus;
      readonly createdAt: string;
    },
  ) {}

  public static create(props: PurchaseProps): Purchase {
    return new Purchase({
      purchaseId: assertNonEmpty(props.purchaseId, 'purchaseId'),
      customerId: assertNonEmpty(props.customerId, 'customerId'),
      amountCents: assertPositiveInteger(props.amountCents, 'amountCents'),
      customerSequence: assertPositiveInteger(props.customerSequence, 'customerSequence'),
      status: props.status ?? 'COMPLETED',
      invalidationSequence: props.invalidationSequence,
      invalidationEventId: props.invalidationEventId,
      invalidationReason: props.invalidationReason,
      createdAt: props.createdAt ?? new Date().toISOString(),
    });
  }

  public static reconstitute(
    props: PurchaseProps & { status: PurchaseStatus; createdAt: string },
  ): Purchase {
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

  get status(): PurchaseStatus {
    return this.props.status;
  }

  get invalidationSequence(): number | undefined {
    return this.props.invalidationSequence;
  }

  get invalidationEventId(): string | undefined {
    return this.props.invalidationEventId;
  }

  get invalidationReason(): string | undefined {
    return this.props.invalidationReason;
  }

  get createdAt(): string {
    return this.props.createdAt;
  }

  public invalidate(details?: InvalidationDetails): Purchase {
    if (this.props.status === 'INVALIDATED') {
      return this;
    }
    return new Purchase({
      ...this.props,
      status: 'INVALIDATED',
      invalidationSequence: details?.sequence ?? this.props.invalidationSequence,
      invalidationEventId: details?.eventId ?? this.props.invalidationEventId,
      invalidationReason: details?.reason ?? this.props.invalidationReason,
    });
  }
}

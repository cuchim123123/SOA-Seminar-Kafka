export interface SalesSummaryProps {
  totalRevenueCents: number;
  purchaseCount: number;
  uniqueCustomerIds: Set<string>;
  lastEventId: string | null;
  lastProcessedAt: Date | null;
}

export class SalesSummary {
  private _totalRevenueCents: number;
  private _purchaseCount: number;
  private readonly _uniqueCustomerIds: Set<string>;
  private _lastEventId: string | null;
  private _lastProcessedAt: Date | null;

  private constructor(props: SalesSummaryProps) {
    this._totalRevenueCents = props.totalRevenueCents;
    this._purchaseCount = props.purchaseCount;
    this._uniqueCustomerIds = new Set(props.uniqueCustomerIds);
    this._lastEventId = props.lastEventId;
    this._lastProcessedAt = props.lastProcessedAt;
  }

  static create(): SalesSummary {
    return new SalesSummary({
      totalRevenueCents: 0,
      purchaseCount: 0,
      uniqueCustomerIds: new Set<string>(),
      lastEventId: null,
      lastProcessedAt: null,
    });
  }

  static reconstitute(props: SalesSummaryProps): SalesSummary {
    return new SalesSummary(props);
  }

  applyPurchase(amountCents: number, customerId: string, eventId: string): void {
    if (!Number.isSafeInteger(amountCents) || amountCents <= 0) {
      throw new Error(`Invalid purchase amount: ${amountCents}. Must be a positive integer.`);
    }
    if (!customerId || customerId.trim() === '') {
      throw new Error('customerId must be a non-empty string');
    }
    if (!eventId || eventId.trim() === '') {
      throw new Error('eventId must be a non-empty string');
    }

    this._totalRevenueCents += amountCents;
    this._purchaseCount += 1;
    this._uniqueCustomerIds.add(customerId);
    this._lastEventId = eventId;
    this._lastProcessedAt = new Date();
  }

  get totalRevenueCents(): number {
    return this._totalRevenueCents;
  }

  get purchaseCount(): number {
    return this._purchaseCount;
  }

  get uniqueCustomerCount(): number {
    return this._uniqueCustomerIds.size;
  }

  get uniqueCustomerIds(): Set<string> {
    return new Set(this._uniqueCustomerIds);
  }

  get lastEventId(): string | null {
    return this._lastEventId;
  }

  get lastProcessedAt(): Date | null {
    return this._lastProcessedAt;
  }
}

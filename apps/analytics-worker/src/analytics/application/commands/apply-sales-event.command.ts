export interface ApplySalesEventInput {
  readonly eventId: string;
  readonly customerId: string;
  readonly purchaseId: string;
  readonly amountCents: number;
  readonly customerSequence: number;
  readonly correlationId?: string;
}

export class ApplySalesEventCommand {
  constructor(public readonly data: ApplySalesEventInput) {}
}

export interface ApplySalesEventResult {
  readonly eventId: string;
  readonly totalRevenueCents: number;
  readonly purchaseCount: number;
  readonly uniqueCustomerCount: number;
}

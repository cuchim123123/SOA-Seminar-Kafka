import { PurchaseInvalidatedReasonV1 } from '@lab/contracts';

export interface InvalidateSyntheticPurchaseInput {
  readonly purchaseId: string;
  readonly reason?: PurchaseInvalidatedReasonV1;
  readonly correlationId?: string;
}

export interface InvalidateSyntheticPurchaseResult {
  readonly purchaseId: string;
  readonly customerId: string;
  readonly status: 'INVALIDATED';
  readonly customerSequence: number;
  readonly eventId: string;
  readonly reason: PurchaseInvalidatedReasonV1;
  readonly isAlreadyInvalidated: boolean;
}

export class InvalidateSyntheticPurchaseCommand {
  constructor(public readonly data: InvalidateSyntheticPurchaseInput) {}
}

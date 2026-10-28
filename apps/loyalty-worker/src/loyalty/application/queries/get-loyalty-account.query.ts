export interface GetLoyaltyAccountInput {
  readonly customerId: string;
}

export class GetLoyaltyAccountQuery {
  constructor(public readonly data: GetLoyaltyAccountInput) {}
}

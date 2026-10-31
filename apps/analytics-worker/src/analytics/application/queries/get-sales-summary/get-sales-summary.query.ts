export interface GetSalesSummaryInput {
  readonly filterDate?: string;
}

export class GetSalesSummaryQuery {
  constructor(public readonly data: GetSalesSummaryInput = {}) {}
}

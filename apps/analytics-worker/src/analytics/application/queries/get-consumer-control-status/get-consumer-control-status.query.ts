export interface GetConsumerControlStatusInput {
  readonly includePartitions?: boolean;
}

export class GetConsumerControlStatusQuery {
  constructor(public readonly data: GetConsumerControlStatusInput = {}) {}
}

export interface GetConsumerGroupLagInput {
  readonly groupId: string;
  readonly topic?: string;
}

export class GetConsumerGroupLagQuery {
  constructor(public readonly data: GetConsumerGroupLagInput) {}
}

export interface DescribeConsumerGroupInput {
  readonly groupId: string;
  readonly topic?: string;
}

export class DescribeConsumerGroupQuery {
  constructor(public readonly data: DescribeConsumerGroupInput) {}
}

export interface ScaleTopicPartitionsCommandData {
  readonly topic: string;
  readonly newPartitionCount: number;
}

export class ScaleTopicPartitionsCommand {
  constructor(public readonly data: ScaleTopicPartitionsCommandData) {}
}

export interface PeekPartitionMessagesQueryData {
  readonly topic: string;
  readonly partition: number;
  readonly limit?: number;
}

export class PeekPartitionMessagesQuery {
  constructor(public readonly data: PeekPartitionMessagesQueryData) {}
}

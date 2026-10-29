export interface ReplayStagingRecordProps {
  id: string;
  runId: string;
  customerId: string;
  points: bigint;
  tier: string;
  eligibleSpendCents: bigint;
  scannedEventCount: number;
  lastCustomerSequence: bigint;
  createdAt: Date;
}

export class ReplayStagingRecord {
  constructor(private readonly props: ReplayStagingRecordProps) {}

  get id(): string {
    return this.props.id;
  }

  get runId(): string {
    return this.props.runId;
  }

  get customerId(): string {
    return this.props.customerId;
  }

  get points(): bigint {
    return this.props.points;
  }

  get tier(): string {
    return this.props.tier;
  }

  get eligibleSpendCents(): bigint {
    return this.props.eligibleSpendCents;
  }

  get scannedEventCount(): number {
    return this.props.scannedEventCount;
  }

  get lastCustomerSequence(): bigint {
    return this.props.lastCustomerSequence;
  }

  get createdAt(): Date {
    return this.props.createdAt;
  }
}

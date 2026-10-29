export type ReplayRunStatus = 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED';

export interface ReplayRunProps {
  id: string;
  groupId: string;
  topic: string;
  status: ReplayRunStatus;
  startOffsets: Record<number, string>;
  highWatermarks: Record<number, string>;
  scannedCount: number;
  errorMessage?: string | null;
  startedAt?: Date | null;
  completedAt?: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

export class ReplayRun {
  private _status: ReplayRunStatus;
  private _scannedCount: number;
  private _errorMessage?: string | null;
  private _startedAt?: Date | null;
  private _completedAt?: Date | null;
  private _updatedAt: Date;

  constructor(private readonly props: ReplayRunProps) {
    this._status = props.status;
    this._scannedCount = props.scannedCount;
    this._errorMessage = props.errorMessage;
    this._startedAt = props.startedAt;
    this._completedAt = props.completedAt;
    this._updatedAt = props.updatedAt;
  }

  static create(params: {
    id: string;
    groupId: string;
    topic: string;
    startOffsets: Record<number, string>;
    highWatermarks: Record<number, string>;
  }): ReplayRun {
    const now = new Date();
    return new ReplayRun({
      id: params.id,
      groupId: params.groupId,
      topic: params.topic,
      status: 'PENDING',
      startOffsets: params.startOffsets,
      highWatermarks: params.highWatermarks,
      scannedCount: 0,
      createdAt: now,
      updatedAt: now,
    });
  }

  get id(): string {
    return this.props.id;
  }

  get groupId(): string {
    return this.props.groupId;
  }

  get topic(): string {
    return this.props.topic;
  }

  get status(): ReplayRunStatus {
    return this._status;
  }

  get startOffsets(): Record<number, string> {
    return { ...this.props.startOffsets };
  }

  get highWatermarks(): Record<number, string> {
    return { ...this.props.highWatermarks };
  }

  get scannedCount(): number {
    return this._scannedCount;
  }

  get errorMessage(): string | null | undefined {
    return this._errorMessage;
  }

  get startedAt(): Date | null | undefined {
    return this._startedAt;
  }

  get completedAt(): Date | null | undefined {
    return this._completedAt;
  }

  get createdAt(): Date {
    return this.props.createdAt;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  start(): void {
    if (this._status !== 'PENDING') {
      throw new Error(`Cannot start replay run in status '${this._status}'`);
    }
    this._status = 'RUNNING';
    this._startedAt = new Date();
    this._updatedAt = new Date();
  }

  recordScan(count: number): void {
    if (this._status !== 'RUNNING') {
      throw new Error(`Cannot record scan in status '${this._status}'`);
    }
    this._scannedCount += count;
    this._updatedAt = new Date();
  }

  complete(): void {
    if (this._status !== 'RUNNING') {
      throw new Error(`Cannot complete replay run in status '${this._status}'`);
    }
    this._status = 'COMPLETED';
    this._completedAt = new Date();
    this._updatedAt = new Date();
  }

  fail(reason: string): void {
    this._status = 'FAILED';
    this._errorMessage = reason;
    this._completedAt = new Date();
    this._updatedAt = new Date();
  }
}

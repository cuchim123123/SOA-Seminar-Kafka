export interface ReplayScannedEvent {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly key: string | null;
  readonly timestamp: string;
  readonly value: unknown;
}

export interface BoundedScanOptions {
  readonly topic: string;
  readonly groupId: string;
  readonly startOffsets: Record<number, string>;
  readonly highWatermarks: Record<number, string>;
  readonly onRecord?: (record: ReplayScannedEvent) => Promise<void>;
}

export interface BoundedScanResult {
  readonly scannedCount: number;
  readonly finalCommittedOffsets: Record<number, string>;
}

export const BOUNDED_REPLAY_SCANNER_PORT = Symbol('BOUNDED_REPLAY_SCANNER_PORT');

export interface BoundedReplayScannerPort {
  scanBoundedTopic(options: BoundedScanOptions): Promise<BoundedScanResult>;
}

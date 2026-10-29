import { DomainError } from '@lab/observability';

export interface PartitionRetentionDiagnostic {
  readonly partition: number;
  readonly earliestRetainedOffset: string;
  readonly requiredStartOffset: string;
  readonly purgedOffsetCount: string;
}

export interface InsufficientRetentionDetails {
  readonly topic: string;
  readonly truncatedPartitions: PartitionRetentionDiagnostic[];
  readonly startOffsets: Record<number, string>;
  readonly highWatermarks: Record<number, string>;
}

/**
 * Domain error thrown when Kafka topic log retention has purged history
 * earlier than the required replay horizon.
 */
export class InsufficientRetentionError extends DomainError {
  constructor(
    message: string,
    public readonly retentionDetails: InsufficientRetentionDetails,
  ) {
    super(message, retentionDetails as unknown as Record<string, unknown>);
    this.name = 'InsufficientRetentionError';
  }
}

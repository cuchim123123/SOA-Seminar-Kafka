export interface ReplayRunResponseDto {
  readonly id: string;
  readonly groupId: string;
  readonly topic: string;
  readonly status: string;
  readonly startOffsets: Record<number, string>;
  readonly highWatermarks: Record<number, string>;
  readonly scannedCount: number;
  readonly finalCommittedOffsets?: Record<number, string>;
  readonly errorMessage?: string | null;
  readonly startedAt?: string | null;
  readonly completedAt?: string | null;
}

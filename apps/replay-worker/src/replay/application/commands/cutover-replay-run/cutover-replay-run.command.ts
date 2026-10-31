export class CutoverReplayRunCommand {
  constructor(readonly runId: string) {}
}

export interface CutoverReplayRunResult {
  readonly runId: string;
  readonly status: 'CUTOVER_COMPLETED';
  readonly appliedCustomersCount: number;
  readonly cutoverAt: string;
}

export interface PauseAnalyticsConsumerInput {
  readonly reason?: string;
}

export class PauseAnalyticsConsumerCommand {
  constructor(public readonly data: PauseAnalyticsConsumerInput = {}) {}
}

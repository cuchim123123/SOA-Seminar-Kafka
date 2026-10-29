export interface ResumeAnalyticsConsumerInput {
  readonly reason?: string;
}

export class ResumeAnalyticsConsumerCommand {
  constructor(public readonly data: ResumeAnalyticsConsumerInput = {}) {}
}

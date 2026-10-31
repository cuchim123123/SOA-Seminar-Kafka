export interface StartBoundedReplayCommandProps {
  readonly runId?: string;
  readonly topic?: string;
  readonly requiredStartOffsets?: Record<number, string>;
  readonly simulateRetentionFailure?: boolean;
}

export class StartBoundedReplayCommand {
  readonly runId?: string;
  readonly topic?: string;
  readonly requiredStartOffsets?: Record<number, string>;
  readonly simulateRetentionFailure?: boolean;

  constructor(props: StartBoundedReplayCommandProps = {}) {
    this.runId = props.runId;
    this.topic = props.topic;
    this.requiredStartOffsets = props.requiredStartOffsets;
    this.simulateRetentionFailure = props.simulateRetentionFailure;
  }
}

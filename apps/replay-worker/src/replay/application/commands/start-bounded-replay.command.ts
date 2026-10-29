export interface StartBoundedReplayCommandProps {
  readonly runId?: string;
  readonly topic?: string;
  readonly requiredStartOffsets?: Record<number, string>;
}

export class StartBoundedReplayCommand {
  readonly runId?: string;
  readonly topic?: string;
  readonly requiredStartOffsets?: Record<number, string>;

  constructor(props: StartBoundedReplayCommandProps = {}) {
    this.runId = props.runId;
    this.topic = props.topic;
    this.requiredStartOffsets = props.requiredStartOffsets;
  }
}

export interface StartBoundedReplayCommandProps {
  readonly runId?: string;
  readonly topic?: string;
}

export class StartBoundedReplayCommand {
  readonly runId?: string;
  readonly topic?: string;

  constructor(props: StartBoundedReplayCommandProps = {}) {
    this.runId = props.runId;
    this.topic = props.topic;
  }
}

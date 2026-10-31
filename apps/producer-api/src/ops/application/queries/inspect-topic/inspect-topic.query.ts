export interface InspectTopicInput {
  readonly topic: string;
}

export class InspectTopicQuery {
  constructor(public readonly data: InspectTopicInput) {}
}

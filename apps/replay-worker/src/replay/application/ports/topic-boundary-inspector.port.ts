export interface TopicBoundaries {
  readonly topic: string;
  readonly startOffsets: Record<number, string>;
  readonly highWatermarks: Record<number, string>;
}

export const TOPIC_BOUNDARY_INSPECTOR_PORT = Symbol('TOPIC_BOUNDARY_INSPECTOR_PORT');

export interface TopicBoundaryInspectorPort {
  getTopicBoundaries(topic: string): Promise<TopicBoundaries>;
}

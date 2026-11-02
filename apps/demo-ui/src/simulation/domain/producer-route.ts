import type { SimCustomNode, SimCustomEdge } from './types';

/** An outbox source publishes to the topic selected by its CDC relay. */
export function producerTopic(
  producer: SimCustomNode | undefined,
  nodes: SimCustomNode[],
  edges: SimCustomEdge[],
): string {
  if (!producer) return '';
  const target = nodes.find((node) =>
    edges.some((edge) => edge.source === producer.id && edge.target === node.id),
  );
  if (target?.data.kind === 'outbox-table') {
    const relay = nodes.find(
      (node) =>
        node.data.kind === 'cdc-relay' &&
        edges.some((edge) => edge.source === target.id && edge.target === node.id),
    );
    return String(relay?.data.targetTopic ?? '');
  }
  return String(producer.data.targetTopic ?? '');
}

import type { SimCustomNode, SimCustomEdge } from './types';
import { SIMULATION_LIMITS } from './limits';

export function scaleConsumers(
  nodes: SimCustomNode[],
  edges: SimCustomEdge[],
  groupId: string,
  count: number,
  allocateId: () => string,
) {
  if (!Number.isInteger(count) || count < 1 || count > SIMULATION_LIMITS.replicasPerGroup)
    throw new Error('Choose 1–12 consumer instances.');
  const members = nodes.filter(
    (node) => node.data.kind === 'consumer' && node.data.groupId === groupId,
  );
  if (!members.length) throw new Error('Select an existing consumer group.');
  const removed = new Set(members.slice(count).map((node) => node.id));
  const nextNodes = nodes.filter((node) => !removed.has(node.id));
  const nextEdges = edges.filter((edge) => !removed.has(edge.source) && !removed.has(edge.target));
  const template = members[0];
  for (let index = members.length; index < count; index++) {
    const id = allocateId();
    if (nextNodes.some((node) => node.id === id))
      throw new Error('Generated instance ID already exists.');
    nextNodes.push({
      ...structuredClone(template),
      id,
      selected: false,
      position: { x: template.position.x, y: template.position.y + (index + 1) * 210 },
      data: {
        ...structuredClone(template.data),
        id,
        instanceId: id,
        title: `${groupId} worker ${index + 1}`,
        status: 'POLLING',
        assignedPartitions: [],
        isPaused: false,
        pausedPartitions: [],
        currentProcessingPartition: undefined,
        currentProcessingOffset: undefined,
        statusText: undefined,
      },
    });
    for (const edge of edges.filter(
      (edge) => edge.source === template.id || edge.target === template.id,
    )) {
      nextEdges.push({
        ...structuredClone(edge),
        id: `${edge.id}:${id}`,
        source: edge.source === template.id ? id : edge.source,
        target: edge.target === template.id ? id : edge.target,
      });
    }
  }
  return { nodes: nextNodes, edges: nextEdges };
}

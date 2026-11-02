import type { SimCustomNode, SimCustomEdge } from './types';
import { SIMULATION_LIMITS } from './limits';
import { isValidConnection } from '../registry/component-registry';

export function validateTopology(
  nodes: SimCustomNode[],
  edges: SimCustomEdge[],
  topics?: Set<string>,
): string[] {
  const issues: string[] = [];
  if (nodes.length > SIMULATION_LIMITS.components || edges.length > SIMULATION_LIMITS.wires)
    issues.push(
      `Use at most ${SIMULATION_LIMITS.components} components and ${SIMULATION_LIMITS.wires} wires in this teaching model.`,
    );
  const byId = new Map(nodes.map((node) => [node.id, node]));
  if (byId.size !== nodes.length) issues.push('Component IDs must be unique.');
  if (!nodes.some((node) => ['kafka-broker', 'kafka-cluster'].includes(node.data.kind)))
    issues.push('Add a broker or cluster before running the system.');
  const instances = nodes
    .filter((node) => node.data.kind === 'consumer')
    .map((node) => node.data.instanceId);
  if (
    instances.some((id) => typeof id !== 'string' || !id.trim()) ||
    new Set(instances).size !== instances.length
  )
    issues.push('Consumer instance IDs must be non-empty and unique.');
  const brokers = nodes.filter((node) => node.data.kind === 'kafka-broker');
  const ids = brokers.map((node) => node.data.brokerId);
  if (new Set(ids).size !== ids.length || ids.some((id) => !Number.isInteger(id) || Number(id) < 1))
    issues.push('Broker IDs must be unique positive integers.');
  const wires = new Set<string>();
  for (const edge of edges) {
    const key = `${edge.source}:${edge.target}`;
    if (wires.has(key)) issues.push(`Duplicate connection ${edge.source} → ${edge.target}.`);
    wires.add(key);
    const source = byId.get(edge.source),
      target = byId.get(edge.target);
    if (!source || !target) issues.push(`Connection ${edge.id} has a missing endpoint.`);
    else if (!isValidConnection(source.data.kind, target.data.kind))
      issues.push(`Unsupported connection ${source.data.title} → ${target.data.title}.`);
  }
  const groupCounts = new Map<string, number>();
  for (const node of nodes.filter((node) => node.data.kind === 'consumer')) {
    const groupId = String(node.data.groupId);
    groupCounts.set(groupId, (groupCounts.get(groupId) ?? 0) + 1);
  }
  for (const [id, count] of groupCounts)
    if (count > SIMULATION_LIMITS.replicasPerGroup)
      issues.push(`Group ${id}: use at most ${SIMULATION_LIMITS.replicasPerGroup} replicas.`);
  const groups = new Map<string, { topic: string; sink: string }>();
  for (const node of nodes) {
    for (const key of ['processingDelayMs', 'writeDelayMs'])
      if (
        key in node.data &&
        (!Number.isFinite(node.data[key]) ||
          Number(node.data[key]) < 0 ||
          Number(node.data[key]) > 60000)
      )
        issues.push(`${node.data.title}: ${key} must be between 0 and 60000 virtual ms.`);
    if (node.data.kind === 'worker-service')
      issues.push(`${node.data.title}: worker-service execution is not supported.`);
    if (node.data.kind !== 'consumer') continue;
    const groupId = String(node.data.groupId ?? '').trim();
    const topic = String(node.data.subscribedTopic ?? '').trim();
    const sinks = edges.filter(
      (edge) => edge.source === node.id && byId.get(edge.target)?.data.kind === 'database',
    );
    if (!groupId || !topic)
      issues.push(`${node.data.title}: group and subscribed topic are required.`);
    if (topics && !topics.has(topic))
      issues.push(`${node.data.title}: subscribed topic ${topic} does not exist.`);
    if (sinks.length !== 1) issues.push(`${node.data.title}: connect exactly one database sink.`);
    if (
      !edges.some(
        (edge) =>
          edge.target === node.id &&
          ['kafka-broker', 'kafka-cluster'].includes(String(byId.get(edge.source)?.data.kind)),
      )
    )
      issues.push(`${node.data.title}: connect a broker or cluster input.`);
    const previous = groups.get(groupId);
    const sink = sinks[0]?.target ?? '';
    if (previous && (previous.topic !== topic || previous.sink !== sink))
      issues.push(
        `Group ${groupId}: replicas must share one subscribed topic and database sink. Use another group for an independent projection.`,
      );
    groups.set(groupId, { topic, sink });
  }
  for (const node of nodes.filter((node) =>
    ['producer', 'outbox-table', 'cdc-relay'].includes(node.data.kind),
  )) {
    const targets = edges
      .filter((edge) => edge.source === node.id)
      .map((edge) => byId.get(edge.target))
      .filter(Boolean);
    if (targets.length === 0) issues.push(`${node.data.title}: connect an output before running.`);
    if (
      node.data.kind === 'producer' &&
      targets.some((target) => target?.data.kind === 'outbox-table') &&
      targets.length !== 1
    )
      issues.push(`${node.data.title}: choose either one outbox or broker bootstrap connections.`);
    if (node.data.kind === 'outbox-table' && targets.length !== 1)
      issues.push(`${node.data.title}: connect one CDC relay.`);
    if (topics && node.data.kind !== 'outbox-table' && !topics.has(String(node.data.targetTopic)))
      issues.push(`${node.data.title}: target topic does not exist.`);
  }
  return [...new Set(issues)];
}

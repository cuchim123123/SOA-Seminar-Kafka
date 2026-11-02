import type { SimEngine } from '../sim-engine';
import { SIMULATION_LIMITS } from '../../domain/limits';

export interface TopicConfiguration {
  partitionCount: number;
  replicationFactor: number;
  minInsyncReplicas: number;
}
function validate(engine: SimEngine, name: string, config: TopicConfiguration) {
  if (!/^[a-zA-Z0-9._-]{1,249}$/.test(name) || name === '.' || name === '..')
    throw new Error('Choose a valid Kafka topic name.');
  if (
    !Number.isInteger(config.partitionCount) ||
    config.partitionCount < 1 ||
    config.partitionCount > SIMULATION_LIMITS.partitionsPerTopic
  )
    throw new Error('Use 1–12 partitions.');
  if (
    !Number.isInteger(config.replicationFactor) ||
    config.replicationFactor < 1 ||
    config.replicationFactor > engine.kafkaCluster.brokers.size
  )
    throw new Error('Replication factor must fit the visible broker count.');
  if (
    !Number.isInteger(config.minInsyncReplicas) ||
    config.minInsyncReplicas < 1 ||
    config.minInsyncReplicas > 100
  )
    throw new Error('Use a minISR between 1 and 100.');
}

export function createTopic(engine: SimEngine, name: string, config: TopicConfiguration) {
  validate(engine, name, config);
  if (engine.kafkaCluster.topics.size >= SIMULATION_LIMITS.topics)
    throw new Error('This model supports at most 24 topics.');
  if (engine.kafkaCluster.topics.has(name))
    throw new Error('Topic already exists; its records are preserved.');
  engine.clock.pause();
  engine.kafkaCluster.createTopic(
    name,
    config.partitionCount,
    config.replicationFactor,
    config.minInsyncReplicas,
  );
  engine.notifyStateUpdated();
}

export function configureTopic(engine: SimEngine, name: string, config: TopicConfiguration) {
  validate(engine, name, config);
  const topic = engine.kafkaCluster.topics.get(name);
  if (!topic) throw new Error('Topic does not exist.');
  if (config.partitionCount < topic.partitions.size)
    throw new Error('Kafka partition counts cannot decrease. Restore a scenario to start over.');
  engine.clock.pause();
  const groups = [...engine.consumerGroups.values()].filter((group) => group.topicName === name);
  for (const group of groups) engine.invalidateConsumerWork(group.groupId);
  engine.kafkaCluster.setPartitionCount(name, config.partitionCount);
  engine.kafkaCluster.reassignReplicas(name, config.replicationFactor);
  topic.minInsyncReplicas = config.minInsyncReplicas;
  for (const group of groups) {
    const assignments = group.rebalance(config.partitionCount);
    for (const member of group.members.values()) {
      const node = engine.nodes.get(member.id)!;
      node.data.assignedPartitions = assignments.get(member.id) ?? [];
      if (node.data.status !== 'CRASHED')
        node.data.status = node.data.isPaused
          ? 'PAUSED'
          : member.assignedPartitions.length
            ? 'POLLING'
            : 'IDLE';
      for (const part of member.assignedPartitions) engine.resumePartitionBacklog(group, part);
    }
  }
  engine.notifyStateUpdated();
}

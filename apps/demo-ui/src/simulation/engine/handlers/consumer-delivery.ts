import type { SimConsumerData } from '../../domain/types';
import type { SimEngine } from '../sim-engine';
import type { SimEvent } from '../../domain/events';
import { createSimEvent } from '../../domain/events';
import { SimClusterOperations } from '../operations/sim-cluster-operations';

export function activeDelivery(engine: SimEngine, event: SimEvent) {
  const consumerId = String(event.metadata?.consumerId ?? event.sourceNodeId);
  const node = engine.nodes.get(consumerId);
  if (!node || node.data.kind !== 'consumer' || node.data.status === 'CRASHED') return null;
  const group = engine.consumerGroups.get(String(event.metadata?.groupId ?? node.data.groupId));
  const member = group?.members.get(consumerId);
  const partition = Number(event.metadata?.partitionId ?? 0);
  if (
    !group ||
    !member ||
    !member.assignedPartitions.includes(partition) ||
    group.topicName !== String(event.metadata?.topic ?? 'purchases')
  )
    return null;
  if (event.metadata?.deliveryId && member.deliveryId !== event.metadata.deliveryId) return null;
  return {
    consumerId,
    node,
    group,
    member,
    partition,
    topic: String(event.metadata?.topic ?? 'purchases'),
  };
}

export function scheduleCommit(engine: SimEngine, event: SimEvent): void {
  const delivery = activeDelivery(engine, event);
  if (!delivery) return;
  engine.queue.push(
    createSimEvent(
      event.scheduledTime + 1,
      'COMMIT_OFFSET',
      delivery.consumerId,
      `Handler succeeded; commit next offset ${Number(event.metadata?.offset ?? 0) + 1} for ${delivery.group.groupId}`,
      { message: event.message, metadata: event.metadata },
    ),
  );
}

export function finishDelivery(engine: SimEngine, event: SimEvent): void {
  const delivery = activeDelivery(engine, event);
  if (!delivery) return;
  const { node, member, group, partition, topic } = delivery;
  if (event.metadata?.commitMode !== 'auto')
    group.commitOffset(topic, partition, Number(event.metadata?.offset ?? 0));
  else group.unlockPartition(partition);
  member.currentProcessingPartition = undefined;
  member.currentProcessingOffset = undefined;
  member.deliveryId = undefined;
  node.data.currentProcessingPartition = undefined;
  node.data.currentProcessingOffset = undefined;
  node.data.status = node.data.isPaused ? 'PAUSED' : 'POLLING';
  engine.processedMessageCount += 1;
  if (node.data.isPaused) return;
  for (const assigned of member.assignedPartitions) {
    if ((node.data as SimConsumerData).pausedPartitions?.includes(assigned)) continue;
    const record = engine.kafkaCluster
      .getPartition(topic, assigned)
      ?.records.find((row) => row.offset === group.getCommittedNextOffset(topic, assigned));
    if (record)
      SimClusterOperations.scheduleConsumerPoll(
        engine,
        topic,
        assigned,
        record.offset,
        record.message,
        event.scheduledTime,
      );
  }
}

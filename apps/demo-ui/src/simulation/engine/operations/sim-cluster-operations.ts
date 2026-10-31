import type { SimEngine } from '../sim-engine';
import type { SimMessage } from '../../domain/types';
import type { SimConsumerGroupModel } from '../../domain/consumer-group';
import { createSimEvent } from '../../domain/events';

export class SimClusterOperations {
  /**
   * Schedule consumer polling for a partition record.
   * Enforces partition locking and checks if the consumer is paused or cut.
   */
  public static scheduleConsumerPoll(
    engine: SimEngine,
    topicName: string,
    partitionId: number,
    _offset: number,
    _message: SimMessage,
    currentTime: number,
  ): void {
    for (const group of engine.consumerGroups.values()) {
      if (group.isPartitionInFlight(partitionId)) {
        continue;
      }

      if (
        !group.hasCommittedOffset(topicName, partitionId) &&
        engine.mechanicsConfig.autoOffsetReset === 'latest'
      ) {
        group.seekNextOffset(topicName, partitionId, _offset);
      }

      const nextOffset = group.getCommittedNextOffset(topicName, partitionId);
      const nextRecord = engine.kafkaCluster
        .getPartition(topicName, partitionId)
        ?.records.find((r) => r.offset === nextOffset);
      if (!nextRecord) continue;
      const targetOffset = nextRecord.offset;
      const targetMessage = nextRecord.message;

      for (const [memberId, instance] of group.members.entries()) {
        if (instance.assignedPartitions.includes(partitionId)) {
          const consumerNode = engine.nodes.get(memberId);
          if (!consumerNode) continue;

          const cData = consumerNode.data as unknown as {
            isPaused?: boolean;
            pausedPartitions?: number[];
            status: string;
          };
          if (
            cData.isPaused ||
            cData.status === 'PAUSED' ||
            cData.status === 'CRASHED' ||
            cData.status === 'ERROR' ||
            instance.currentProcessingPartition !== undefined ||
            cData.pausedPartitions?.includes(partitionId)
          ) {
            continue;
          }

          group.lockPartition(partitionId);

          if (engine.mechanicsConfig.commitMode === 'auto') {
            group.seekNextOffset(topicName, partitionId, targetOffset + 1);
          }

          const dispatchEdge = Array.from(engine.edges.values()).find((e) => e.target === memberId);
          const transitMs = 350;
          const arriveTime = currentTime + transitMs;

          if (dispatchEdge) {
            engine.inFlightTokens.push({
              id: engine.nextTokenId(`token-poll-${targetMessage.id}`),
              messageId: targetMessage.id,
              messageKey: targetMessage.key,
              edgeId: dispatchEdge.id,
              sourceNodeId: dispatchEdge.source,
              targetNodeId: memberId,
              startTime: currentTime,
              arriveTime,
              color: '#fbbf24',
            });
          }

          const startEvent = createSimEvent(
            arriveTime,
            'START_PROCESSING',
            dispatchEdge ? dispatchEdge.source : 'kafka-cluster',
            `Consumer ${memberId} polled record offset #${targetOffset} from P${partitionId}`,
            {
              targetNodeId: memberId,
              edgeId: dispatchEdge?.id,
              message: targetMessage,
              metadata: { partitionId, offset: targetOffset, topic: topicName },
            },
          );
          engine.queue.push(startEvent);
          break;
        }
      }
    }
  }

  /**
   * Resumes consuming any unprocessed backlog records accumulated on this partition
   */
  public static resumePartitionBacklog(
    engine: SimEngine,
    group: SimConsumerGroupModel,
    partitionId: number,
    topicName: string = 'purchases',
  ): void {
    const partition = engine.kafkaCluster.getPartition(topicName, partitionId);
    if (!partition) return;

    const nextOffset = group.getCommittedNextOffset(topicName, partitionId);
    if (nextOffset >= partition.logEndOffset) return;

    const nextRecord = partition.records.find((r) => r.offset === nextOffset);
    if (!nextRecord) return;

    this.scheduleConsumerPoll(
      engine,
      topicName,
      partitionId,
      nextRecord.offset,
      nextRecord.message,
      engine.clock.virtualTimeMs,
    );
  }

  /**
   * Rewinds/advances a group's next-read offset for one partition
   */
  public static seekPartitionOffset(
    engine: SimEngine,
    groupId: string,
    partitionId: number,
    nextOffset: number,
  ): boolean {
    const group = engine.consumerGroups.get(groupId);
    const partition = engine.kafkaCluster.getPartition('purchases', partitionId);
    if (!group || !partition) return false;

    // Release partition lock so seek takes effect immediately
    group.unlockPartition(partitionId);

    const target = Math.min(Math.max(0, nextOffset), partition.logEndOffset);
    group.seekNextOffset('purchases', partitionId, target);
    engine.recordEvent(
      createSimEvent(
        engine.clock.virtualTimeMs,
        'DISPATCH_MESSAGE',
        'kafka-cluster',
        `🎯 [OFFSET SEEK] Group ${groupId} next-read offset for P${partitionId} set to #${target} (LEO #${partition.logEndOffset}).`,
      ),
    );
    this.resumePartitionBacklog(engine, group, partitionId, 'purchases');
    engine.notifyStateUpdated();
    return true;
  }

  /**
   * Bulk resets all committed partition offsets for a consumer group
   */
  public static resetGroupOffsets(
    engine: SimEngine,
    groupId: string,
    policy: 'earliest' | 'latest',
  ): void {
    const group = engine.consumerGroups.get(groupId);
    const topic = engine.kafkaCluster.topics.get('purchases');
    if (!group || !topic) return;

    // Reset unlocks all partitions and clears error status on consumers in this group
    group.unlockAllPartitions();
    for (const memberId of group.members.keys()) {
      const node = engine.nodes.get(memberId);
      if (node && node.data.kind === 'consumer' && node.data.status === 'ERROR') {
        node.data.status = 'POLLING';
        node.data.statusText = undefined;
      }
    }

    for (const [pId, part] of topic.partitions.entries()) {
      const target = policy === 'earliest' ? 0 : part.logEndOffset;
      group.seekNextOffset('purchases', pId, target);
      if (policy === 'earliest') {
        this.resumePartitionBacklog(engine, group, pId, 'purchases');
      }
    }

    engine.recordEvent(
      createSimEvent(
        engine.clock.virtualTimeMs,
        'DISPATCH_MESSAGE',
        'kafka-cluster',
        `🎯 [OFFSET RESET] Group ${groupId} all partition offsets reset to "${policy.toUpperCase()}".`,
      ),
    );
    engine.notifyStateUpdated();
  }

  /**
   * Simulates a broker node crash
   */
  public static crashBroker(engine: SimEngine, brokerId: number): void {
    const crashed = engine.kafkaCluster.crashBroker(brokerId);
    if (!crashed) return;

    const event = createSimEvent(
      engine.clock.virtualTimeMs,
      'BROKER_CRASH',
      'kafka-cluster',
      `💥 Broker B${brokerId} CRASHED! KRaft metadata triggered automatic partition leader failover.`,
      { metadata: { brokerId } },
    );
    engine.recordEvent(event);

    const clusterNode = Array.from(engine.nodes.values()).find(
      (n) => n.data.kind === 'kafka-cluster',
    );
    if (clusterNode) {
      const activeBrokers = Array.from(engine.kafkaCluster.brokers.values()).filter(
        (b) => b.status === 'ONLINE',
      ).length;
      clusterNode.data.status =
        activeBrokers === 0 ? 'CRASHED' : activeBrokers < 2 ? 'DEGRADED' : 'ACTIVE';
    }
    engine.notifyStateUpdated();
  }

  /**
   * Revives a previously crashed broker
   */
  public static reviveBroker(engine: SimEngine, brokerId: number): void {
    const revived = engine.kafkaCluster.reviveBroker(brokerId);
    if (!revived) return;

    const event = createSimEvent(
      engine.clock.virtualTimeMs,
      'BROKER_REVIVE',
      'kafka-cluster',
      `🩺 Broker B${brokerId} RECOVERED and rejoined the KRaft cluster quorum (ISR synced).`,
      { metadata: { brokerId } },
    );
    engine.recordEvent(event);

    const clusterNode = Array.from(engine.nodes.values()).find(
      (n) => n.data.kind === 'kafka-cluster',
    );
    if (clusterNode) {
      clusterNode.data.status = 'ACTIVE';
    }
    engine.notifyStateUpdated();
  }

  /**
   * Simulates a consumer instance crash and triggers immediate failover rebalance
   */
  public static crashConsumer(engine: SimEngine, consumerNodeId: string): void {
    const consumerNode = engine.nodes.get(consumerNodeId);
    if (!consumerNode || consumerNode.data.kind !== 'consumer') return;

    const activePartition = consumerNode.data.currentProcessingPartition;
    consumerNode.data.status = 'CRASHED';
    consumerNode.data.assignedPartitions = [];
    consumerNode.data.currentProcessingPartition = undefined;
    consumerNode.data.currentProcessingOffset = undefined;

    const groupId = (consumerNode.data.groupId as string) || 'loyalty-v1';
    const group = engine.consumerGroups.get(groupId);
    const topic = engine.kafkaCluster.topics.get('purchases');
    const pCount = topic ? topic.partitions.size : 4;

    if (group) {
      if (activePartition !== undefined) {
        group.unlockPartition(Number(activePartition));
      }
      group.crashMember(consumerNodeId, pCount);

      // Purge pending in-flight processing/start events for the crashed consumer
      engine.queue.removeWhere(
        (e) =>
          (e.type === 'START_PROCESSING' && e.targetNodeId === consumerNodeId) ||
          (e.type === 'COMPLETE_PROCESSING' && e.sourceNodeId === consumerNodeId),
      );

      // Sync remaining members
      for (const [memberId, instance] of group.members.entries()) {
        const node = engine.nodes.get(memberId);
        if (node && node.data.kind === 'consumer' && node.id !== consumerNodeId) {
          node.data.assignedPartitions = [...instance.assignedPartitions];
          if (node.data.status !== 'PAUSED') {
            node.data.status = instance.assignedPartitions.length > 0 ? 'POLLING' : 'IDLE';
          }
        }
      }

      // Resume backlog on surviving active members for reassigned partitions
      for (const [memberId, instance] of group.members.entries()) {
        if (
          memberId !== consumerNodeId &&
          instance.status !== 'CRASHED' &&
          instance.status !== 'PAUSED'
        ) {
          for (const p of instance.assignedPartitions) {
            this.resumePartitionBacklog(engine, group, p, topic?.name || 'purchases');
          }
        }
      }
    }

    engine.recordEvent(
      createSimEvent(
        engine.clock.virtualTimeMs,
        'CRASH_CONSUMER',
        consumerNodeId,
        `Consumer ${consumerNodeId} CRASHED! Group ${groupId} rebalanced partitions across surviving fleet.`,
      ),
    );

    engine.notifyStateUpdated();
  }

  /**
   * Revives a crashed consumer
   */
  public static reviveConsumer(engine: SimEngine, consumerNodeId: string): void {
    const consumerNode = engine.nodes.get(consumerNodeId);
    if (!consumerNode || consumerNode.data.kind !== 'consumer') return;

    consumerNode.data.status = 'POLLING';
    consumerNode.data.isPaused = false;

    const groupId = (consumerNode.data.groupId as string) || 'loyalty-v1';
    const group = engine.consumerGroups.get(groupId);
    const topic = engine.kafkaCluster.topics.get('purchases');
    const pCount = topic ? topic.partitions.size : 4;

    if (group) {
      group.reviveMember(consumerNodeId, pCount);

      // Sync all members
      for (const [memberId, instance] of group.members.entries()) {
        const node = engine.nodes.get(memberId);
        if (node && node.data.kind === 'consumer') {
          node.data.assignedPartitions = [...instance.assignedPartitions];
          if (node.data.status !== 'PAUSED') {
            node.data.status = instance.assignedPartitions.length > 0 ? 'POLLING' : 'IDLE';
          }
        }
      }

      // Resume backlogs across all online members with newly assigned partitions
      for (const instance of group.members.values()) {
        if (instance.status !== 'CRASHED' && instance.status !== 'PAUSED') {
          for (const p of instance.assignedPartitions) {
            this.resumePartitionBacklog(engine, group, p, topic?.name || 'purchases');
          }
        }
      }
    }

    engine.recordEvent(
      createSimEvent(
        engine.clock.virtualTimeMs,
        'REVIVE_CONSUMER',
        consumerNodeId,
        `Consumer ${consumerNodeId} REVIVED! Rejoined group ${groupId} and triggered rebalance.`,
      ),
    );

    engine.notifyStateUpdated();
  }

  /**
   * Toggles pause/resume on consumer polling
   */
  public static togglePauseConsumer(engine: SimEngine, consumerId: string): boolean {
    const consumerNode = engine.nodes.get(consumerId);
    if (!consumerNode || consumerNode.data.kind !== 'consumer') return false;

    const cData = consumerNode.data as unknown as {
      isPaused?: boolean;
      status: string;
      groupId?: string;
      assignedPartitions?: number[];
    };
    cData.isPaused = !cData.isPaused;
    cData.status = cData.isPaused
      ? 'PAUSED'
      : cData.assignedPartitions && cData.assignedPartitions.length > 0
        ? 'POLLING'
        : 'IDLE';

    const groupId = cData.groupId || 'loyalty-v1';
    const group = engine.consumerGroups.get(groupId);
    const instance = group?.members.get(consumerId);
    if (instance) {
      instance.status = cData.status as typeof instance.status;
    }

    engine.recordEvent(
      createSimEvent(
        engine.clock.virtualTimeMs,
        'DISPATCH_MESSAGE',
        consumerId,
        cData.isPaused
          ? `⏸️ [POLL PAUSED] Consumer ${consumerId} paused polling! New messages will accumulate in partition backlogs.`
          : `▶️ [POLL RESUMED] Consumer ${consumerId} resumed polling! Draining backlog across assigned partitions.`,
      ),
    );

    if (!cData.isPaused && group) {
      const assigned = cData.assignedPartitions || instance?.assignedPartitions || [];
      for (const p of assigned) {
        this.resumePartitionBacklog(engine, group, p, 'purchases');
      }
    }

    engine.notifyStateUpdated();
    return Boolean(cData.isPaused);
  }

  /**
   * Toggles pause on an individual partition for a consumer
   */
  public static togglePauseConsumerPartition(
    engine: SimEngine,
    consumerId: string,
    partitionId: number,
  ): boolean {
    const consumerNode = engine.nodes.get(consumerId);
    if (!consumerNode || consumerNode.data.kind !== 'consumer') return false;

    const cData = consumerNode.data as unknown as {
      pausedPartitions?: number[];
      groupId?: string;
    };
    const current = cData.pausedPartitions || [];
    const isCurrentlyCut = current.includes(partitionId);
    const next = isCurrentlyCut
      ? current.filter((p) => p !== partitionId)
      : [...current, partitionId];
    cData.pausedPartitions = next;

    const groupId = cData.groupId || 'loyalty-v1';
    const group = engine.consumerGroups.get(groupId);

    engine.recordEvent(
      createSimEvent(
        engine.clock.virtualTimeMs,
        'DISPATCH_MESSAGE',
        consumerId,
        isCurrentlyCut
          ? `🔌 [WIRE RECONNECTED] Consumer ${consumerId} reconnected polling wire for partition P${partitionId}! Draining backlog.`
          : `✂️ [WIRE CUT] Consumer ${consumerId} severed polling wire for partition P${partitionId}! Backlog will accumulate.`,
      ),
    );

    if (isCurrentlyCut && group) {
      this.resumePartitionBacklog(engine, group, partitionId, 'purchases');
    }

    engine.notifyStateUpdated();
    return !isCurrentlyCut;
  }
}

import { SimClock } from './sim-clock';
import { SimPriorityQueue } from './priority-queue';
import { SimKafkaClusterModel } from '../domain/kafka-cluster';
import { SimConsumerGroupModel } from '../domain/consumer-group';
import type {
  SimCustomNode,
  SimCustomEdge,
  SimEvent,
  SimMessage,
  SimInFlightToken,
  SimKafkaMechanicsConfig,
} from '../domain/types';
import { SimMessageDispatcher } from './dispatcher/sim-message-dispatcher';
import { SimClusterOperations } from './operations/sim-cluster-operations';
import { SimEventHandlers } from './handlers/sim-event-handlers';

const DEFAULT_MECHANICS_CONFIG: SimKafkaMechanicsConfig = {
  commitMode: 'manual',
  autoOffsetReset: 'latest',
  consumerIdempotency: true,
  keyOrderingMode: 'MURMUR2_KEYED',
  partitioningStrategy: 'murmur2-key',
  producerAcks: 'all',
};

export type SimEngineListener = (() => void) | { onStateUpdated?: () => void };

export class SimEngine {
  public clock: SimClock = new SimClock();
  public queue: SimPriorityQueue = new SimPriorityQueue();
  public kafkaCluster: SimKafkaClusterModel = new SimKafkaClusterModel('kraft-sim', 3);
  public consumerGroups: Map<string, SimConsumerGroupModel> = new Map();

  public nodes: Map<string, SimCustomNode> = new Map();
  public edges: Map<string, SimCustomEdge> = new Map();

  public eventLog: SimEvent[] = [];
  public inFlightTokens: SimInFlightToken[] = [];
  public processedMessageCount: number = 0;

  public mechanicsConfig: SimKafkaMechanicsConfig = { ...DEFAULT_MECHANICS_CONFIG };

  public processedEvents: Set<string> = new Set();
  public roundRobinIndex: number = 0;
  public lastDispatchedMessage: SimMessage | null = null;
  private tokenSequence: number = 0;
  private topologySignature = '';

  private listeners: SimEngineListener[] = [];

  constructor() {
    this.kafkaCluster.createTopic('purchases', 4, 3, 2);
  }

  public nextTokenId(prefix: string): string {
    return `${prefix}-${++this.tokenSequence}`;
  }

  public setMechanicsConfig(config: Partial<SimKafkaMechanicsConfig>): void {
    this.mechanicsConfig = { ...this.mechanicsConfig, ...config };
    this.notifyStateUpdated();
  }

  public addListener(listener: SimEngineListener): void {
    this.listeners.push(listener);
  }

  public removeListener(listener: SimEngineListener): void {
    this.listeners = this.listeners.filter((l) => l !== listener);
  }

  public setTopology(nodes: SimCustomNode[], edges: SimCustomEdge[]): void {
    const oldConsumers = new Set(
      Array.from(this.nodes.values())
        .filter((node) => node.data.kind === 'consumer')
        .map((node) => node.id),
    );
    this.nodes.clear();
    for (const node of nodes) {
      this.nodes.set(node.id, node);
    }

    this.edges.clear();
    for (const edge of edges) {
      this.edges.set(edge.id, edge);
    }

    // Geometry/selection/React Flow measurements never change execution semantics.
    const signature = JSON.stringify({
      nodes: nodes
        .map((node) => ({
          id: node.id,
          kind: node.data.kind,
          brokerId: node.data.brokerId,
          groupId: node.data.groupId,
          subscribedTopic: node.data.subscribedTopic,
          activeTopic: node.data.activeTopic,
          partitionCount: node.data.partitionCount,
          replicationFactor: node.data.replicationFactor,
          minInsyncReplicas: node.data.minInsyncReplicas,
        }))
        .sort((a, b) => a.id.localeCompare(b.id)),
      edges: edges
        .map((edge) => ({
          id: edge.id,
          source: edge.source,
          target: edge.target,
          sourceHandle: edge.sourceHandle,
          targetHandle: edge.targetHandle,
        }))
        .sort((a, b) => a.id.localeCompare(b.id)),
    });
    if (signature === this.topologySignature) return;
    const wasInitialized = !!this.topologySignature;
    this.topologySignature = signature;
    if (wasInitialized) {
      this.clock.pause();
      const consumerIds = new Set([
        ...oldConsumers,
        ...nodes.filter((node) => node.data.kind === 'consumer').map((node) => node.id),
      ]);
      this.queue.removeWhere(
        (event) =>
          !!event.metadata?.deliveryId ||
          !this.nodes.has(event.sourceNodeId) ||
          (!!event.targetNodeId && !this.nodes.has(event.targetNodeId)) ||
          (!!event.edgeId && !this.edges.has(event.edgeId)),
      );
      this.inFlightTokens = this.inFlightTokens.filter(
        (token) =>
          !consumerIds.has(token.sourceNodeId) &&
          !consumerIds.has(token.targetNodeId) &&
          this.edges.has(token.edgeId),
      );
      for (const group of this.consumerGroups.values()) {
        group.unlockAllPartitions();
        for (const member of group.members.values()) {
          member.deliveryId = undefined;
          member.currentProcessingPartition = undefined;
          member.currentProcessingOffset = undefined;
        }
      }
      for (const node of nodes)
        if (node.data.kind === 'consumer') {
          node.data.currentProcessingPartition = undefined;
          node.data.currentProcessingOffset = undefined;
          node.data.statusText = undefined;
          if (node.data.status !== 'CRASHED')
            node.data.status = node.data.isPaused ? 'PAUSED' : 'POLLING';
        }
    }

    // Sync Kafka Broker topology and Topic configuration
    const clusterNode = nodes.find((n) => n.data.kind === 'kafka-cluster');
    const brokerNodes = nodes.filter((n) => n.data.kind === 'kafka-broker');

    if (brokerNodes.length > 0) {
      const visibleIds = new Set(brokerNodes.map((node) => Number(node.data.brokerId)));
      for (const id of this.kafkaCluster.brokers.keys())
        if (!visibleIds.has(id)) {
          this.kafkaCluster.crashBroker(id);
          this.kafkaCluster.brokers.delete(id);
        }
      // Ensure all broker nodes are registered in kafkaCluster.brokers
      for (const bNode of brokerNodes) {
        const bId = (bNode.data.brokerId as number) || 1;
        if (!this.kafkaCluster.brokers.has(bId)) {
          this.kafkaCluster.brokers.set(bId, {
            id: bId,
            host: (bNode.data.host as string) || `broker-${bId}:9092`,
            isController: bId === 1,
            status: bNode.data.status === 'CRASHED' ? 'CRASHED' : 'ONLINE',
          });
        }
      }
      for (const topic of this.kafkaCluster.topics.values()) {
        if (
          !wasInitialized &&
          topic.replicationFactor > brokerNodes.length &&
          Array.from(topic.partitions.values()).every((part) => part.records.length === 0)
        ) {
          this.kafkaCluster.reassignReplicas(topic.name, brokerNodes.length);
          topic.minInsyncReplicas = Math.min(topic.minInsyncReplicas, brokerNodes.length);
        } else if (
          topic.replicationFactor <= brokerNodes.length &&
          Array.from(topic.partitions.values()).some((part) =>
            part.replicas.some((id) => !visibleIds.has(id)),
          )
        )
          this.kafkaCluster.reassignReplicas(topic.name, topic.replicationFactor);
      }
      // Topic sync with broker count
      const topicName = 'purchases';
      const existingTopic = this.kafkaCluster.topics.get(topicName);
      const safeRF = Math.min(brokerNodes.length, existingTopic?.replicationFactor || 3);
      if (!existingTopic) {
        this.kafkaCluster.createTopic(topicName, 4, safeRF, Math.min(safeRF, 2));
      }
    } else if (clusterNode) {
      if (!wasInitialized)
        this.kafkaCluster.initializeBrokers(Number(clusterNode.data.brokerCount ?? 3));
      const topicName = String(clusterNode.data.activeTopic ?? 'purchases');
      const existingTopic = this.kafkaCluster.topics.get(topicName);
      const pCount = Number(clusterNode.data.partitionCount ?? existingTopic?.partitions.size ?? 4);
      const rf = Number(
        clusterNode.data.replicationFactor ?? existingTopic?.replicationFactor ?? 3,
      );
      const minIsr = Number(
        clusterNode.data.minInsyncReplicas ?? existingTopic?.minInsyncReplicas ?? 2,
      );
      if (!existingTopic) {
        this.kafkaCluster.createTopic(topicName, pCount, rf, minIsr);
      } else {
        if (existingTopic.partitions.size !== pCount) {
          this.kafkaCluster.setPartitionCount(topicName, pCount);
        }
        if (existingTopic.replicationFactor !== rf && rf <= this.kafkaCluster.brokers.size)
          this.kafkaCluster.reassignReplicas(topicName, rf);
        existingTopic.minInsyncReplicas = minIsr;
      }
    }

    // Synchronize consumer groups from topology
    const foundGroupIds = new Set<string>();
    const nodeGroups = new Map<string, string[]>();

    for (const node of nodes) {
      if (node.data.kind === 'consumer') {
        const gId = (node.data.groupId as string) || 'loyalty-v1';
        foundGroupIds.add(gId);
        if (!nodeGroups.has(gId)) nodeGroups.set(gId, []);
        nodeGroups.get(gId)!.push(node.id);
      }
    }

    // Remove deleted groups
    for (const gId of this.consumerGroups.keys()) {
      if (!foundGroupIds.has(gId)) {
        this.consumerGroups.delete(gId);
      }
    }

    // Add or update groups and trigger rebalance
    for (const [gId, memberIds] of nodeGroups.entries()) {
      let group = this.consumerGroups.get(gId);
      if (!group) {
        group = new SimConsumerGroupModel(gId);
        this.consumerGroups.set(gId, group);
      }

      group.topicName = String(this.nodes.get(memberIds[0])?.data.subscribedTopic ?? 'purchases');
      const topic = this.kafkaCluster.topics.get(group.topicName);
      const partitionCount = topic?.partitions.size ?? 0;
      if (topic)
        for (const part of topic.partitions.values())
          if (!group.hasCommittedOffset(topic.name, part.partitionId))
            group.seekNextOffset(
              topic.name,
              part.partitionId,
              this.mechanicsConfig.autoOffsetReset === 'latest'
                ? part.logEndOffset
                : part.logStartOffset,
            );
      // Sync members
      const currentMembers = new Set(group.members.keys());
      for (const mId of memberIds) {
        if (!currentMembers.has(mId)) {
          group.registerConsumer(mId);
        }
      }
      for (const cur of currentMembers) {
        if (!memberIds.includes(cur)) {
          group.unregisterConsumer(cur);
        }
      }

      // Rebalance active members
      const assignments = group.rebalance(partitionCount);

      // Write assignments back to node data
      for (const mId of memberIds) {
        const cNode = this.nodes.get(mId);
        if (cNode && cNode.data.kind === 'consumer') {
          const assigned = assignments.get(mId) || [];
          cNode.data.assignedPartitions = assigned;
          if (cNode.data.status !== 'CRASHED' && cNode.data.status !== 'PAUSED') {
            cNode.data.status = assigned.length > 0 ? 'POLLING' : 'IDLE';
          }
        }
      }
    }

    if (wasInitialized)
      for (const topic of this.kafkaCluster.topics.values())
        for (const part of topic.partitions.values()) {
          const record = part.records[0];
          if (record)
            this.scheduleConsumerPoll(
              topic.name,
              part.partitionId,
              record.offset,
              record.message,
              this.clock.virtualTimeMs,
            );
        }
    this.notifyStateUpdated();
  }

  public invalidateConsumerWork(groupId: string): void {
    const group = this.consumerGroups.get(groupId);
    if (!group) return;
    const members = new Set(group.members.keys());
    this.queue.removeWhere(
      (event) => event.metadata?.groupId === groupId && !!event.metadata?.deliveryId,
    );
    this.inFlightTokens = this.inFlightTokens.filter(
      (token) => !members.has(token.sourceNodeId) && !members.has(token.targetNodeId),
    );
    group.unlockAllPartitions();
    for (const member of group.members.values()) {
      member.deliveryId = undefined;
      member.currentProcessingPartition = undefined;
      member.currentProcessingOffset = undefined;
      const node = this.nodes.get(member.id);
      if (!node) continue;
      node.data.currentProcessingPartition = undefined;
      node.data.currentProcessingOffset = undefined;
      if (node.data.status !== 'CRASHED')
        node.data.status = node.data.isPaused ? 'PAUSED' : 'POLLING';
    }
  }

  public dispatchProducerMessage(
    producerNodeId: string,
    customKey?: string,
    messageOptions?: {
      purchaseId?: string;
      itemDescription?: string;
      targetFailedConsumerIds?: string[];
      targetFailedGroupIds?: string[];
    },
  ): SimMessage | null {
    return SimMessageDispatcher.dispatchProducerMessage(
      this,
      producerNodeId,
      customKey,
      messageOptions,
    );
  }

  public dispatchDuplicateMessage(customKey?: string): SimMessage | null {
    return SimMessageDispatcher.dispatchDuplicateMessage(this, customKey);
  }

  public dispatchOrderedBurst(customerKey: string = 'U001', count: number = 3): void {
    SimMessageDispatcher.dispatchOrderedBurst(this, customerKey, count);
  }

  public tick(realDeltaMs: number): void {
    if (this.clock.status !== 'RUNNING') return;

    const currentVirtualTime = this.clock.advance(realDeltaMs);

    let executionBudget = 20;
    while (this.queue.size() > 0 && executionBudget > 0) {
      const nextEvent = this.queue.peek();
      if (!nextEvent || nextEvent.scheduledTime > currentVirtualTime) break;

      const event = this.queue.pop()!;
      this.executeEvent(event);
      executionBudget -= 1;
    }

    // Clean up expired in-flight tokens
    this.inFlightTokens = this.inFlightTokens.filter(
      (token) => token.arriveTime > currentVirtualTime,
    );

    this.notifyStateUpdated();
  }

  public step(): SimEvent | null {
    if (this.queue.size() === 0) return null;

    const nextEvent = this.queue.pop()!;
    this.clock.jumpTo(nextEvent.scheduledTime);
    this.executeEvent(nextEvent);

    this.inFlightTokens = this.inFlightTokens.filter(
      (token) => token.arriveTime > this.clock.virtualTimeMs,
    );

    this.notifyStateUpdated();
    return nextEvent;
  }

  public reset(preserveConfiguration = true): void {
    this.clock.reset();
    this.queue.clear();
    this.eventLog = [];
    this.inFlightTokens = [];
    this.processedMessageCount = 0;
    this.processedEvents.clear();
    this.roundRobinIndex = 0;
    this.lastDispatchedMessage = null;
    this.tokenSequence = 0;

    const defaults = new SimKafkaClusterModel('kraft-sim', 3);
    const configuration = preserveConfiguration ? this.kafkaCluster : defaults;
    const topics = Array.from(configuration.topics.values()).map((topic) => ({
      name: topic.name,
      partitionCount: topic.partitions.size,
      rf: topic.replicationFactor,
      minIsr: topic.minInsyncReplicas,
    }));
    const brokers = Array.from(configuration.brokers.values());
    const strategies = new Map(
      Array.from(this.consumerGroups.values()).map((group) => [
        group.groupId,
        group.assignorStrategy,
      ]),
    );
    if (!preserveConfiguration) {
      strategies.clear();
      this.mechanicsConfig = { ...DEFAULT_MECHANICS_CONFIG };
    }
    this.kafkaCluster = new SimKafkaClusterModel('kraft-sim', 3);
    this.kafkaCluster.brokers = new Map(
      brokers.map((broker, index) => [
        broker.id,
        { ...broker, status: 'ONLINE' as const, isController: index === 0 },
      ]),
    );
    this.kafkaCluster.topics.clear();
    for (const topic of topics)
      this.kafkaCluster.createTopic(topic.name, topic.partitionCount, topic.rf, topic.minIsr);
    this.consumerGroups.clear();
    this.topologySignature = '';

    for (const node of this.nodes.values()) {
      if (node.data.kind === 'consumer') {
        node.data.status = 'POLLING';
        node.data.currentProcessingPartition = undefined;
        node.data.currentProcessingOffset = undefined;
        node.data.statusText = undefined;
        node.data.isPaused = false;
        node.data.pausedPartitions = [];
      } else if (node.data.kind === 'kafka-cluster') {
        const topic = this.kafkaCluster.topics.get(String(node.data.activeTopic ?? 'purchases'));
        if (topic) {
          node.data.partitionCount = topic.partitions.size;
          node.data.replicationFactor = topic.replicationFactor;
          node.data.minInsyncReplicas = topic.minInsyncReplicas;
        }
      } else if (node.data.kind === 'producer') {
        node.data.status = 'IDLE';
      } else if (node.data.kind === 'database') {
        node.data.committedRecordCount = 0;
        node.data.status = 'IDLE';
      } else if (node.data.kind === 'dead-letter-queue') {
        node.data.deadLetterCount = 0;
        node.data.status = 'IDLE';
      } else if (node.data.kind === 'outbox-table') {
        node.data.pendingRecordsCount = 0;
        node.data.publishedRecordsCount = 0;
        node.data.status = 'IDLE';
      } else if (node.data.kind === 'cdc-relay') {
        node.data.streamedCount = 0;
        node.data.status = 'POLLING';
      }
    }

    this.setTopology(Array.from(this.nodes.values()), Array.from(this.edges.values()));
    for (const [id, strategy] of strategies) {
      const group = this.consumerGroups.get(id);
      group?.setAssignorStrategy(strategy);
      group?.rebalance(this.kafkaCluster.topics.get(group.topicName)?.partitions.size ?? 0);
      group?.members.forEach((member) => {
        const node = this.nodes.get(member.id);
        if (node) node.data.assignedPartitions = [...member.assignedPartitions];
      });
    }
    this.clock.pause();
    this.notifyStateUpdated();
  }

  private executeEvent(event: SimEvent): void {
    SimEventHandlers.executeEvent(this, event);
  }

  public scheduleConsumerPoll(
    topicName: string,
    partitionId: number,
    offset: number,
    message: SimMessage,
    currentTime: number,
  ): void {
    SimClusterOperations.scheduleConsumerPoll(
      this,
      topicName,
      partitionId,
      offset,
      message,
      currentTime,
    );
  }

  public seekPartitionOffset(groupId: string, partitionId: number, nextOffset: number): boolean {
    return SimClusterOperations.seekPartitionOffset(this, groupId, partitionId, nextOffset);
  }

  public resetGroupOffsets(groupId: string, policy: 'earliest' | 'latest'): void {
    SimClusterOperations.resetGroupOffsets(this, groupId, policy);
  }

  public resumePartitionBacklog(group: SimConsumerGroupModel, partitionId: number): void {
    SimClusterOperations.resumePartitionBacklog(this, group, partitionId);
  }

  public recordEvent(event: SimEvent): void {
    this.eventLog.unshift(event);
    if (this.eventLog.length > 100) {
      this.eventLog.pop();
    }
  }

  public crashBroker(brokerId: number): void {
    SimClusterOperations.crashBroker(this, brokerId);
  }

  public reviveBroker(brokerId: number): void {
    SimClusterOperations.reviveBroker(this, brokerId);
  }

  public crashConsumer(consumerNodeId: string): void {
    SimClusterOperations.crashConsumer(this, consumerNodeId);
  }

  public reviveConsumer(consumerNodeId: string): void {
    SimClusterOperations.reviveConsumer(this, consumerNodeId);
  }

  public togglePauseConsumer(consumerId: string): boolean {
    return SimClusterOperations.togglePauseConsumer(this, consumerId);
  }

  public togglePauseConsumerPartition(consumerId: string, partitionId: number): boolean {
    return SimClusterOperations.togglePauseConsumerPartition(this, consumerId, partitionId);
  }

  public notifyStateUpdated(): void {
    // Calculate live per-partition lag and attach to Kafka Cluster nodes
    const primaryGroup =
      this.consumerGroups.get('loyalty-v1') || Array.from(this.consumerGroups.values())[0];
    const topic = this.kafkaCluster.topics.get('purchases');
    if (topic) {
      const partitionLags: Record<number, number> = {};
      const partitionLogEndOffsets: Record<number, number> = {};
      for (const [pId, part] of topic.partitions.entries()) {
        partitionLags[pId] = primaryGroup
          ? primaryGroup.calculateLag('purchases', pId, part.logEndOffset)
          : 0;
        partitionLogEndOffsets[pId] = part.logEndOffset;
      }

      const brokerStatuses: Record<number, 'ONLINE' | 'CRASHED'> = {};
      const brokerControllers: Record<number, boolean> = {};
      for (const [bId, broker] of this.kafkaCluster.brokers.entries()) {
        brokerStatuses[bId] = broker.status;
        brokerControllers[bId] = broker.isController;
      }

      const partitionLeaders: Record<number, number> = {};
      const partitionIsr: Record<number, number[]> = {};
      for (const [pId, part] of topic.partitions.entries()) {
        partitionLeaders[pId] = part.leaderBrokerId;
        partitionIsr[pId] = [...part.isr];
      }

      for (const node of this.nodes.values()) {
        if (node.data.kind === 'kafka-cluster') {
          node.data.partitionLags = { ...partitionLags };
          node.data.partitionLogEndOffsets = { ...partitionLogEndOffsets };
          node.data.partitionCount = topic.partitions.size;
          node.data.brokerStatuses = brokerStatuses;
          node.data.brokerControllers = brokerControllers;
          node.data.partitionLeaders = partitionLeaders;
          node.data.partitionIsr = partitionIsr;
        } else if (node.data.kind === 'kafka-broker') {
          const bId = (node.data.brokerId as number) || 1;
          const broker = this.kafkaCluster.brokers.get(bId);
          if (broker) {
            node.data.status = broker.status === 'CRASHED' ? 'CRASHED' : 'ACTIVE';
            node.data.isController = broker.isController;
          }

          // Compute hosted partition replicas on this broker
          const hosted: Array<{
            topic: string;
            partitionId: number;
            isLeader: boolean;
            isInIsr: boolean;
            replicas: number[];
            isr: number[];
          }> = [];

          for (const [tName, t] of this.kafkaCluster.topics.entries()) {
            for (const [pId, part] of t.partitions.entries()) {
              if (part.replicas.includes(bId)) {
                hosted.push({
                  topic: tName,
                  partitionId: pId,
                  isLeader: part.leaderBrokerId === bId,
                  isInIsr: part.isr.includes(bId),
                  replicas: [...part.replicas],
                  isr: [...part.isr],
                });
              }
            }
          }
          node.data.topicPartitions = hosted;
        }
      }
    }

    for (const listener of this.listeners) {
      if (typeof listener === 'function') {
        listener();
      } else if (
        listener &&
        typeof (listener as { onStateUpdated?: () => void }).onStateUpdated === 'function'
      ) {
        (listener as { onStateUpdated: () => void }).onStateUpdated();
      }
    }
  }
}

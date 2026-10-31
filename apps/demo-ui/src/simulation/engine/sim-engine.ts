import { SimClock } from './sim-clock';
import { SimPriorityQueue } from './priority-queue';
import { SimKafkaClusterModel } from '../domain/kafka-cluster';
import { SimConsumerGroupModel } from '../domain/consumer-group';
import { createSimMessage, type SimMessage } from '../domain/message';
import { createSimEvent, type SimEvent } from '../domain/events';
import type {
  SimCustomNode,
  SimCustomEdge,
  SimInFlightToken,
  SimKafkaMechanicsConfig,
} from '../domain/types';

export interface SimEngineListener {
  onEventProcessed?: (event: SimEvent) => void;
  onStateUpdated?: () => void;
}

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

  // Master Kafka Mechanics Config
  public mechanicsConfig: SimKafkaMechanicsConfig = {
    commitMode: 'manual',
    autoOffsetReset: 'earliest',
    consumerIdempotency: true,
    keyOrderingMode: 'MURMUR2_KEYED',
    dlqRoutingEnabled: true,
    producerAcks: 'all',
  };

  // Idempotency tracking (processed event dedup per consumer group)
  public processedEvents: Set<string> = new Set();
  public roundRobinIndex: number = 0;
  public lastDispatchedMessage: SimMessage | null = null;

  private listeners: SimEngineListener[] = [];

  constructor() {
    this.consumerGroups.set('loyalty-v1', new SimConsumerGroupModel('loyalty-v1'));
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
    this.nodes.clear();

    // Reset member lists across all consumer groups before registering current topology nodes
    for (const group of this.consumerGroups.values()) {
      group.members.clear();
    }

    // Configure Kafka Cluster topic & brokers from cluster node if present
    const clusterNode = nodes.find((n) => n.data.kind === 'kafka-cluster');
    if (clusterNode) {
      const pCount = (clusterNode.data.partitionCount as number) || 4;
      const rf = (clusterNode.data.replicationFactor as number) || 3;
      const activeTopic = (clusterNode.data.activeTopic as string) || 'purchases';
      const bCount = (clusterNode.data.brokerCount as number) || 3;
      this.kafkaCluster.initializeBrokers(bCount);
      this.kafkaCluster.createTopic(activeTopic, pCount, rf, Math.max(1, Math.min(2, rf)));
    }

    for (const node of nodes) {
      this.nodes.set(node.id, node);
      if (node.data.kind === 'consumer') {
        const groupId = (node.data.groupId as string) || 'loyalty-v1';
        let group = this.consumerGroups.get(groupId);
        if (!group) {
          group = new SimConsumerGroupModel(groupId);
          this.consumerGroups.set(groupId, group);
        }
        group.registerConsumer(node.id, (node.data.processingDelayMs as number) || 120);
      }
    }

    this.edges.clear();
    for (const edge of edges) {
      this.edges.set(edge.id, edge);
    }

    // Trigger rebalance across all consumer groups with topic partition count
    const defaultTopic = this.kafkaCluster.topics.get('purchases');
    const pCount = defaultTopic ? defaultTopic.partitions.size : 4;
    for (const group of this.consumerGroups.values()) {
      group.rebalance(pCount);
      // Sync assignments back to nodes
      for (const [memberId, instance] of group.members.entries()) {
        const node = this.nodes.get(memberId);
        if (node && node.data.kind === 'consumer') {
          node.data.assignedPartitions = [...instance.assignedPartitions];
          node.data.status = instance.assignedPartitions.length > 0 ? 'POLLING' : 'IDLE';
        }
      }
    }

    this.notifyStateUpdated();
  }

  /**
   * Dispatch a single synthetic purchase message from a producer node
   */
  public dispatchProducerMessage(producerNodeId: string, customKey?: string): SimMessage | null {
    const producerNode = this.nodes.get(producerNodeId);
    if (!producerNode) return null;

    // Find outgoing edge from producer to Kafka Cluster
    const publishEdge = Array.from(this.edges.values()).find((e) => e.source === producerNodeId);
    if (!publishEdge) {
      const errorEvent = createSimEvent(
        this.clock.virtualTimeMs,
        'DISPATCH_MESSAGE',
        producerNodeId,
        `Cannot dispatch: Producer is not wired to a Kafka Cluster`,
      );
      this.recordEvent(errorEvent);
      return null;
    }

    const targetNodeId = publishEdge.target;
    const targetNode = this.nodes.get(targetNodeId);
    const key = customKey || (producerNode.data.messageKey as string) || 'U001';
    const message = createSimMessage(key, 4999, this.clock.virtualTimeMs);

    // Case 1: Wired to Transactional Outbox Table
    if (targetNode && targetNode.data.kind === 'outbox-table') {
      const dispatchEvent = createSimEvent(
        this.clock.virtualTimeMs,
        'DISPATCH_MESSAGE',
        producerNodeId,
        `Producer committed local transaction to Outbox Table for key "${key}" (${message.payload.purchaseId})`,
        {
          targetNodeId,
          edgeId: publishEdge.id,
          message,
        },
      );
      this.recordEvent(dispatchEvent);

      const transitMs = 250;
      const arriveTime = this.clock.virtualTimeMs + transitMs;

      this.inFlightTokens.push({
        id: `token-outbox-${message.id}-${publishEdge.id}`,
        messageId: message.id,
        messageKey: key,
        edgeId: publishEdge.id,
        sourceNodeId: producerNodeId,
        targetNodeId,
        startTime: this.clock.virtualTimeMs,
        arriveTime,
        color: '#ea580c',
      });

      const writeOutboxEvent = createSimEvent(
        arriveTime,
        'WRITE_OUTBOX',
        targetNodeId,
        `Outbox Table buffered pending record ${message.payload.purchaseId}`,
        {
          edgeId: publishEdge.id,
          message,
        },
      );
      this.queue.push(writeOutboxEvent);
      this.notifyStateUpdated();
      return message;
    }

    // Case 2: Directly wired to Kafka Cluster
    const topic = (producerNode.data.targetTopic as string) || 'purchases';
    const isRoundRobin = this.mechanicsConfig.keyOrderingMode === 'ROUND_ROBIN';
    let targetPartition: number;
    if (isRoundRobin) {
      const pCount = this.kafkaCluster.topics.get(topic)?.partitions.size || 4;
      targetPartition = this.roundRobinIndex++ % pCount;
    } else {
      targetPartition = this.kafkaCluster.routeKeyToPartition(topic, key);
    }

    this.lastDispatchedMessage = message;

    // 1. Dispatch Event
    const routingNote = isRoundRobin
      ? `⚠️ ROUND-ROBIN ROUTED to P${targetPartition} (Key hashing disabled! Sequential customer messages will scatter!)`
      : `Murmur2("${key}") ➔ Target Partition P${targetPartition} strictly preserving FIFO`;

    const dispatchEvent = createSimEvent(
      this.clock.virtualTimeMs,
      'DISPATCH_MESSAGE',
      producerNodeId,
      `Producer generated order ${message.payload.purchaseId} for key "${key}" (${routingNote})`,
      {
        targetNodeId,
        edgeId: publishEdge.id,
        message,
      },
    );
    this.recordEvent(dispatchEvent);

    // Producer Acks handling
    const effectiveAcks =
      this.mechanicsConfig.producerAcks || (producerNode.data.acks as string) || 'all';
    if (effectiveAcks === '0') {
      const ackEvent = createSimEvent(
        this.clock.virtualTimeMs,
        'DISPATCH_MESSAGE',
        producerNodeId,
        `⚡ [acks=0] Fire-and-forget: Producer acknowledged immediately without waiting for broker commit log!`,
      );
      this.recordEvent(ackEvent);
    }

    // 2. Schedule in-flight transit across edge (transit duration: 400ms virtual time)
    const transitMs = 400;
    const arriveTime = this.clock.virtualTimeMs + transitMs;

    this.inFlightTokens.push({
      id: `token-${message.id}-${publishEdge.id}`,
      messageId: message.id,
      messageKey: key,
      edgeId: publishEdge.id,
      sourceNodeId: producerNodeId,
      targetNodeId,
      startTime: this.clock.virtualTimeMs,
      arriveTime,
      color: isRoundRobin ? '#ec4899' : '#38bdf8',
    });

    // 3. Schedule broker log append when message arrives
    const appendEvent = createSimEvent(
      arriveTime,
      'APPEND_PARTITION',
      targetNodeId,
      `Record ${message.payload.purchaseId} stored in partition P${targetPartition}`,
      {
        edgeId: publishEdge.id,
        message,
        metadata: { partitionId: targetPartition, topic },
      },
    );
    this.queue.push(appendEvent);

    this.notifyStateUpdated();
    return message;
  }

  /**
   * Dispatch duplicate message to test consumer idempotency
   */
  public dispatchDuplicateMessage(customKey?: string): SimMessage | null {
    const producerNode = Array.from(this.nodes.values()).find((n) => n.data.kind === 'producer');
    if (!producerNode) return null;

    let messageToDup: SimMessage;
    if (this.lastDispatchedMessage) {
      // Re-use exact same message payload & ID to simulate transport retry / at-least-once duplicate
      messageToDup = {
        ...this.lastDispatchedMessage,
        timestampMs: this.clock.virtualTimeMs,
      };
    } else {
      const key = customKey || (producerNode.data.messageKey as string) || 'U001';
      messageToDup = createSimMessage(key, 5000, this.clock.virtualTimeMs);
      this.lastDispatchedMessage = messageToDup;
    }

    const publishEdge = Array.from(this.edges.values()).find((e) => e.source === producerNode.id);
    if (!publishEdge) return null;

    const targetNodeId = publishEdge.target;
    const topic = (producerNode.data.targetTopic as string) || 'purchases';
    const targetPartition = this.kafkaCluster.routeKeyToPartition(topic, messageToDup.key);

    const dispatchEvent = createSimEvent(
      this.clock.virtualTimeMs,
      'DISPATCH_MESSAGE',
      producerNode.id,
      `🚨 [DUPLICATE INJECTION] Producer re-sent DUPLICATE event ID ${messageToDup.id} (${messageToDup.payload.purchaseId}) for key "${messageToDup.key}" into P${targetPartition}`,
      {
        targetNodeId,
        edgeId: publishEdge.id,
        message: messageToDup,
      },
    );
    this.recordEvent(dispatchEvent);

    const transitMs = 400;
    const arriveTime = this.clock.virtualTimeMs + transitMs;

    this.inFlightTokens.push({
      id: `token-dup-${messageToDup.id}-${Date.now()}`,
      messageId: messageToDup.id,
      messageKey: messageToDup.key,
      edgeId: publishEdge.id,
      sourceNodeId: producerNode.id,
      targetNodeId,
      startTime: this.clock.virtualTimeMs,
      arriveTime,
      color: '#f59e0b',
    });

    const appendEvent = createSimEvent(
      arriveTime,
      'APPEND_PARTITION',
      targetNodeId,
      `Duplicate Record ${messageToDup.payload.purchaseId} stored in partition P${targetPartition}`,
      {
        edgeId: publishEdge.id,
        message: messageToDup,
        metadata: { partitionId: targetPartition, topic },
      },
    );
    this.queue.push(appendEvent);
    this.notifyStateUpdated();
    return messageToDup;
  }

  /**
   * Dispatch a burst of 3 sequential orders for a single customer to test ordering
   */
  public dispatchOrderedBurst(customerKey: string = 'U001', count: number = 3): void {
    const producerNode = Array.from(this.nodes.values()).find((n) => n.data.kind === 'producer');
    if (!producerNode) return;

    for (let i = 1; i <= count; i++) {
      const msg = createSimMessage(customerKey, 1000 * i, this.clock.virtualTimeMs + (i - 1) * 120);
      msg.payload.purchaseId = `ORDER-${customerKey}-SEQ0${i}`;

      const publishEdge = Array.from(this.edges.values()).find((e) => e.source === producerNode.id);
      if (!publishEdge) continue;

      const targetNodeId = publishEdge.target;
      const topic = (producerNode.data.targetTopic as string) || 'purchases';
      const isRoundRobin = this.mechanicsConfig.keyOrderingMode === 'ROUND_ROBIN';
      let targetPartition: number;
      if (isRoundRobin) {
        const pCount = this.kafkaCluster.topics.get(topic)?.partitions.size || 4;
        targetPartition = this.roundRobinIndex++ % pCount;
      } else {
        targetPartition = this.kafkaCluster.routeKeyToPartition(topic, customerKey);
      }

      const scheduleTime = this.clock.virtualTimeMs + (i - 1) * 120;
      const routingDesc = isRoundRobin
        ? `⚠️ ROUND-ROBIN SCATTERED to P${targetPartition}`
        : `➔ Murmur2 FIFO to P${targetPartition}`;

      const dispatchEvent = createSimEvent(
        scheduleTime,
        'DISPATCH_MESSAGE',
        producerNode.id,
        `[BURST ${i}/${count}] Order ${msg.payload.purchaseId} (${routingDesc})`,
        {
          targetNodeId,
          edgeId: publishEdge.id,
          message: msg,
        },
      );
      this.recordEvent(dispatchEvent);

      const transitMs = 400;
      const arriveTime = scheduleTime + transitMs;
      this.inFlightTokens.push({
        id: `token-burst-${msg.id}-${i}`,
        messageId: msg.id,
        messageKey: customerKey,
        edgeId: publishEdge.id,
        sourceNodeId: producerNode.id,
        targetNodeId,
        startTime: scheduleTime,
        arriveTime,
        color: isRoundRobin ? '#ec4899' : '#0284c7',
      });

      const appendEvent = createSimEvent(
        arriveTime,
        'APPEND_PARTITION',
        targetNodeId,
        `Burst Record ${msg.payload.purchaseId} appended to P${targetPartition}`,
        {
          edgeId: publishEdge.id,
          message: msg,
          metadata: { partitionId: targetPartition, topic },
        },
      );
      this.queue.push(appendEvent);
    }
    this.notifyStateUpdated();
  }

  /**
   * Main simulation tick loop: advances virtual time and executes all due events
   */
  public tick(realDeltaMs: number): void {
    if (this.clock.status !== 'RUNNING') return;

    this.clock.advance(realDeltaMs);
    const currentVirtualTime = this.clock.virtualTimeMs;

    // Execute all events whose scheduledTime <= currentVirtualTime
    let executionBudget = 500; // Protection against zero-delay runaway loops
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

  /**
   * Step forward: jumps virtual time exactly to the next event in the queue and executes it
   */
  public step(): SimEvent | null {
    if (this.queue.size() === 0) return null;

    const nextEvent = this.queue.pop()!;
    this.clock.jumpTo(nextEvent.scheduledTime);
    this.executeEvent(nextEvent);

    // Clean up expired tokens
    this.inFlightTokens = this.inFlightTokens.filter(
      (token) => token.arriveTime > this.clock.virtualTimeMs,
    );

    this.notifyStateUpdated();
    return nextEvent;
  }

  public reset(): void {
    this.clock.reset();
    this.queue.clear();
    this.eventLog = [];
    this.inFlightTokens = [];
    this.processedMessageCount = 0;
    this.processedEvents.clear();
    this.roundRobinIndex = 0;
    this.lastDispatchedMessage = null;

    // Reset Kafka partitions and consumer offsets
    this.kafkaCluster = new SimKafkaClusterModel('kraft-sim', 3);
    for (const group of this.consumerGroups.values()) {
      group.committedNextOffsets.clear();
      group.rebalance(4);
    }

    // Reset database node metrics
    for (const node of this.nodes.values()) {
      if (node.data.kind === 'database') {
        node.data.committedRecordCount = 0;
        node.data.status = 'IDLE';
      } else if (node.data.kind === 'consumer') {
        const cData = node.data as unknown as { assignedPartitions?: number[]; status: string };
        cData.status =
          cData.assignedPartitions && cData.assignedPartitions.length > 0 ? 'POLLING' : 'IDLE';
      }
    }

    this.notifyStateUpdated();
  }

  private executeEvent(event: SimEvent): void {
    this.recordEvent(event);

    switch (event.type) {
      case 'APPEND_PARTITION': {
        const { message, metadata } = event;
        if (!message || !metadata) break;

        const partitionId = Number(metadata.partitionId ?? 0);
        const topicName = (metadata.topic as string) || 'purchases';

        // Append to broker partition log
        const { offset } = this.kafkaCluster.appendRecord(
          topicName,
          partitionId,
          message,
          event.scheduledTime,
        );

        // Record ACK confirmation into timeline
        const effectiveAcks = this.mechanicsConfig.producerAcks;
        if (effectiveAcks === '1') {
          this.recordEvent(
            createSimEvent(
              event.scheduledTime,
              'DISPATCH_MESSAGE',
              event.sourceNodeId || 'kafka-cluster',
              `⚡ [acks=1] Leader ACK: Partition Leader confirmed write of record ${message.payload.purchaseId} at offset ${offset}.`,
            ),
          );
        } else if (effectiveAcks === 'all') {
          this.recordEvent(
            createSimEvent(
              event.scheduledTime,
              'DISPATCH_MESSAGE',
              event.sourceNodeId || 'kafka-cluster',
              `⚡ [acks=all] ISR Quorum ACK: Leader and all In-Sync Replicas committed offset ${offset} to persistent log.`,
            ),
          );
        }

        // Find consumer assigned to this partition
        this.scheduleConsumerPoll(topicName, partitionId, offset, message, event.scheduledTime);
        break;
      }

      case 'START_PROCESSING': {
        const { message, targetNodeId, metadata } = event;
        if (!targetNodeId || !message) break;

        const consumerNode = this.nodes.get(targetNodeId);
        if (consumerNode && consumerNode.data.kind === 'consumer') {
          consumerNode.data.status = 'PROCESSING';
          consumerNode.data.currentProcessingPartition = Number(metadata?.partitionId ?? 0);
          consumerNode.data.currentProcessingOffset = Number(metadata?.offset ?? 0);
          const delay = (consumerNode.data.processingDelayMs as number) || 120;

          // Schedule completion
          const completeEvent = createSimEvent(
            event.scheduledTime + delay,
            'COMPLETE_PROCESSING',
            targetNodeId,
            `Consumer ${targetNodeId} completed processing order ${message.payload.purchaseId} (P${metadata?.partitionId}:#${metadata?.offset})`,
            { message, metadata: event.metadata },
          );
          this.queue.push(completeEvent);
        }
        break;
      }

      case 'COMPLETE_PROCESSING': {
        const { message, sourceNodeId, metadata } = event;
        if (!message || !metadata) break;

        const consumerNode = this.nodes.get(sourceNodeId);
        if (consumerNode && consumerNode.data.kind === 'consumer') {
          consumerNode.data.status = 'POLLING';
          consumerNode.data.currentProcessingPartition = undefined;
          consumerNode.data.currentProcessingOffset = undefined;
          this.processedMessageCount += 1;

          const groupId = (consumerNode.data.groupId as string) || 'loyalty-v1';
          const group = this.consumerGroups.get(groupId);
          const partitionId = Number(metadata.partitionId ?? 0);
          const offset = Number(metadata.offset ?? 0);

          const isPoisonPill =
            message.key === 'POISON_PILL' || message.payload.purchaseId.startsWith('POISON');

          if (isPoisonPill) {
            // Check if Dead-Letter Queue routing is enabled and wired
            const dlqEdge = Array.from(this.edges.values()).find((e) => {
              if (e.source !== sourceNodeId) return false;
              const targetNode = this.nodes.get(e.target);
              return targetNode?.data.kind === 'dead-letter-queue';
            });

            if (this.mechanicsConfig.dlqRoutingEnabled && dlqEdge) {
              // DLQ Enabled: Commit offset to avoid head-of-line blocking!
              if (group) {
                group.commitOffset('purchases', partitionId, offset);
              }

              const dlqNodeId = dlqEdge.target;
              const dlqTransitMs = 350;
              const arriveTime = event.scheduledTime + dlqTransitMs;

              this.inFlightTokens.push({
                id: `token-dlq-${message.id}-${dlqEdge.id}`,
                messageId: message.id,
                messageKey: message.key,
                edgeId: dlqEdge.id,
                sourceNodeId,
                targetNodeId: dlqNodeId,
                startTime: event.scheduledTime,
                arriveTime,
                color: '#ef4444',
              });

              const dlqEvent = createSimEvent(
                arriveTime,
                'ROUTE_DLQ',
                dlqNodeId,
                `🛡️ [DLQ ROUTING] Consumer ${sourceNodeId} isolated poison-pill ${message.payload.purchaseId} to Dead-Letter Queue! Partition P${partitionId} unblocked.`,
                {
                  message,
                  edgeId: dlqEdge.id,
                },
              );
              this.queue.push(dlqEvent);
            } else {
              // DLQ Disabled or Unwired -> Head-of-Line Blocking!
              consumerNode.data.status = 'ERROR';
              consumerNode.data.statusText = '💥 Stalled by Poison Pill (DLQ Disabled)';

              const errorEvent = createSimEvent(
                event.scheduledTime,
                'CONSUMER_ERROR',
                sourceNodeId,
                `💥 [HEAD-OF-LINE BLOCKING] Consumer crashed on poison-pill ${message.payload.purchaseId}! DLQ disabled. Offset NOT committed. Partition P${partitionId} consumption stalled!`,
                { message, metadata: event.metadata },
              );
              this.recordEvent(errorEvent);
              break; // Abort: do not advance offset or process next record!
            }
          } else {
            // Normal Record: Check Idempotency Deduplication
            const dedupKey = `${groupId}:${message.id}`;
            const isDuplicate = this.processedEvents.has(dedupKey);

            if (isDuplicate && this.mechanicsConfig.consumerIdempotency) {
              // Idempotency ON: Duplicate detected! Commit offset to advance, but skip DB mutation!
              if (group) {
                group.commitOffset('purchases', partitionId, offset);
              }

              consumerNode.data.statusText = '⚠️ Duplicate Skipped (Idempotent)';
              const skipEvent = createSimEvent(
                event.scheduledTime,
                'IDEMPOTENCY_SKIP',
                sourceNodeId,
                `⚠️ [IDEMPOTENCY ACTIVE] Consumer ${sourceNodeId} identified DUPLICATE event ID ${message.id} (${message.payload.purchaseId}). Processed_events idempotency check skipped DB mutation to preserve exact-once business state!`,
                { message, metadata: event.metadata },
              );
              this.recordEvent(skipEvent);
            } else {
              // Not duplicate, OR Idempotency OFF (Duplicate Hazard)
              if (isDuplicate && !this.mechanicsConfig.consumerIdempotency) {
                const hazardEvent = createSimEvent(
                  event.scheduledTime,
                  'IDEMPOTENCY_DISABLED_HAZARD',
                  sourceNodeId,
                  `🚨 [IDEMPOTENCY DISABLED] Consumer ${sourceNodeId} processed DUPLICATE event ID ${message.id} (${message.payload.purchaseId}) and executed DUPLICATE DB mutation! Phantom points/revenue added to read-model!`,
                  { message, metadata: event.metadata },
                );
                this.recordEvent(hazardEvent);
              } else {
                this.processedEvents.add(dedupKey);
              }

              // Commit Kafka offset
              if (group) {
                group.commitOffset('purchases', partitionId, offset);
              }

              // Route mutation to database
              const dbEdge = Array.from(this.edges.values()).find((e) => {
                if (e.source !== sourceNodeId) return false;
                const targetNode = this.nodes.get(e.target);
                return targetNode?.data.kind === 'database';
              });

              if (dbEdge) {
                const dbNodeId = dbEdge.target;
                const dbTransitMs = 350;
                const arriveTime = event.scheduledTime + dbTransitMs;

                this.inFlightTokens.push({
                  id: `token-db-${message.id}-${dbEdge.id}`,
                  messageId: message.id,
                  messageKey: message.key,
                  edgeId: dbEdge.id,
                  sourceNodeId,
                  targetNodeId: dbNodeId,
                  startTime: event.scheduledTime,
                  arriveTime,
                  color: isDuplicate ? '#ef4444' : '#10b981',
                });

                const dbWriteEvent = createSimEvent(
                  arriveTime,
                  'WRITE_DATABASE',
                  dbNodeId,
                  `Database committed read-model update for order ${message.payload.purchaseId} ($${(message.payload.amountCents / 100).toFixed(2)})${isDuplicate ? ' 🚨 [DUPLICATE PHANTOM WRITE]' : ''}`,
                  {
                    message,
                    edgeId: dbEdge.id,
                  },
                );
                this.queue.push(dbWriteEvent);
              }
            }
          }

          // Strict ordering invariant: check if there are further records in partition backlog!
          if (group) {
            const partition = this.kafkaCluster.getPartition('purchases', partitionId);
            const nextCommitted = group.getCommittedNextOffset('purchases', partitionId);
            if (partition && nextCommitted < partition.logEndOffset) {
              const nextRecord = partition.records.find((r) => r.offset === nextCommitted);
              if (nextRecord) {
                this.scheduleConsumerPoll(
                  'purchases',
                  partitionId,
                  nextRecord.offset,
                  nextRecord.message,
                  event.scheduledTime,
                );
              }
            }
          }
        }
        break;
      }

      case 'WRITE_DATABASE': {
        const dbNode = this.nodes.get(event.sourceNodeId);
        if (dbNode && dbNode.data.kind === 'database') {
          dbNode.data.committedRecordCount = (Number(dbNode.data.committedRecordCount) || 0) + 1;
          dbNode.data.status = 'ACTIVE';
        }
        break;
      }

      case 'ROUTE_DLQ': {
        const dlqNode = this.nodes.get(event.sourceNodeId);
        if (dlqNode && dlqNode.data.kind === 'dead-letter-queue') {
          dlqNode.data.deadLetterCount = (Number(dlqNode.data.deadLetterCount) || 0) + 1;
          dlqNode.data.status = 'ACTIVE';
        }
        break;
      }

      case 'WRITE_OUTBOX': {
        const outboxNode = this.nodes.get(event.sourceNodeId);
        if (outboxNode && outboxNode.data.kind === 'outbox-table') {
          outboxNode.data.pendingRecordsCount =
            (Number(outboxNode.data.pendingRecordsCount) || 0) + 1;
          outboxNode.data.status = 'ACTIVE';

          // Check if outbox-table is wired to a cdc-relay
          const cdcEdge = Array.from(this.edges.values()).find(
            (e) => e.source === event.sourceNodeId,
          );
          if (cdcEdge && event.message) {
            const cdcRelayNodeId = cdcEdge.target;
            const cdcRelayNode = this.nodes.get(cdcRelayNodeId);
            if (cdcRelayNode && cdcRelayNode.data.kind === 'cdc-relay') {
              const transitMs = 250;
              const arriveTime = event.scheduledTime + transitMs;
              this.inFlightTokens.push({
                id: `token-cdc-${event.message.id}-${cdcEdge.id}`,
                messageId: event.message.id,
                messageKey: event.message.key,
                edgeId: cdcEdge.id,
                sourceNodeId: event.sourceNodeId,
                targetNodeId: cdcRelayNodeId,
                startTime: event.scheduledTime,
                arriveTime,
                color: '#0891b2',
              });

              const cdcEvent = createSimEvent(
                arriveTime,
                'CDC_RELAY_STREAM',
                cdcRelayNodeId,
                `Debezium CDC tailed outbox row ${event.message.payload.purchaseId}`,
                {
                  message: event.message,
                  metadata: { outboxNodeId: event.sourceNodeId },
                  edgeId: cdcEdge.id,
                },
              );
              this.queue.push(cdcEvent);
            }
          }
        }
        break;
      }

      case 'CDC_RELAY_STREAM': {
        const cdcRelayNode = this.nodes.get(event.sourceNodeId);
        if (!cdcRelayNode || cdcRelayNode.data.kind !== 'cdc-relay' || !event.message) break;

        cdcRelayNode.data.streamedCount = (Number(cdcRelayNode.data.streamedCount) || 0) + 1;
        cdcRelayNode.data.status = 'ACTIVE';

        // Decrement pending on outbox node, increment published
        const outboxNodeId = event.metadata?.outboxNodeId as string;
        if (outboxNodeId) {
          const outboxNode = this.nodes.get(outboxNodeId);
          if (outboxNode && outboxNode.data.kind === 'outbox-table') {
            const currentPending = Number(outboxNode.data.pendingRecordsCount) || 0;
            outboxNode.data.pendingRecordsCount = Math.max(0, currentPending - 1);
            outboxNode.data.publishedRecordsCount =
              (Number(outboxNode.data.publishedRecordsCount) || 0) + 1;
            if (outboxNode.data.pendingRecordsCount === 0) {
              outboxNode.data.status = 'IDLE';
            }
          }
        }

        // Find edge from CDC Relay -> Kafka Cluster
        const kafkaEdge = Array.from(this.edges.values()).find(
          (e) => e.source === event.sourceNodeId,
        );
        if (kafkaEdge) {
          const kafkaNodeId = kafkaEdge.target;
          const topicName = (cdcRelayNode.data.targetTopic as string) || 'purchases';
          const targetPartition = this.kafkaCluster.routeKeyToPartition(
            topicName,
            event.message.key,
          );

          const transitMs = 300;
          const arriveTime = event.scheduledTime + transitMs;
          this.inFlightTokens.push({
            id: `token-relay-kafka-${event.message.id}-${kafkaEdge.id}`,
            messageId: event.message.id,
            messageKey: event.message.key,
            edgeId: kafkaEdge.id,
            sourceNodeId: event.sourceNodeId,
            targetNodeId: kafkaNodeId,
            startTime: event.scheduledTime,
            arriveTime,
            color: '#38bdf8',
          });

          const appendEvent = createSimEvent(
            arriveTime,
            'APPEND_PARTITION',
            kafkaNodeId,
            `Record ${event.message.payload.purchaseId} published via CDC to P${targetPartition}`,
            {
              message: event.message,
              metadata: {
                topic: topicName,
                partitionId: targetPartition,
              },
              edgeId: kafkaEdge.id,
            },
          );
          this.queue.push(appendEvent);
        }
        break;
      }

      default:
        break;
    }
  }

  private scheduleConsumerPoll(
    topicName: string,
    partitionId: number,
    offset: number,
    message: SimMessage,
    currentTime: number,
  ): void {
    // Check all registered consumer groups
    for (const group of this.consumerGroups.values()) {
      // Enforce single-partition ordering: if partition is currently in-flight, record stays in backlog!
      if (group.isPartitionInFlight(partitionId)) {
        continue;
      }

      // auto.offset.reset only applies when the group has no committed offset for this partition
      if (
        !group.hasCommittedOffset(topicName, partitionId) &&
        this.mechanicsConfig.autoOffsetReset === 'latest'
      ) {
        group.seekNextOffset(topicName, partitionId, offset);
      }

      // Consumers always fetch from their committed next offset, never skipping a backlog
      const nextOffset = group.getCommittedNextOffset(topicName, partitionId);
      const nextRecord = this.kafkaCluster
        .getPartition(topicName, partitionId)
        ?.records.find((r) => r.offset === nextOffset);
      if (!nextRecord) continue;
      offset = nextRecord.offset;
      message = nextRecord.message;

      // Find which member instance owns this partition
      for (const [memberId, instance] of group.members.entries()) {
        if (instance.assignedPartitions.includes(partitionId)) {
          // Find consumer node
          const consumerNode = this.nodes.get(memberId);
          if (!consumerNode) continue;

          // Check if consumer is paused, crashed, or this specific partition is paused
          const cData = consumerNode.data as unknown as {
            isPaused?: boolean;
            pausedPartitions?: number[];
            status: string;
          };
          if (
            cData.isPaused ||
            cData.status === 'PAUSED' ||
            cData.status === 'CRASHED' ||
            cData.pausedPartitions?.includes(partitionId)
          ) {
            // Polling paused! Record remains unconsumed in broker log, accumulating lag!
            continue;
          }

          // Lock partition for in-flight processing
          group.lockPartition(partitionId);

          // Auto-commit: offset advances at poll time, before the handler finishes (at-most-once risk)
          if (this.mechanicsConfig.commitMode === 'auto') {
            group.seekNextOffset(topicName, partitionId, offset + 1);
          }

          // Find edge connecting Kafka Cluster -> Consumer
          const dispatchEdge = Array.from(this.edges.values()).find((e) => e.target === memberId);

          const transitMs = 350;
          const arriveTime = currentTime + transitMs;

          if (dispatchEdge) {
            this.inFlightTokens.push({
              id: `token-poll-${message.id}-${dispatchEdge.id}`,
              messageId: message.id,
              messageKey: message.key,
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
            `Consumer ${memberId} polled record offset #${offset} from P${partitionId}`,
            {
              targetNodeId: memberId,
              edgeId: dispatchEdge?.id,
              message,
              metadata: { partitionId, offset, topic: topicName },
            },
          );
          this.queue.push(startEvent);
          break;
        }
      }
    }
  }

  /** Rewinds/advances a group's next-read offset for one partition. Rejected while a record is in flight. */
  public seekPartitionOffset(groupId: string, partitionId: number, nextOffset: number): boolean {
    const group = this.consumerGroups.get(groupId);
    const partition = this.kafkaCluster.getPartition('purchases', partitionId);
    if (!group || !partition || group.isPartitionInFlight(partitionId)) return false;

    const target = Math.min(Math.max(0, nextOffset), partition.logEndOffset);
    group.seekNextOffset('purchases', partitionId, target);
    this.recordEvent(
      createSimEvent(
        this.clock.virtualTimeMs,
        'DISPATCH_MESSAGE',
        'kafka-cluster',
        `🎯 [OFFSET SEEK] Group ${groupId} next-read offset for P${partitionId} set to #${target} (LEO #${partition.logEndOffset}).`,
      ),
    );
    this.resumePartitionBacklog(group, partitionId);
    this.notifyStateUpdated();
    return true;
  }

  /** Seeks every idle partition of a group to earliest (0) or latest (LEO). */
  public resetGroupOffsets(groupId: string, policy: 'earliest' | 'latest'): void {
    for (const p of this.kafkaCluster.getAllPartitions('purchases')) {
      this.seekPartitionOffset(groupId, p.partitionId, policy === 'earliest' ? 0 : p.logEndOffset);
    }
  }

  private resumePartitionBacklog(group: SimConsumerGroupModel, partitionId: number): void {
    const partition = this.kafkaCluster.getPartition('purchases', partitionId);
    if (!partition) return;
    const next = partition.records.find(
      (r) => r.offset === group.getCommittedNextOffset('purchases', partitionId),
    );
    if (next) {
      this.scheduleConsumerPoll(
        'purchases',
        partitionId,
        next.offset,
        next.message,
        this.clock.virtualTimeMs,
      );
    }
  }

  private recordEvent(event: SimEvent): void {
    this.eventLog.unshift(event);
    if (this.eventLog.length > 200) {
      this.eventLog.pop();
    }
    for (const listener of this.listeners) {
      listener.onEventProcessed?.(event);
    }
  }

  public crashBroker(brokerId: number): void {
    const result = this.kafkaCluster.crashBroker(brokerId);
    const time = this.clock.virtualTimeMs;

    this.recordEvent(
      createSimEvent(
        time,
        'CRASH_BROKER',
        'kafka-cluster',
        `Broker B${brokerId} CRASHED! ${result.newController ? `Controller failed over to B${result.newController}.` : ''}`,
      ),
    );

    for (const part of result.affectedPartitions) {
      if (part.newLeader !== undefined) {
        this.recordEvent(
          createSimEvent(
            time,
            'LEADER_FAILOVER',
            'kafka-cluster',
            `Partition P${part.partitionId} leader failed over to Broker B${part.newLeader}`,
            { metadata: { partitionId: part.partitionId, newLeader: part.newLeader } },
          ),
        );
      } else {
        this.recordEvent(
          createSimEvent(
            time,
            'LEADER_FAILOVER',
            'kafka-cluster',
            `Partition P${part.partitionId} has NO ONLINE LEADER! Writes will fail.`,
            { metadata: { partitionId: part.partitionId } },
          ),
        );
      }
    }

    this.notifyStateUpdated();
  }

  public reviveBroker(brokerId: number): void {
    this.kafkaCluster.reviveBroker(brokerId);
    this.recordEvent(
      createSimEvent(
        this.clock.virtualTimeMs,
        'CRASH_BROKER',
        'kafka-cluster',
        `Broker B${brokerId} REVIVED and rejoined ISR.`,
      ),
    );
    this.notifyStateUpdated();
  }

  public crashConsumer(consumerNodeId: string): void {
    const consumerNode = this.nodes.get(consumerNodeId);
    if (!consumerNode || consumerNode.data.kind !== 'consumer') return;

    consumerNode.data.status = 'CRASHED';
    consumerNode.data.assignedPartitions = [];
    consumerNode.data.currentProcessingPartition = undefined;
    consumerNode.data.currentProcessingOffset = undefined;

    const groupId = (consumerNode.data.groupId as string) || 'loyalty-v1';
    const group = this.consumerGroups.get(groupId);
    const topic = this.kafkaCluster.topics.get('purchases');
    const pCount = topic ? topic.partitions.size : 4;

    if (group) {
      group.crashMember(consumerNodeId, pCount);

      // Sync remaining members
      for (const [memberId, instance] of group.members.entries()) {
        const node = this.nodes.get(memberId);
        if (node && node.data.kind === 'consumer' && node.id !== consumerNodeId) {
          node.data.assignedPartitions = [...instance.assignedPartitions];
          node.data.status = instance.assignedPartitions.length > 0 ? 'POLLING' : 'IDLE';
        }
      }
    }

    this.recordEvent(
      createSimEvent(
        this.clock.virtualTimeMs,
        'CRASH_CONSUMER',
        consumerNodeId,
        `Consumer ${consumerNodeId} CRASHED! Group ${groupId} rebalanced partitions across surviving fleet.`,
      ),
    );

    this.notifyStateUpdated();
  }

  public reviveConsumer(consumerNodeId: string): void {
    const consumerNode = this.nodes.get(consumerNodeId);
    if (!consumerNode || consumerNode.data.kind !== 'consumer') return;

    consumerNode.data.status = 'POLLING';

    const groupId = (consumerNode.data.groupId as string) || 'loyalty-v1';
    const group = this.consumerGroups.get(groupId);
    const topic = this.kafkaCluster.topics.get('purchases');
    const pCount = topic ? topic.partitions.size : 4;

    if (group) {
      group.reviveMember(consumerNodeId, pCount);

      // Sync all members
      for (const [memberId, instance] of group.members.entries()) {
        const node = this.nodes.get(memberId);
        if (node && node.data.kind === 'consumer') {
          node.data.assignedPartitions = [...instance.assignedPartitions];
          node.data.status = instance.assignedPartitions.length > 0 ? 'POLLING' : 'IDLE';
        }
      }
    }

    this.recordEvent(
      createSimEvent(
        this.clock.virtualTimeMs,
        'REVIVE_CONSUMER',
        consumerNodeId,
        `Consumer ${consumerNodeId} REVIVED! Rejoined group ${groupId} and triggered rebalance.`,
      ),
    );

    this.notifyStateUpdated();
  }

  private notifyStateUpdated(): void {
    // Calculate live per-partition lag and attach to Kafka Cluster nodes
    const primaryGroup = this.consumerGroups.get('loyalty-v1');
    const topic = this.kafkaCluster.topics.get('purchases');
    if (topic && primaryGroup) {
      const partitionLags: Record<number, number> = {};
      const partitionLogEndOffsets: Record<number, number> = {};
      for (const [pId, part] of topic.partitions.entries()) {
        partitionLags[pId] = primaryGroup.calculateLag('purchases', pId, part.logEndOffset);
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
        }
      }
    }

    for (const listener of this.listeners) {
      listener.onStateUpdated?.();
    }
  }
}

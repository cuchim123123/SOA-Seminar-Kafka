import type { SimEngine } from '../sim-engine';
import type { SimMessage } from '../../domain/types';
import { createSimMessage } from '../../domain/message';
import { createSimEvent } from '../../domain/events';

export class SimMessageDispatcher {
  /**
   * Dispatch a single synthetic purchase message from a producer node
   */
  public static dispatchProducerMessage(
    engine: SimEngine,
    producerNodeId: string,
    customKey?: string,
    messageOptions?: {
      purchaseId?: string;
      itemDescription?: string;
      targetFailedConsumerIds?: string[];
      targetFailedGroupIds?: string[];
    },
  ): SimMessage | null {
    if (engine.clock.status === 'STOPPED') {
      engine.clock.start();
    }

    const producerNode = engine.nodes.get(producerNodeId);
    if (!producerNode) return null;

    // Find outgoing edge from producer
    const publishEdge = Array.from(engine.edges.values()).find((e) => e.source === producerNodeId);
    if (!publishEdge) {
      const errorEvent = createSimEvent(
        engine.clock.virtualTimeMs,
        'DISPATCH_MESSAGE',
        producerNodeId,
        `Cannot dispatch: Producer is not wired to a target node`,
      );
      engine.recordEvent(errorEvent);
      return null;
    }

    const targetNodeId = publishEdge.target;
    const targetNode = engine.nodes.get(targetNodeId);
    const key = customKey || (producerNode.data.messageKey as string) || 'U001';
    const message = createSimMessage(key, 4999, engine.clock.virtualTimeMs, messageOptions);

    // Save as last dispatched message for duplicate testing
    engine.lastDispatchedMessage = message;

    // Case 1: Wired to Transactional Outbox Table
    if (targetNode && targetNode.data.kind === 'outbox-table') {
      const dispatchEvent = createSimEvent(
        engine.clock.virtualTimeMs,
        'DISPATCH_MESSAGE',
        producerNodeId,
        `Producer committed local transaction to Outbox Table for key "${key}" (${message.payload.purchaseId})`,
        {
          targetNodeId,
          edgeId: publishEdge.id,
          message,
        },
      );
      engine.recordEvent(dispatchEvent);

      const transitMs = 250;
      const arriveTime = engine.clock.virtualTimeMs + transitMs;

      engine.inFlightTokens.push({
        id: engine.nextTokenId(`token-outbox-${message.id}`),
        messageId: message.id,
        messageKey: key,
        edgeId: publishEdge.id,
        sourceNodeId: producerNodeId,
        targetNodeId,
        startTime: engine.clock.virtualTimeMs,
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
      engine.queue.push(writeOutboxEvent);
      engine.notifyStateUpdated();
      return message;
    }

    // Case 2: Directly wired to Kafka Broker (or Cluster)
    const topic = (producerNode.data.targetTopic as string) || 'purchases';
    const isRoundRobin = engine.mechanicsConfig.keyOrderingMode === 'ROUND_ROBIN';
    let targetPartition: number;
    if (isRoundRobin) {
      const pCount = engine.kafkaCluster.topics.get(topic)?.partitions.size || 4;
      targetPartition = engine.roundRobinIndex++ % pCount;
    } else {
      targetPartition = engine.kafkaCluster.routeKeyToPartition(topic, key);
    }

    // Resolve partition leader broker node
    const partition = engine.kafkaCluster.getPartition(topic, targetPartition);
    const leaderBrokerId = partition?.leaderBrokerId ?? 1;
    const leaderBrokerNode = Array.from(engine.nodes.values()).find(
      (n) => n.data.kind === 'kafka-broker' && n.data.brokerId === leaderBrokerId,
    );

    // If an edge directly connects to the leader broker, use that edge.
    // Otherwise, route through the existing publishEdge but record the destination
    const effectiveEdge =
      Array.from(engine.edges.values()).find(
        (e) => e.source === producerNodeId && e.target === leaderBrokerNode?.id,
      ) || publishEdge;

    const effectiveTargetNodeId = leaderBrokerNode ? leaderBrokerNode.id : targetNodeId;

    const routingNote = isRoundRobin
      ? `⚠️ ROUND-ROBIN ROUTED to P${targetPartition} [Leader: B${leaderBrokerId}] (Key hashing disabled!)`
      : `Murmur2("${key}") ➔ Target P${targetPartition} [Leader: B${leaderBrokerId}] (Strict FIFO)`;

    const dispatchEvent = createSimEvent(
      engine.clock.virtualTimeMs,
      'DISPATCH_MESSAGE',
      producerNodeId,
      `Producer generated order ${message.payload.purchaseId} for key "${key}" (${routingNote})`,
      {
        targetNodeId: effectiveTargetNodeId,
        edgeId: effectiveEdge.id,
        message,
      },
    );
    engine.recordEvent(dispatchEvent);

    // Producer Acks handling
    const effectiveAcks =
      engine.mechanicsConfig.producerAcks || (producerNode.data.acks as string) || 'all';
    if (effectiveAcks === '0') {
      const ackEvent = createSimEvent(
        engine.clock.virtualTimeMs,
        'DISPATCH_MESSAGE',
        producerNodeId,
        `⚡ [acks=0] Fire-and-forget: Producer acknowledged immediately without waiting for broker commit log!`,
      );
      engine.recordEvent(ackEvent);
    }

    // Schedule in-flight transit across edge
    const transitMs = 400;
    const arriveTime = engine.clock.virtualTimeMs + transitMs;

    engine.inFlightTokens.push({
      id: engine.nextTokenId(`token-${message.id}`),
      messageId: message.id,
      messageKey: key,
      edgeId: effectiveEdge.id,
      sourceNodeId: producerNodeId,
      targetNodeId: effectiveTargetNodeId,
      startTime: engine.clock.virtualTimeMs,
      arriveTime,
      color: '#38bdf8',
    });

    // Schedule append partition event
    const appendEvent = createSimEvent(
      arriveTime,
      'APPEND_PARTITION',
      effectiveTargetNodeId,
      `Record ${message.payload.purchaseId} stored in partition P${targetPartition} on Leader Broker B${leaderBrokerId}`,
      {
        edgeId: effectiveEdge.id,
        message,
        metadata: {
          partitionId: targetPartition,
          topic,
          leaderBrokerId,
        },
      },
    );
    engine.queue.push(appendEvent);
    engine.notifyStateUpdated();
    return message;
  }

  /**
   * Dispatch duplicate message to test consumer idempotency filter vs phantom writes
   */
  public static dispatchDuplicateMessage(engine: SimEngine, customKey?: string): SimMessage | null {
    if (engine.clock.status === 'STOPPED') {
      engine.clock.start();
    }

    const producerNode = Array.from(engine.nodes.values()).find((n) => n.data.kind === 'producer');
    if (!producerNode) return null;

    let messageToDup: SimMessage;
    if (engine.lastDispatchedMessage) {
      // Re-use exact same message payload & ID to simulate transport retry / at-least-once duplicate
      messageToDup = {
        ...engine.lastDispatchedMessage,
        timestampMs: engine.clock.virtualTimeMs,
      };
    } else {
      const key = customKey || (producerNode.data.messageKey as string) || 'U001';
      messageToDup = createSimMessage(key, 5000, engine.clock.virtualTimeMs);
      engine.lastDispatchedMessage = messageToDup;
    }

    const publishEdge = Array.from(engine.edges.values()).find((e) => e.source === producerNode.id);
    if (!publishEdge) return null;

    const targetNodeId = publishEdge.target;
    const targetNode = engine.nodes.get(targetNodeId);

    // Case 1: Wired to Transactional Outbox Table
    if (targetNode && targetNode.data.kind === 'outbox-table') {
      const dispatchEvent = createSimEvent(
        engine.clock.virtualTimeMs,
        'DISPATCH_MESSAGE',
        producerNode.id,
        `🚨 [DUPLICATE INJECTION] Producer re-sent DUPLICATE event ID ${messageToDup.id} (${messageToDup.payload.purchaseId}) for key "${messageToDup.key}" into Outbox Table`,
        {
          targetNodeId,
          edgeId: publishEdge.id,
          message: messageToDup,
        },
      );
      engine.recordEvent(dispatchEvent);

      const transitMs = 250;
      const arriveTime = engine.clock.virtualTimeMs + transitMs;

      engine.inFlightTokens.push({
        id: engine.nextTokenId(`token-outbox-dup-${messageToDup.id}`),
        messageId: messageToDup.id,
        messageKey: messageToDup.key,
        edgeId: publishEdge.id,
        sourceNodeId: producerNode.id,
        targetNodeId,
        startTime: engine.clock.virtualTimeMs,
        arriveTime,
        color: '#f59e0b',
      });

      const writeOutboxEvent = createSimEvent(
        arriveTime,
        'WRITE_OUTBOX',
        targetNodeId,
        `Outbox Table buffered pending duplicate record ${messageToDup.payload.purchaseId}`,
        {
          edgeId: publishEdge.id,
          message: messageToDup,
        },
      );
      engine.queue.push(writeOutboxEvent);
      engine.notifyStateUpdated();
      return messageToDup;
    }

    // Case 2: Directly wired to Kafka Cluster
    const topic = (producerNode.data.targetTopic as string) || 'purchases';
    const targetPartition = engine.kafkaCluster.routeKeyToPartition(topic, messageToDup.key);

    const dispatchEvent = createSimEvent(
      engine.clock.virtualTimeMs,
      'DISPATCH_MESSAGE',
      producerNode.id,
      `🚨 [DUPLICATE INJECTION] Producer re-sent DUPLICATE event ID ${messageToDup.id} (${messageToDup.payload.purchaseId}) for key "${messageToDup.key}" into P${targetPartition}`,
      {
        targetNodeId,
        edgeId: publishEdge.id,
        message: messageToDup,
      },
    );
    engine.recordEvent(dispatchEvent);

    const transitMs = 400;
    const arriveTime = engine.clock.virtualTimeMs + transitMs;

    engine.inFlightTokens.push({
      id: engine.nextTokenId(`token-dup-${messageToDup.id}`),
      messageId: messageToDup.id,
      messageKey: messageToDup.key,
      edgeId: publishEdge.id,
      sourceNodeId: producerNode.id,
      targetNodeId,
      startTime: engine.clock.virtualTimeMs,
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
        metadata: {
          partitionId: targetPartition,
          topic,
        },
      },
    );
    engine.queue.push(appendEvent);
    engine.notifyStateUpdated();
    return messageToDup;
  }

  /**
   * Dispatch an ordered burst of messages with identical key
   */
  public static dispatchOrderedBurst(
    engine: SimEngine,
    customerKey: string = 'U001',
    count: number = 3,
  ): void {
    if (engine.clock.status === 'STOPPED') {
      engine.clock.start();
    }

    const producerNode = Array.from(engine.nodes.values()).find((n) => n.data.kind === 'producer');
    if (!producerNode) return;

    const publishEdge = Array.from(engine.edges.values()).find((e) => e.source === producerNode.id);
    if (!publishEdge) return;

    const targetNodeId = publishEdge.target;
    const targetNode = engine.nodes.get(targetNodeId);
    const topic = (producerNode.data.targetTopic as string) || 'purchases';
    const isRoundRobin = engine.mechanicsConfig.keyOrderingMode === 'ROUND_ROBIN';

    for (let i = 0; i < count; i++) {
      const message = createSimMessage(
        customerKey,
        1500 + i * 500,
        engine.clock.virtualTimeMs + i * 120,
      );
      engine.lastDispatchedMessage = message;

      // Handle Outbox Table burst
      if (targetNode && targetNode.data.kind === 'outbox-table') {
        const dispatchTime = engine.clock.virtualTimeMs + i * 120;
        const arriveTime = dispatchTime + 250;

        const dispatchEvent = createSimEvent(
          dispatchTime,
          'DISPATCH_MESSAGE',
          producerNode.id,
          `⚡ [BURST #${i + 1}/${count}] Producer committed order ${message.payload.purchaseId} to Outbox Table`,
          { targetNodeId, edgeId: publishEdge.id, message },
        );
        engine.recordEvent(dispatchEvent);

        engine.inFlightTokens.push({
          id: engine.nextTokenId(`token-burst-outbox-${message.id}`),
          messageId: message.id,
          messageKey: customerKey,
          edgeId: publishEdge.id,
          sourceNodeId: producerNode.id,
          targetNodeId,
          startTime: dispatchTime,
          arriveTime,
          color: '#f97316',
        });

        const writeOutboxEvent = createSimEvent(
          arriveTime,
          'WRITE_OUTBOX',
          targetNodeId,
          `Outbox buffered burst record ${message.payload.purchaseId}`,
          { edgeId: publishEdge.id, message },
        );
        engine.queue.push(writeOutboxEvent);
        continue;
      }

      // Handle direct Kafka Cluster burst
      let targetPartition: number;
      if (isRoundRobin) {
        const pCount = engine.kafkaCluster.topics.get(topic)?.partitions.size || 4;
        targetPartition = engine.roundRobinIndex++ % pCount;
      } else {
        targetPartition = engine.kafkaCluster.routeKeyToPartition(topic, customerKey);
      }

      const dispatchTime = engine.clock.virtualTimeMs + i * 120;
      const arriveTime = dispatchTime + 400;

      const routingNote = isRoundRobin
        ? `⚠️ ROUND-ROBIN ROUTED to P${targetPartition} (Key hashing disabled!)`
        : `Murmur2("${customerKey}") ➔ P${targetPartition} (FIFO Guaranteed)`;

      const dispatchEvent = createSimEvent(
        dispatchTime,
        'DISPATCH_MESSAGE',
        producerNode.id,
        `⚡ [BURST #${i + 1}/${count}] Order ${message.payload.purchaseId} (${routingNote})`,
        { targetNodeId, edgeId: publishEdge.id, message },
      );
      engine.recordEvent(dispatchEvent);

      engine.inFlightTokens.push({
        id: engine.nextTokenId(`token-burst-${message.id}`),
        messageId: message.id,
        messageKey: customerKey,
        edgeId: publishEdge.id,
        sourceNodeId: producerNode.id,
        targetNodeId,
        startTime: dispatchTime,
        arriveTime,
        color: '#6366f1',
      });

      const appendEvent = createSimEvent(
        arriveTime,
        'APPEND_PARTITION',
        targetNodeId,
        `Burst Record ${message.payload.purchaseId} stored in P${targetPartition}`,
        {
          edgeId: publishEdge.id,
          message,
          metadata: {
            partitionId: targetPartition,
            topic,
            burstIndex: i + 1,
            burstTotal: count,
          },
        },
      );
      engine.queue.push(appendEvent);
    }

    engine.notifyStateUpdated();
  }
}

import type { SimEngine } from '../sim-engine';
import type { SimConsumerData, SimEvent } from '../../domain/types';
import { createSimEvent } from '../../domain/events';
import { SimClusterOperations } from '../operations/sim-cluster-operations';

export class SimEventHandlers {
  public static executeEvent(engine: SimEngine, event: SimEvent): void {
    engine.recordEvent(event);

    switch (event.type) {
      case 'APPEND_PARTITION': {
        const { message, metadata } = event;
        if (!message || !metadata) break;

        const partitionId = Number(metadata.partitionId ?? 0);
        const topicName = (metadata.topic as string) || 'purchases';

        // Append to broker partition log
        const { offset } = engine.kafkaCluster.appendRecord(
          topicName,
          partitionId,
          message,
          event.scheduledTime,
        );

        // Record ACK confirmation into timeline
        const effectiveAcks = engine.mechanicsConfig.producerAcks;
        if (effectiveAcks === '1') {
          engine.recordEvent(
            createSimEvent(
              event.scheduledTime,
              'DISPATCH_MESSAGE',
              event.sourceNodeId || 'kafka-cluster',
              `⚡ [acks=1] Leader ACK: Partition Leader confirmed write of record ${message.payload.purchaseId} at offset ${offset}.`,
            ),
          );
        } else if (effectiveAcks === 'all') {
          engine.recordEvent(
            createSimEvent(
              event.scheduledTime,
              'DISPATCH_MESSAGE',
              event.sourceNodeId || 'kafka-cluster',
              `⚡ [acks=all] ISR Quorum ACK: Leader and all In-Sync Replicas committed offset ${offset} to persistent log.`,
            ),
          );
        }

        // Schedule consumer poll for this partition record
        SimClusterOperations.scheduleConsumerPoll(
          engine,
          topicName,
          partitionId,
          offset,
          message,
          event.scheduledTime,
        );
        break;
      }

      case 'START_PROCESSING': {
        const { message, targetNodeId, metadata } = event;
        if (!targetNodeId || !message) break;

        const consumerNode = engine.nodes.get(targetNodeId);
        if (consumerNode && consumerNode.data.kind === 'consumer') {
          if (consumerNode.data.status === 'CRASHED') break;

          consumerNode.data.status = 'PROCESSING';
          const pId = Number(metadata?.partitionId ?? 0);
          const off = Number(metadata?.offset ?? 0);
          consumerNode.data.currentProcessingPartition = pId;
          consumerNode.data.currentProcessingOffset = off;

          const groupId = (consumerNode.data.groupId as string) || 'loyalty-v1';
          const group = engine.consumerGroups.get(groupId);
          const instance = group?.members.get(targetNodeId);
          if (instance) {
            instance.currentProcessingPartition = pId;
            instance.currentProcessingOffset = off;
          }

          const delay = (consumerNode.data.processingDelayMs as number) || 120;

          const completeEvent = createSimEvent(
            event.scheduledTime + delay,
            'COMPLETE_PROCESSING',
            targetNodeId,
            `Consumer ${targetNodeId} completed processing order ${message.payload.purchaseId} (P${metadata?.partitionId}:#${metadata?.offset})`,
            { message, metadata: event.metadata },
          );
          engine.queue.push(completeEvent);
        }
        break;
      }

      case 'COMPLETE_PROCESSING': {
        const { message, sourceNodeId, metadata } = event;
        if (!message || !metadata) break;

        const consumerNode = engine.nodes.get(sourceNodeId);
        if (consumerNode && consumerNode.data.kind === 'consumer') {
          const consumerData = consumerNode.data as SimConsumerData;
          if (consumerData.status === 'CRASHED') break;

          const isPaused = Boolean(consumerData.isPaused || consumerData.status === 'PAUSED');
          consumerData.status = isPaused ? 'PAUSED' : 'POLLING';
          consumerData.currentProcessingPartition = undefined;
          consumerData.currentProcessingOffset = undefined;
          engine.processedMessageCount += 1;

          const groupId = consumerData.groupId || 'loyalty-v1';
          const group = engine.consumerGroups.get(groupId);
          const instance = group?.members.get(sourceNodeId);
          if (instance) {
            instance.currentProcessingPartition = undefined;
            instance.currentProcessingOffset = undefined;
          }
          const partitionId = Number(metadata.partitionId ?? 0);
          const offset = Number(metadata.offset ?? 0);
          const topicName = (metadata.topic as string) || 'purchases';

          const isGlobalPoisonPill =
            message.key === 'POISON_PILL' || message.payload.purchaseId.startsWith('POISON');
          const targetFailedConsumers = message.payload.metadata?.targetFailedConsumerIds as
            string[] | undefined;
          const targetFailedGroups = message.payload.metadata?.targetFailedGroupIds as
            string[] | undefined;

          let isPoisonPillForThisConsumer = false;
          if (isGlobalPoisonPill) {
            if (targetFailedConsumers && targetFailedConsumers.length > 0) {
              isPoisonPillForThisConsumer = targetFailedConsumers.includes(sourceNodeId);
            } else if (targetFailedGroups && targetFailedGroups.length > 0) {
              isPoisonPillForThisConsumer = targetFailedGroups.includes(groupId);
            } else {
              isPoisonPillForThisConsumer = true;
            }
          }

          if (isPoisonPillForThisConsumer) {
            // Check if Dead-Letter Queue component is wired to this consumer
            const dlqEdge = Array.from(engine.edges.values()).find((e) => {
              if (e.source !== sourceNodeId) return false;
              const targetNode = engine.nodes.get(e.target);
              return targetNode?.data.kind === 'dead-letter-queue';
            });

            if (dlqEdge) {
              // DLQ Component is wired: Commit offset to avoid head-of-line blocking, and route to DLQ node!
              if (group) {
                group.commitOffset(topicName, partitionId, offset);
              }

              const dlqNodeId = dlqEdge.target;
              const dlqTransitMs = 350;
              const arriveTime = event.scheduledTime + dlqTransitMs;

              engine.inFlightTokens.push({
                id: engine.nextTokenId(`token-dlq-${message.id}`),
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
              engine.queue.push(dlqEvent);

              // Release in-flight partition lock
              if (group) {
                group.unlockPartition(partitionId);
              }

              // Resume any pending backlog on this partition
              const isPartitionCut = Boolean(consumerData.pausedPartitions?.includes(partitionId));
              if (group && !isPaused && !isPartitionCut) {
                SimClusterOperations.resumePartitionBacklog(engine, group, partitionId);
              }
            } else {
              // DLQ Component NOT wired -> Head-of-Line Blocking!
              consumerNode.data.status = 'ERROR';
              consumerNode.data.statusText = '💥 Stalled by Poison Pill (No DLQ Added)';

              const errorEvent = createSimEvent(
                event.scheduledTime,
                'CONSUMER_ERROR',
                sourceNodeId,
                `💥 [HEAD-OF-LINE BLOCKING] Consumer crashed on poison-pill ${message.payload.purchaseId}! No DLQ component added. Offset NOT committed. Partition P${partitionId} consumption stalled!`,
                { message, metadata: event.metadata },
              );
              engine.recordEvent(errorEvent);
              break; // Abort: do not unlock partition or process next record!
            }
          } else {
            // Normal Record: Check Idempotency Deduplication
            const dedupKey = `${groupId}:${message.id}`;
            const isDuplicate = engine.processedEvents.has(dedupKey);

            if (isDuplicate && engine.mechanicsConfig.consumerIdempotency) {
              // Idempotency ON: Duplicate detected! Commit offset, but skip DB mutation!
              if (group) {
                group.commitOffset(topicName, partitionId, offset);
              }

              consumerNode.data.statusText = '⚠️ Duplicate Skipped (Idempotent)';
              const skipEvent = createSimEvent(
                event.scheduledTime,
                'IDEMPOTENCY_SKIP',
                sourceNodeId,
                `⚠️ [IDEMPOTENCY ACTIVE] Consumer ${sourceNodeId} identified DUPLICATE event ID ${message.id} (${message.payload.purchaseId}). Processed_events idempotency check skipped DB mutation to preserve exact-once business state!`,
                { message, metadata: event.metadata },
              );
              engine.recordEvent(skipEvent);
            } else {
              // Not duplicate, OR Idempotency OFF (Duplicate Hazard)
              if (isDuplicate && !engine.mechanicsConfig.consumerIdempotency) {
                const hazardEvent = createSimEvent(
                  event.scheduledTime,
                  'IDEMPOTENCY_DISABLED_HAZARD',
                  sourceNodeId,
                  `🚨 [IDEMPOTENCY DISABLED] Consumer ${sourceNodeId} processed DUPLICATE event ID ${message.id} (${message.payload.purchaseId}) and executed DUPLICATE DB mutation! Phantom points/revenue added to read-model!`,
                  { message, metadata: event.metadata },
                );
                engine.recordEvent(hazardEvent);
              } else {
                engine.processedEvents.add(dedupKey);
              }

              // Commit Kafka offset
              if (group) {
                group.commitOffset(topicName, partitionId, offset);
              }

              // Route mutation to database
              const dbEdge = Array.from(engine.edges.values()).find((e) => {
                if (e.source !== sourceNodeId) return false;
                const targetNode = engine.nodes.get(e.target);
                return targetNode?.data.kind === 'database';
              });

              if (dbEdge) {
                const dbNodeId = dbEdge.target;
                const dbTransitMs = 350;
                const arriveTime = event.scheduledTime + dbTransitMs;

                engine.inFlightTokens.push({
                  id: engine.nextTokenId(`token-db-${message.id}`),
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
                engine.queue.push(dbWriteEvent);
              }
            }

            // Release in-flight partition lock so next backlog record can be polled!
            if (group) {
              group.unlockPartition(partitionId);
            }

            // Strict ordering invariant: check if there are further records in partition backlog!
            const isPartitionCut = Boolean(consumerData.pausedPartitions?.includes(partitionId));
            if (group && !isPaused && !isPartitionCut) {
              const partition = engine.kafkaCluster.getPartition(topicName, partitionId);
              const nextCommitted = group.getCommittedNextOffset(topicName, partitionId);
              if (partition && nextCommitted < partition.logEndOffset) {
                const nextRecord = partition.records.find((r) => r.offset === nextCommitted);
                if (nextRecord) {
                  SimClusterOperations.scheduleConsumerPoll(
                    engine,
                    topicName,
                    partitionId,
                    nextRecord.offset,
                    nextRecord.message,
                    event.scheduledTime,
                  );
                }
              }
            }
          }
        }
        break;
      }

      case 'WRITE_DATABASE': {
        const dbNode = engine.nodes.get(event.sourceNodeId);
        if (dbNode && dbNode.data.kind === 'database') {
          dbNode.data.committedRecordCount = (Number(dbNode.data.committedRecordCount) || 0) + 1;
          dbNode.data.status = 'ACTIVE';
        }
        break;
      }

      case 'ROUTE_DLQ': {
        const dlqNode = engine.nodes.get(event.sourceNodeId);
        if (dlqNode && dlqNode.data.kind === 'dead-letter-queue') {
          dlqNode.data.deadLetterCount = (Number(dlqNode.data.deadLetterCount) || 0) + 1;
          dlqNode.data.status = 'ACTIVE';
        }
        break;
      }

      case 'WRITE_OUTBOX': {
        const outboxNode = engine.nodes.get(event.sourceNodeId);
        if (outboxNode && outboxNode.data.kind === 'outbox-table') {
          outboxNode.data.pendingRecordsCount =
            (Number(outboxNode.data.pendingRecordsCount) || 0) + 1;
          outboxNode.data.status = 'ACTIVE';

          // Check if outbox-table is wired to a cdc-relay
          const cdcEdge = Array.from(engine.edges.values()).find(
            (e) => e.source === event.sourceNodeId,
          );
          if (cdcEdge && event.message) {
            const cdcRelayNodeId = cdcEdge.target;
            const cdcRelayNode = engine.nodes.get(cdcRelayNodeId);
            if (cdcRelayNode && cdcRelayNode.data.kind === 'cdc-relay') {
              const transitMs = 250;
              const arriveTime = event.scheduledTime + transitMs;
              engine.inFlightTokens.push({
                id: engine.nextTokenId(`token-cdc-${event.message.id}`),
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
              engine.queue.push(cdcEvent);
            }
          }
        }
        break;
      }

      case 'CDC_RELAY_STREAM': {
        const cdcRelayNode = engine.nodes.get(event.sourceNodeId);
        if (!cdcRelayNode || cdcRelayNode.data.kind !== 'cdc-relay' || !event.message) break;

        cdcRelayNode.data.streamedCount = (Number(cdcRelayNode.data.streamedCount) || 0) + 1;
        cdcRelayNode.data.status = 'ACTIVE';

        // Decrement pending on outbox node, increment published
        const outboxNodeId = event.metadata?.outboxNodeId as string;
        if (outboxNodeId) {
          const outboxNode = engine.nodes.get(outboxNodeId);
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

        // Find edge from CDC Relay -> Kafka Broker (or Cluster)
        const kafkaEdge = Array.from(engine.edges.values()).find(
          (e) => e.source === event.sourceNodeId,
        );
        if (kafkaEdge) {
          const defaultKafkaNodeId = kafkaEdge.target;
          const topicName = (cdcRelayNode.data.targetTopic as string) || 'purchases';
          const targetPartition = engine.kafkaCluster.routeKeyToPartition(
            topicName,
            event.message.key,
          );

          // Find partition leader broker node
          const partition = engine.kafkaCluster.getPartition(topicName, targetPartition);
          const leaderBrokerId = partition?.leaderBrokerId ?? 1;
          const leaderBrokerNode = Array.from(engine.nodes.values()).find(
            (n) => n.data.kind === 'kafka-broker' && n.data.brokerId === leaderBrokerId,
          );

          const effectiveEdge =
            Array.from(engine.edges.values()).find(
              (e) => e.source === event.sourceNodeId && e.target === leaderBrokerNode?.id,
            ) || kafkaEdge;

          const targetKafkaNodeId = leaderBrokerNode ? leaderBrokerNode.id : defaultKafkaNodeId;

          const transitMs = 300;
          const arriveTime = event.scheduledTime + transitMs;
          engine.inFlightTokens.push({
            id: engine.nextTokenId(`token-relay-kafka-${event.message.id}`),
            messageId: event.message.id,
            messageKey: event.message.key,
            edgeId: effectiveEdge.id,
            sourceNodeId: event.sourceNodeId,
            targetNodeId: targetKafkaNodeId,
            startTime: event.scheduledTime,
            arriveTime,
            color: '#38bdf8',
          });

          const appendEvent = createSimEvent(
            arriveTime,
            'APPEND_PARTITION',
            targetKafkaNodeId,
            `Record ${event.message.payload.purchaseId} published via CDC to P${targetPartition} on Leader Broker B${leaderBrokerId}`,
            {
              message: event.message,
              metadata: {
                topic: topicName,
                partitionId: targetPartition,
                leaderBrokerId,
              },
              edgeId: effectiveEdge.id,
            },
          );
          engine.queue.push(appendEvent);
        }
        break;
      }
    }
  }
}

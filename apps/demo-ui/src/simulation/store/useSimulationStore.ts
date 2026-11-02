import { useState, useEffect, useRef, useCallback } from 'react';
import type { Connection, EdgeChange, NodeChange } from '@xyflow/react';
import { applyNodeChanges, applyEdgeChanges } from '@xyflow/react';
import { SimEngine } from '../engine/sim-engine';
import { COMPONENT_REGISTRY, isValidConnection } from '../registry/component-registry';
import { validateTopology } from '../domain/topology-validation';
import { scaleConsumers } from '../domain/scale-consumers';
import type {
  SimCustomNode,
  SimKafkaBrokerData,
  SimCustomEdge,
  ComponentKind,
  SimProducerAcks,
  SimCommitMode,
  SimOffsetResetPolicy,
} from '../domain/types';
import type { SimEvent } from '../domain/events';
import { SYSTEM_BLUEPRINTS } from '../blueprints/systemBlueprints';

let nextNodeId = 1;
function allocateNodeId(kind: ComponentKind, nodes: SimCustomNode[]) {
  let id: string;
  do id = `node-${kind}-${++nextNodeId}`;
  while (nodes.some((node) => node.id === id || node.data.instanceId === id));
  return id;
}

export function useSimulationStore() {
  const engineRef = useRef<SimEngine | null>(null);
  if (!engineRef.current) engineRef.current = new SimEngine();
  const engine = engineRef.current;

  const [validationMessage, setValidationMessage] = useState('');
  const [nodes, setNodes] = useState<SimCustomNode[]>([]);
  const [edges, setEdges] = useState<SimCustomEdge[]>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [activeBlueprintId, setActiveBlueprintId] = useState<string>('full-enterprise-production');
  const [eventLog, setEventLog] = useState<SimEvent[]>([]);
  const [virtualTimeMs, setVirtualTimeMs] = useState<number>(0);
  const [playbackStatus, setPlaybackStatus] = useState<'STOPPED' | 'RUNNING' | 'PAUSED'>('STOPPED');
  const [speedMultiplier, setSpeedMultiplier] = useState<number>(1.0);
  const [inFlightTokens, setInFlightTokens] = useState(engine.inFlightTokens);
  const [mechanicsConfig, setMechanicsConfig] = useState({ ...engine.mechanicsConfig });

  // Sync engine state to React
  const syncFromEngine = useCallback(() => {
    setVirtualTimeMs(engine.clock.virtualTimeMs);
    setPlaybackStatus(engine.clock.status);
    setSpeedMultiplier(engine.clock.speedMultiplier);
    setEventLog([...engine.eventLog]);
    setInFlightTokens([...engine.inFlightTokens]);
    setMechanicsConfig({ ...engine.mechanicsConfig });
  }, [engine]);

  const pauseForEdit = useCallback(() => {
    engine.clock.pause();
    syncFromEngine();
  }, [engine, syncFromEngine]);
  const topologyIssues = validateTopology(nodes, edges, new Set(engine.kafkaCluster.topics.keys()));
  const readyToExecute = useCallback(() => {
    engine.setTopology(nodes, edges);
    const issues = validateTopology(nodes, edges, new Set(engine.kafkaCluster.topics.keys()));
    for (const topic of engine.kafkaCluster.topics.values())
      if (topic.replicationFactor > engine.kafkaCluster.brokers.size)
        issues.push(`Topic ${topic.name}: replication factor exceeds the visible broker count.`);
    if (issues.length) {
      engine.clock.pause();
      setValidationMessage(issues.join(' '));
      syncFromEngine();
      return false;
    }
    setValidationMessage('');
    return true;
  }, [engine, nodes, edges, syncFromEngine]);

  // Animation frame loop when simulation is running
  useEffect(() => {
    let animId: number;
    let lastRealTime = performance.now();

    const loop = (currentTime: number) => {
      const deltaMs = currentTime - lastRealTime;
      lastRealTime = currentTime;

      if (engine.clock.status === 'RUNNING') {
        engine.tick(deltaMs);
        syncFromEngine();
      }

      animId = requestAnimationFrame(loop);
    };

    animId = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(animId);
  }, [engine, syncFromEngine]);

  // Sync topology to engine whenever nodes or edges change
  useEffect(() => {
    engine.setTopology(nodes, edges);
  }, [nodes, edges, engine]);

  // Node & Edge changes from React Flow
  const onNodesChange = useCallback(
    (changes: NodeChange<SimCustomNode>[]) => {
      if (changes.some((change) => !['position', 'dimensions', 'select'].includes(change.type)))
        pauseForEdit();
      setNodes((nds) => applyNodeChanges(changes, nds));
    },
    [pauseForEdit],
  );

  const onEdgesChange = useCallback(
    (changes: EdgeChange<SimCustomEdge>[]) => {
      if (changes.some((change) => change.type !== 'select')) pauseForEdit();
      setEdges((eds) => applyEdgeChanges(changes, eds));
    },
    [pauseForEdit],
  );

  // Wire connection validation and creation
  const onConnect = useCallback(
    (connection: Connection) => {
      if (!connection.source || !connection.target) return;

      const sourceNode = nodes.find((n) => n.id === connection.source);
      const targetNode = nodes.find((n) => n.id === connection.target);

      if (!sourceNode || !targetNode) return;

      const isValid = isValidConnection(sourceNode.data.kind, targetNode.data.kind);
      if (!isValid) {
        setValidationMessage(
          `Invalid connection: Cannot wire ${sourceNode.data.title} (${sourceNode.data.kind}) to ${targetNode.data.title} (${targetNode.data.kind}).`,
        );
        return;
      }

      if (
        edges.some((edge) => edge.source === connection.source && edge.target === connection.target)
      ) {
        setValidationMessage('These components are already connected.');
        return;
      }
      pauseForEdit();
      setValidationMessage('');
      const newEdge: SimCustomEdge = {
        id: `edge-${connection.source}-${connection.target}`,
        source: connection.source,
        target: connection.target,
        type: 'simFlowEdge',
        data: {
          portType:
            sourceNode.data.kind === 'producer'
              ? 'publish'
              : sourceNode.data.kind === 'kafka-cluster'
                ? 'dispatch'
                : 'handler-action',
        },
      };

      setEdges((eds) => [...eds, newEdge]);
    },
    [nodes, edges, pauseForEdit],
  );

  // Add node from Palette drag-and-drop
  const addNodeFromPalette = useCallback(
    (kind: ComponentKind, position: { x: number; y: number }) => {
      const def = COMPONENT_REGISTRY.find((c) => c.kind === kind);
      if (!def) return;
      pauseForEdit();

      const id = allocateNodeId(kind, nodes);

      let nodeType = `${kind}Node`;
      if (kind === 'kafka-broker') nodeType = 'kafkaBrokerNode';
      if (kind === 'kafka-cluster') nodeType = 'kafkaClusterNode';
      if (kind === 'dead-letter-queue') nodeType = 'deadLetterQueueNode';
      if (kind === 'outbox-table') nodeType = 'outboxTableNode';
      if (kind === 'cdc-relay') nodeType = 'cdcRelayNode';

      let initialData: Record<string, unknown> = {
        ...def.defaultData,
        id,
        kind,
      };

      if (kind === 'kafka-broker') {
        const existingBrokers = nodes
          .filter((n) => n.data.kind === 'kafka-broker')
          .map((n) => (n.data as SimKafkaBrokerData).brokerId || 1);
        const nextBrokerId = existingBrokers.length > 0 ? Math.max(...existingBrokers) + 1 : 1;
        initialData = {
          ...initialData,
          brokerId: nextBrokerId,
          host: `broker-${nextBrokerId}:9092`,
          title: `Broker B${nextBrokerId}`,
          isController: nextBrokerId === 1,
        };
      }

      if (kind === 'consumer') {
        const uniqueGroupId = `consumer-group-${nextNodeId}`;
        initialData = {
          ...initialData,
          groupId: uniqueGroupId,
          instanceId: `consumer-${nextNodeId}`,
          title: `Consumer (${uniqueGroupId})`,
        };
      }

      const newNode: SimCustomNode = {
        id,
        type: nodeType,
        position,
        data: initialData as unknown as SimCustomNode['data'],
      };

      setNodes((nds) => [...nds, newNode]);
      setSelectedNodeId(id);
    },
    [nodes, engine, pauseForEdit],
  );

  // Duplicate a node with context-aware connection replication for consumers
  const duplicateNode = useCallback(
    (nodeId: string) => {
      const sourceNode = nodes.find((n) => n.id === nodeId);
      if (!sourceNode) return;
      pauseForEdit();

      const newId = allocateNodeId(sourceNode.data.kind, nodes);
      const newPos = { x: sourceNode.position.x + 40, y: sourceNode.position.y + 40 };

      const newNode: SimCustomNode = {
        id: newId,
        type: sourceNode.type,
        position: newPos,
        data: {
          ...sourceNode.data,
          id: newId,
          title: `${sourceNode.data.title} #${nextNodeId}`,
        } as unknown as SimCustomNode['data'],
      };

      if (sourceNode.data.kind === 'consumer') {
        newNode.data.instanceId = newId;
        newNode.data.status = 'POLLING';
        newNode.data.assignedPartitions = [];
        newNode.data.isPaused = false;
        newNode.data.pausedPartitions = [];
        newNode.data.currentProcessingPartition = undefined;
        newNode.data.currentProcessingOffset = undefined;
      }
      if (sourceNode.data.kind === 'kafka-broker') {
        const brokerId =
          Math.max(
            0,
            ...nodes
              .filter((node) => node.data.kind === 'kafka-broker')
              .map((node) => Number(node.data.brokerId)),
          ) + 1;
        Object.assign(newNode.data, {
          brokerId,
          host: `broker-${brokerId}:9092`,
          isController: false,
          status: 'ACTIVE',
          topicPartitions: [],
        });
      }
      // Context-aware replication: if duplicating a consumer, replicate inbound wire from Kafka
      const newEdges: SimCustomEdge[] = [];
      if (sourceNode.data.kind === 'consumer') {
        for (const edge of edges.filter(
          (edge) => edge.source === nodeId || edge.target === nodeId,
        )) {
          newEdges.push({
            ...structuredClone(edge),
            id: `${edge.id}:${newId}`,
            source: edge.source === nodeId ? newId : edge.source,
            target: edge.target === nodeId ? newId : edge.target,
          });
        }
      }

      setNodes((nds) => [...nds, newNode]);
      if (newEdges.length > 0) {
        setEdges((eds) => [...eds, ...newEdges]);
      }
      setSelectedNodeId(newId);
    },
    [nodes, edges, pauseForEdit],
  );

  // Delete node and clean up connected edges
  const deleteNode = useCallback(
    (nodeId: string) => {
      pauseForEdit();
      setNodes((nds) => nds.filter((n) => n.id !== nodeId));
      setEdges((eds) => eds.filter((e) => e.source !== nodeId && e.target !== nodeId));
      if (selectedNodeId === nodeId) {
        setSelectedNodeId(null);
      }
    },
    [selectedNodeId, pauseForEdit],
  );

  // Update node data from Inspector
  const updateNodeData = useCallback(
    (nodeId: string, partial: Record<string, unknown>) => {
      pauseForEdit();
      // 2. Sync React Flow nodes
      setNodes((nds) =>
        nds.map((n) => {
          if (n.id === nodeId) {
            return {
              ...n,
              data: { ...n.data, ...partial },
            };
          }
          return n;
        }),
      );

      syncFromEngine();
    },
    [pauseForEdit, syncFromEngine],
  );

  // Simulation Playback Controls
  const play = useCallback(() => {
    if (!readyToExecute()) return;
    engine.clock.start();
    syncFromEngine();
  }, [engine, syncFromEngine, readyToExecute]);

  const pause = useCallback(() => {
    engine.clock.pause();
    syncFromEngine();
  }, [engine, syncFromEngine]);

  const step = useCallback(() => {
    if (!readyToExecute()) return;
    engine.clock.pause();
    engine.step();
    syncFromEngine();
  }, [engine, syncFromEngine, readyToExecute]);

  const reset = useCallback(() => {
    engine.reset();
    syncFromEngine();
  }, [engine, syncFromEngine]);

  const setSpeed = useCallback(
    (multiplier: number) => {
      engine.clock.setSpeed(multiplier);
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  // Dispatch message from Producer
  const dispatchMessage = useCallback(
    (producerId?: string, customKey?: string) => {
      if (!readyToExecute()) return;
      const pNode = producerId
        ? nodes.find((n) => n.id === producerId)
        : nodes.find((n) => n.data.kind === 'producer');

      if (!pNode) {
        setValidationMessage('Add a Producer to the canvas first.');
        return;
      }

      engine.dispatchProducerMessage(pNode.id, customKey);
      syncFromEngine();
    },
    [nodes, engine, syncFromEngine, readyToExecute],
  );

  const setTopicPartitionCount = useCallback(
    (count: number) => {
      pauseForEdit();
      const topic = engine.kafkaCluster.topics.get('purchases');
      if (
        !Number.isInteger(count) ||
        count < 1 ||
        count > 12 ||
        (topic && count < topic.partitions.size)
      ) {
        setValidationMessage(
          'Kafka partitions can only increase (up to 12 in this simulator). Restore a scenario to start over.',
        );
        return;
      }
      for (const group of engine.consumerGroups.values())
        if (group.topicName === 'purchases') engine.invalidateConsumerWork(group.groupId);
      engine.kafkaCluster.setPartitionCount('purchases', count);
      // Rebalance all consumer groups
      for (const group of engine.consumerGroups.values()) {
        if (group.topicName !== 'purchases') continue;
        group.rebalance(count);
        for (const member of group.members.values())
          for (const part of member.assignedPartitions) engine.resumePartitionBacklog(group, part);
      }
      // Sync to nodes
      setNodes((nds) =>
        nds.map((n) => {
          if (n.data.kind === 'kafka-cluster') {
            return {
              ...n,
              data: {
                ...n.data,
                partitionCount: count,
              },
            };
          }
          if (n.data.kind === 'consumer') {
            const groupId = (n.data.groupId as string) || 'loyalty-v1';
            const group = engine.consumerGroups.get(groupId);
            const member = group?.members.get(n.id);
            const partitions = member?.assignedPartitions || [];
            return {
              ...n,
              data: {
                ...n.data,
                assignedPartitions: [...partitions],
                status:
                  n.data.status === 'CRASHED' || n.data.isPaused
                    ? n.data.status
                    : partitions.length > 0
                      ? 'POLLING'
                      : 'IDLE',
              },
            };
          }
          return n;
        }),
      );
      syncFromEngine();
    },
    [engine, syncFromEngine, pauseForEdit],
  );

  const setAssignorStrategy = useCallback(
    (strategy: 'round-robin' | 'range') => {
      pauseForEdit();
      for (const group of engine.consumerGroups.values()) {
        engine.invalidateConsumerWork(group.groupId);
        group.setAssignorStrategy(strategy);
        const topic = engine.kafkaCluster.topics.get(group.topicName);
        group.rebalance(topic?.partitions.size ?? 0);
        for (const member of group.members.values())
          for (const part of member.assignedPartitions) engine.resumePartitionBacklog(group, part);
      }
      // Sync to nodes
      setNodes((nds) =>
        nds.map((n) => {
          if (n.data.kind === 'consumer') {
            const groupId = (n.data.groupId as string) || 'loyalty-v1';
            const group = engine.consumerGroups.get(groupId);
            const member = group?.members.get(n.id);
            const partitions = member?.assignedPartitions || [];
            return {
              ...n,
              data: {
                ...n.data,
                assignedPartitions: [...partitions],
                status:
                  n.data.status === 'CRASHED' || n.data.isPaused
                    ? n.data.status
                    : partitions.length > 0
                      ? 'POLLING'
                      : 'IDLE',
              },
            };
          }
          return n;
        }),
      );
      syncFromEngine();
    },
    [engine, syncFromEngine, pauseForEdit],
  );

  const setTopicReplicationFactor = useCallback(
    (rf: number) => {
      pauseForEdit();
      const topic = engine.kafkaCluster.topics.get('purchases');
      if (topic) {
        // Re-distribute replicas without replacing partition logs.
        engine.clock.pause();
        try {
          engine.kafkaCluster.reassignReplicas('purchases', rf);
        } catch (error) {
          setValidationMessage((error as Error).message);
          return;
        }
      }
      engine.notifyStateUpdated();
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  const setTopicMinIsr = useCallback(
    (minIsr: number) => {
      pauseForEdit();
      const topic = engine.kafkaCluster.topics.get('purchases');
      if (topic) {
        topic.minInsyncReplicas = minIsr;
      }
      engine.notifyStateUpdated();
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  // Load architectural blueprint onto canvas
  const loadBlueprint = useCallback(
    (blueprintId: string) => {
      const blueprint = SYSTEM_BLUEPRINTS.find((b) => b.id === blueprintId);
      if (!blueprint) return;

      // Deep clone nodes and edges to ensure fresh state
      const clonedNodes: SimCustomNode[] = JSON.parse(JSON.stringify(blueprint.nodes));
      const clonedEdges: SimCustomEdge[] = JSON.parse(JSON.stringify(blueprint.edges));

      setNodes(clonedNodes);
      setEdges(clonedEdges);
      setSelectedNodeId(null);
      setActiveBlueprintId(blueprintId);

      engine.reset(false);
      engine.setTopology(clonedNodes, clonedEdges);
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  // Load default Starter Graph (Full Enterprise Production System)
  const loadDefaultGraph = useCallback(() => {
    loadBlueprint('full-enterprise-production');
  }, [loadBlueprint]);

  const selectedNode = nodes.find((n) => n.id === selectedNodeId) || null;

  const crashBroker = useCallback(
    (brokerId: number) => {
      engine.crashBroker(brokerId);
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  const reviveBroker = useCallback(
    (brokerId: number) => {
      engine.reviveBroker(brokerId);
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  const crashConsumer = useCallback(
    (consumerId: string) => {
      engine.crashConsumer(consumerId);
      setNodes((nds) =>
        nds.map((n) => {
          const engNode = engine.nodes.get(n.id);
          if (engNode && n.data.kind === 'consumer') {
            return {
              ...n,
              data: {
                ...n.data,
                assignedPartitions: engNode.data.assignedPartitions,
                status: engNode.data.status,
              },
            };
          }
          return n;
        }),
      );
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  const reviveConsumer = useCallback(
    (consumerId: string) => {
      engine.reviveConsumer(consumerId);
      setNodes((nds) =>
        nds.map((n) => {
          const engNode = engine.nodes.get(n.id);
          if (engNode && n.data.kind === 'consumer') {
            return {
              ...n,
              data: {
                ...n.data,
                assignedPartitions: engNode.data.assignedPartitions,
                status: engNode.data.status,
                isPaused: engNode.data.isPaused,
              },
            };
          }
          return n;
        }),
      );
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  const togglePauseConsumer = useCallback(
    (consumerId: string) => {
      engine.togglePauseConsumer(consumerId);
      const engineNode = engine.nodes.get(consumerId);
      if (engineNode) {
        setNodes((nds) =>
          nds.map((n) => {
            if (n.id === consumerId) {
              return {
                ...n,
                data: {
                  ...n.data,
                  isPaused: engineNode.data.isPaused,
                  status: engineNode.data.status,
                },
              };
            }
            return n;
          }),
        );
      }
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  const togglePauseConsumerPartition = useCallback(
    (consumerId: string, partitionId: number) => {
      engine.togglePauseConsumerPartition(consumerId, partitionId);
      const engineNode = engine.nodes.get(consumerId);
      if (engineNode) {
        setNodes((nds) =>
          nds.map((n) => {
            if (n.id === consumerId) {
              return {
                ...n,
                data: {
                  ...n.data,
                  pausedPartitions: engineNode.data.pausedPartitions,
                },
              };
            }
            return n;
          }),
        );
      }
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  const createClusterTopic = useCallback(
    (
      clusterNodeId: string,
      topicName: string,
      partitionCount: number = 4,
      replicationFactor: number = 3,
    ) => {
      pauseForEdit();
      if (!topicName.trim() || engine.kafkaCluster.topics.has(topicName)) {
        setValidationMessage(
          'Choose a new, non-empty topic name. Existing topic records are preserved.',
        );
        return;
      }
      if (
        !Number.isInteger(partitionCount) ||
        partitionCount < 1 ||
        partitionCount > 12 ||
        !Number.isInteger(replicationFactor) ||
        replicationFactor < 1 ||
        replicationFactor > engine.kafkaCluster.brokers.size
      ) {
        setValidationMessage(
          'Use 1–12 partitions and a replication factor within the broker count.',
        );
        return;
      }
      engine.kafkaCluster.createTopic(
        topicName,
        partitionCount,
        replicationFactor,
        Math.min(2, replicationFactor),
      );
      const allTopics = Array.from(engine.kafkaCluster.topics.keys());
      updateNodeData(clusterNodeId, {
        topicsList: allTopics,
        activeTopic: topicName,
        partitionCount,
        replicationFactor,
        minInsyncReplicas: Math.min(2, replicationFactor),
      });
      syncFromEngine();
    },
    [engine, updateNodeData, syncFromEngine, pauseForEdit],
  );

  const setActiveClusterTopic = useCallback(
    (clusterNodeId: string, topicName: string) => {
      const topic = engine.kafkaCluster.topics.get(topicName);
      if (!topic) return;
      updateNodeData(clusterNodeId, {
        activeTopic: topicName,
        partitionCount: topic.partitions.size,
        replicationFactor: topic.replicationFactor,
        minInsyncReplicas: topic.minInsyncReplicas,
      });
      syncFromEngine();
    },
    [engine, updateNodeData, syncFromEngine],
  );

  // Global window listener for card action triggers
  useEffect(() => {
    const handleToggle = (e: Event) => {
      const customEvent = e as CustomEvent<{ id: string }>;
      if (customEvent.detail?.id) {
        togglePauseConsumer(customEvent.detail.id);
      }
    };
    window.addEventListener('sim:toggle-pause-consumer', handleToggle);
    return () => window.removeEventListener('sim:toggle-pause-consumer', handleToggle);
  }, [togglePauseConsumer]);

  useEffect(() => {
    const handleToggleBroker = (e: Event) => {
      const customEvent = e as CustomEvent<{ brokerId: number }>;
      const bId = customEvent.detail?.brokerId;
      if (bId !== undefined) {
        const broker = engine.kafkaCluster.brokers.get(bId);
        if (broker?.status === 'ONLINE') {
          crashBroker(bId);
        } else {
          reviveBroker(bId);
        }
      }
    };
    window.addEventListener('sim:toggle-broker-crash', handleToggleBroker);
    return () => window.removeEventListener('sim:toggle-broker-crash', handleToggleBroker);
  }, [engine, crashBroker, reviveBroker]);

  const dispatchPoisonPill = useCallback(
    (options?: { targetFailedConsumerIds?: string[]; targetFailedGroupIds?: string[] }) => {
      if (!readyToExecute()) return;
      const pNode = nodes.find((n) => n.data.kind === 'producer');
      if (!pNode) {
        setValidationMessage('Add a Producer to the canvas first.');
        return;
      }
      engine.dispatchProducerMessage(pNode.id, 'POISON_PILL', {
        purchaseId: 'POISON-ERR-999',
        itemDescription: '☠️ Poison-Pill Record (Deserialization/Validation Hazard)',
        targetFailedConsumerIds: options?.targetFailedConsumerIds,
        targetFailedGroupIds: options?.targetFailedGroupIds,
      });
      syncFromEngine();
    },
    [nodes, engine, syncFromEngine, readyToExecute],
  );

  const setProducerAcks = useCallback(
    (acks: SimProducerAcks) => {
      engine.mechanicsConfig.producerAcks = acks;
      setMechanicsConfig({ ...engine.mechanicsConfig });
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  const setPartitioningStrategy = useCallback(
    (strategy: 'murmur2-key' | 'round-robin') => {
      engine.mechanicsConfig.partitioningStrategy = strategy;
      engine.mechanicsConfig.keyOrderingMode =
        strategy === 'round-robin' ? 'ROUND_ROBIN' : 'MURMUR2_KEYED';
      setMechanicsConfig({ ...engine.mechanicsConfig });
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  const toggleConsumerIdempotency = useCallback(() => {
    engine.mechanicsConfig.consumerIdempotency = !engine.mechanicsConfig.consumerIdempotency;
    setMechanicsConfig({ ...engine.mechanicsConfig });
    syncFromEngine();
  }, [engine, syncFromEngine]);

  const setCommitMode = useCallback(
    (mode: SimCommitMode) => {
      engine.setMechanicsConfig({ commitMode: mode });
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  const setAutoOffsetReset = useCallback(
    (policy: SimOffsetResetPolicy) => {
      engine.setMechanicsConfig({ autoOffsetReset: policy });
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  const seekPartitionOffset = useCallback(
    (groupId: string, partitionId: number, nextOffset: number) => {
      const ok = engine.seekPartitionOffset(groupId, partitionId, nextOffset);
      syncFromEngine();
      return ok;
    },
    [engine, syncFromEngine],
  );

  const resetGroupOffsets = useCallback(
    (groupId: string, policy: SimOffsetResetPolicy) => {
      engine.resetGroupOffsets(groupId, policy);
      syncFromEngine();
    },
    [engine, syncFromEngine],
  );

  const dispatchDuplicateMessage = useCallback(() => {
    if (!readyToExecute()) return;
    const pNode = nodes.find((n) => n.data.kind === 'producer');
    if (!pNode) {
      setValidationMessage('Add a Producer to the canvas first.');
      return;
    }
    if (engine.clock.status === 'STOPPED') {
      engine.clock.start();
    }
    const customKey = (pNode.data.messageKey as string) || 'U001';
    engine.dispatchDuplicateMessage(customKey);
    syncFromEngine();
  }, [nodes, engine, syncFromEngine, readyToExecute]);

  const dispatchOrderedBurst = useCallback(
    (count = 5, customerId = 'U001') => {
      if (!readyToExecute()) return;
      const pNode = nodes.find((n) => n.data.kind === 'producer');
      if (!pNode) {
        setValidationMessage('Add a Producer to the canvas first.');
        return;
      }
      engine.dispatchOrderedBurst(customerId, count);
      syncFromEngine();
    },
    [nodes, engine, syncFromEngine, readyToExecute],
  );

  const scaleGroup = useCallback(
    (groupId: string, count: number) => {
      try {
        const next = scaleConsumers(nodes, edges, groupId, count, () =>
          allocateNodeId('consumer', nodes),
        );
        pauseForEdit();
        setNodes(next.nodes);
        setEdges(next.edges);
        setValidationMessage('');
      } catch (error) {
        setValidationMessage(error instanceof Error ? error.message : String(error));
      }
    },
    [nodes, edges, pauseForEdit],
  );

  return {
    topologyIssues,
    validationMessage,
    scaleGroup,
    engine,
    nodes,
    edges,
    selectedNode,
    selectedNodeId,
    eventLog,
    virtualTimeMs,
    playbackStatus,
    speedMultiplier,
    inFlightTokens,
    mechanicsConfig,
    onNodesChange,
    onEdgesChange,
    onConnect,
    addNodeFromPalette,
    duplicateNode,
    deleteNode,
    selectNode: setSelectedNodeId,
    updateNodeData,
    play,
    pause,
    step,
    reset,
    setSpeed,
    dispatchMessage,
    dispatchPoisonPill,
    dispatchDuplicateMessage,
    dispatchOrderedBurst,
    setProducerAcks,
    setPartitioningStrategy,
    toggleConsumerIdempotency,
    setCommitMode,
    setAutoOffsetReset,
    seekPartitionOffset,
    resetGroupOffsets,
    loadDefaultGraph,
    loadBlueprint,
    blueprints: SYSTEM_BLUEPRINTS,
    activeBlueprintId,
    setTopicPartitionCount,
    setTopicReplicationFactor,
    setTopicMinIsr,
    setAssignorStrategy,
    crashBroker,
    reviveBroker,
    crashConsumer,
    reviveConsumer,
    togglePauseConsumer,
    togglePauseConsumerPartition,
    createClusterTopic,
    setActiveClusterTopic,
  };
}

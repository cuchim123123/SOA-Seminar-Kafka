import React, { useState, useEffect, useMemo, useCallback } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  type NodeTypes,
  type EdgeTypes,
  type ReactFlowInstance,
  type NodeChange,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import { useSimulationStore } from '../store/useSimulationStore';
import { SimProducerNode } from './nodes/SimProducerNode';
import { SimKafkaNode } from './nodes/SimKafkaNode';
import { SimConsumerNode } from './nodes/SimConsumerNode';
import { SimDatabaseNode } from './nodes/SimDatabaseNode';
import { SimDlqNode } from './nodes/SimDlqNode';
import { SimOutboxTableNode } from './nodes/SimOutboxTableNode';
import { SimCdcRelayNode } from './nodes/SimCdcRelayNode';
import { SimFlowEdge } from './canvas/SimFlowEdge';
import { SimConsumerGroupContainerNode } from './nodes/SimConsumerGroupContainerNode';
import {
  buildGroupContainerNodes,
  GROUP_BOX_NODE_TYPE,
  GROUP_BOX_PREFIX,
} from './canvas/groupContainerNodes';
import { ComponentPalette } from './palette/ComponentPalette';
import { SimNodeInspector } from './inspector/SimNodeInspector';
import { SimPlaybackBar } from './controls/SimPlaybackBar';
import { KafkaMechanicsToolbar } from './controls/KafkaMechanicsToolbar';
import { CurrentOffsetMenu } from './controls/CurrentOffsetMenu';
import { SimEventTimeline } from './timeline/SimEventTimeline';
import { BlueprintSelectorModal } from './blueprints/BlueprintSelectorModal';
import type { ComponentKind, SimCustomNode } from '../domain/types';

export const SimulationLab: React.FC = () => {
  const {
    nodes,
    edges,
    selectedNode,
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
    selectNode,
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
    toggleDlqRouting,
    setCommitMode,
    setAutoOffsetReset,
    seekPartitionOffset,
    resetGroupOffsets,
    loadDefaultGraph,
    loadBlueprint,
    blueprints,
    activeBlueprintId,
    setTopicPartitionCount,
    setAssignorStrategy,
    crashBroker,
    reviveBroker,
    crashConsumer,
    reviveConsumer,
    togglePauseConsumer,
    togglePauseConsumerPartition,
    createClusterTopic,
    setActiveClusterTopic,
    engine,
  } = useSimulationStore();

  const [paletteOpen, setPaletteOpen] = useState(true);
  const [blueprintModalOpen, setBlueprintModalOpen] = useState(false);
  const [reactFlowInstance, setReactFlowInstance] = useState<ReactFlowInstance | null>(null);

  // Load blueprint and smoothly fit view onto canvas
  const handleSelectBlueprint = useCallback(
    (blueprintId: string) => {
      loadBlueprint(blueprintId);
      setTimeout(() => {
        reactFlowInstance?.fitView({ padding: 0.18, duration: 400 });
      }, 80);
    },
    [loadBlueprint, reactFlowInstance],
  );

  // Load starter graph on initial mount
  useEffect(() => {
    loadDefaultGraph();
  }, [loadDefaultGraph]);

  // Node & Edge types registration
  const nodeTypes: NodeTypes = useMemo(
    () => ({
      producerNode: SimProducerNode,
      kafkaClusterNode: SimKafkaNode,
      consumerNode: SimConsumerNode,
      databaseNode: SimDatabaseNode,
      deadLetterQueueNode: SimDlqNode,
      outboxTableNode: SimOutboxTableNode,
      cdcRelayNode: SimCdcRelayNode,
      [GROUP_BOX_NODE_TYPE]: SimConsumerGroupContainerNode,
    }),
    [],
  );

  const edgeTypes: EdgeTypes = useMemo(
    () => ({
      simFlowEdge: SimFlowEdge,
    }),
    [],
  );

  // Attach in-flight tokens to edges for visual particle animation
  const animatedEdges = useMemo(() => {
    return edges.map((edge) => {
      const tokensForEdge = inFlightTokens.filter((t) => t.edgeId === edge.id);
      return {
        ...edge,
        data: {
          ...edge.data,
          inFlightTokens: tokensForEdge,
        },
      };
    });
  }, [edges, inFlightTokens]);

  // View-only wrapper rectangles grouping consumers of the same consumer group
  const canvasNodes = [...buildGroupContainerNodes(nodes, engine), ...nodes];

  // Dragging a group box moves every consumer of that group by the same delta
  const handleNodesChange = useCallback(
    (changes: NodeChange<SimCustomNode>[]) => {
      const translated: NodeChange<SimCustomNode>[] = [];
      for (const change of changes) {
        if (!('id' in change) || !change.id.startsWith(GROUP_BOX_PREFIX)) {
          translated.push(change);
          continue;
        }
        if (change.type !== 'position' || !change.position) continue;
        const box = canvasNodes.find((n) => n.id === change.id);
        if (!box) continue;
        const dx = change.position.x - box.position.x;
        const dy = change.position.y - box.position.y;
        const groupId = change.id.slice(GROUP_BOX_PREFIX.length);
        for (const member of nodes) {
          if (member.data.kind !== 'consumer' || member.data.groupId !== groupId) continue;
          translated.push({
            type: 'position',
            id: member.id,
            position: { x: member.position.x + dx, y: member.position.y + dy },
            dragging: change.dragging,
          });
        }
      }
      onNodesChange(translated);
    },
    [canvasNodes, nodes, onNodesChange],
  );

  // Drag and Drop from Palette onto Canvas
  const onDragStart = useCallback((event: React.DragEvent, kind: string) => {
    event.dataTransfer.setData('application/reactflow-kind', kind);
    event.dataTransfer.effectAllowed = 'move';
  }, []);

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();
      const kind = event.dataTransfer.getData('application/reactflow-kind') as ComponentKind;
      if (!kind || !reactFlowInstance) return;

      const position = reactFlowInstance.screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });

      addNodeFromPalette(kind, position);
    },
    [reactFlowInstance, addNodeFromPalette],
  );

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        height: 'calc(100vh - 160px)',
        minHeight: '740px',
        background: '#ffffff',
        borderRadius: '12px',
        border: '1px solid #cbd5e1',
        overflow: 'hidden',
        boxShadow: 'var(--shadow-card)',
        position: 'relative',
      }}
    >
      {/* ── TOP PLAYBACK & TIME CONTROLS ── */}
      <SimPlaybackBar
        status={playbackStatus}
        virtualTimeMs={virtualTimeMs}
        speedMultiplier={speedMultiplier}
        onPlay={play}
        onPause={pause}
        onStep={step}
        onReset={reset}
        onSetSpeed={setSpeed}
        onDispatchMessage={() => dispatchMessage()}
        onDispatchPoisonPill={dispatchPoisonPill}
        onLoadDefaultGraph={loadDefaultGraph}
        blueprints={blueprints}
        activeBlueprintId={activeBlueprintId}
        onOpenBlueprintModal={() => setBlueprintModalOpen(true)}
        onSelectBlueprint={handleSelectBlueprint}
      />

      {/* ── KAFKA STREAMING MECHANICS CONTROLS ── */}
      <KafkaMechanicsToolbar
        config={mechanicsConfig}
        onSetAcks={setProducerAcks}
        onSetPartitioning={setPartitioningStrategy}
        onToggleIdempotency={toggleConsumerIdempotency}
        onToggleDlq={toggleDlqRouting}
        onDispatchDuplicate={dispatchDuplicateMessage}
        onDispatchOrderedBurst={dispatchOrderedBurst}
        onDispatchPoisonPill={dispatchPoisonPill}
        offsetMenu={
          <CurrentOffsetMenu
            engine={engine}
            config={mechanicsConfig}
            onSetCommitMode={setCommitMode}
            onSetAutoOffsetReset={setAutoOffsetReset}
            onSeek={seekPartitionOffset}
            onResetGroup={resetGroupOffsets}
          />
        }
      />

      {/* ── CENTER WORKSPACE: PALETTE + CANVAS + INSPECTOR ── */}
      <div style={{ display: 'flex', flex: 1, position: 'relative', overflow: 'hidden' }}>
        {/* Left Palette */}
        <ComponentPalette
          isOpen={paletteOpen}
          onToggle={() => setPaletteOpen(!paletteOpen)}
          onDragStart={onDragStart}
        />

        {/* Main React Flow Canvas */}
        <div
          style={{ flex: 1, height: '100%', position: 'relative' }}
          onDragOver={onDragOver}
          onDrop={onDrop}
        >
          <ReactFlow
            nodes={canvasNodes}
            edges={animatedEdges}
            onNodesChange={handleNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            onNodeClick={(_, node) => selectNode(node.id)}
            onPaneClick={() => selectNode(null)}
            onInit={setReactFlowInstance}
            fitView
            minZoom={0.2}
            maxZoom={2.0}
            defaultEdgeOptions={{ type: 'simFlowEdge' }}
          >
            <Background gap={16} size={1} color="#e2e8f0" />
            <Controls />
            <MiniMap
              nodeColor={(n) => {
                if (n.data?.kind === 'producer') return '#7c3aed';
                if (n.data?.kind === 'kafka-cluster') return '#0284c7';
                if (n.data?.kind === 'consumer') return '#059669';
                if (n.data?.kind === 'database') return '#10b981';
                if (n.data?.kind === 'dead-letter-queue') return '#ef4444';
                if (n.data?.kind === 'outbox-table') return '#ea580c';
                if (n.data?.kind === 'cdc-relay') return '#0891b2';
                return '#94a3b8';
              }}
              style={{ bottom: 12, right: 12 }}
            />
          </ReactFlow>

          {/* Context-Aware Node Inspector Drawer */}
          <SimNodeInspector
            selectedNode={selectedNode}
            onClose={() => selectNode(null)}
            onUpdateData={updateNodeData}
            onDuplicateNode={duplicateNode}
            onDeleteNode={deleteNode}
            onSetPartitionCount={setTopicPartitionCount}
            onSetAssignorStrategy={setAssignorStrategy}
            onCrashBroker={crashBroker}
            onReviveBroker={reviveBroker}
            onCrashConsumer={crashConsumer}
            onReviveConsumer={reviveConsumer}
            onTogglePauseConsumer={togglePauseConsumer}
            onTogglePauseConsumerPartition={togglePauseConsumerPartition}
            onCreateClusterTopic={createClusterTopic}
            onSetActiveClusterTopic={setActiveClusterTopic}
            brokers={Array.from(engine.kafkaCluster.brokers.values())}
            assignorStrategy={engine.consumerGroups.get('loyalty-v1')?.assignorStrategy || 'round-robin'}
            partitionLogs={engine.kafkaCluster.getAllPartitions(
              (selectedNode?.data.activeTopic as string) || 'purchases',
            )}
            committedOffsets={
              engine.consumerGroups.get(
                (selectedNode?.data.groupId as string) || 'loyalty-v1',
              )?.committedNextOffsets || new Map()
            }
            consumerGroups={Array.from(engine.consumerGroups.values())}
            allTopics={Array.from(engine.kafkaCluster.topics.values())}
          />
        </div>
      </div>

      {/* ── BOTTOM DISCRETE-EVENT TIMELINE ── */}
      <SimEventTimeline events={eventLog} />

      {/* ── ARCHITECTURAL BLUEPRINT SELECTOR MODAL ── */}
      <BlueprintSelectorModal
        isOpen={blueprintModalOpen}
        onClose={() => setBlueprintModalOpen(false)}
        blueprints={blueprints}
        activeBlueprintId={activeBlueprintId}
        onSelectBlueprint={handleSelectBlueprint}
      />
    </div>
  );
};

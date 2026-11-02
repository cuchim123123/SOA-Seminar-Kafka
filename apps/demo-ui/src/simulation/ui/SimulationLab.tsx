import React, { useState, useMemo, useCallback } from 'react';
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
import './simulation.css';

import { useSimulationStore } from '../store/SimulationSession';
import { SimProducerNode } from './nodes/SimProducerNode';
import { SimKafkaNode } from './nodes/SimKafkaNode';
import { SimKafkaBrokerNode } from './nodes/SimKafkaBrokerNode';
import { SimConsumerNode } from './nodes/SimConsumerNode';
import { SimDatabaseNode } from './nodes/SimDatabaseNode';
import { SimDlqNode } from './nodes/SimDlqNode';
import { SimOutboxTableNode } from './nodes/SimOutboxTableNode';
import { SimCdcRelayNode } from './nodes/SimCdcRelayNode';
import { SimFlowEdge } from './canvas/SimFlowEdge';
import { SimulationClockContext } from './canvas/SimulationClockContext';
import { SimConsumerGroupContainerNode } from './nodes/SimConsumerGroupContainerNode';
import {
  buildGroupContainerNodes,
  GROUP_BOX_NODE_TYPE,
  GROUP_BOX_PREFIX,
} from './canvas/groupContainerNodes';
import { ComponentPalette } from './palette/ComponentPalette';
import { SimNodeInspector } from './inspector/SimNodeInspector';
import { SimulationControls } from './controls/SimulationControls';
import { SimEventTimeline } from './timeline/SimEventTimeline';
import { PoisonPillModal } from './controls/PoisonPillModal';
import type { ComponentKind, SimCustomNode, SimCustomEdge } from '../domain/types';

export const SimulationLab: React.FC = () => {
  const store = useSimulationStore();
  const {
    nodes,
    edges,
    selectedNode,
    eventLog,
    onNodesChange,
    onEdgesChange,
    onConnect,
    addNodeFromPalette,
    duplicateNode,
    deleteNode,
    selectNode,
    updateNodeData,
    dispatchPoisonPill,
    loadBlueprint,
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
    engine,
    topologyIssues,
    validationMessage,
    scaleGroup,
    frameClock,
  } = store;

  const [paletteOpen, setPaletteOpen] = useState(false);
  const [poisonProducerId, setPoisonProducerId] = useState<string>();
  const [poisonPillModalOpen, setPoisonPillModalOpen] = useState(false);
  const [reactFlowInstance, setReactFlowInstance] = useState<ReactFlowInstance<
    SimCustomNode,
    SimCustomEdge
  > | null>(null);

  const inspectionTopic = String(
    selectedNode?.data.subscribedTopic ?? selectedNode?.data.activeTopic ?? 'purchases',
  );
  const consumerNodes = useMemo(() => nodes.filter((n) => n.data.kind === 'consumer'), [nodes]);

  // Load blueprint and smoothly fit view onto canvas
  const handleSelectBlueprint = useCallback(
    (blueprintId: string) => {
      loadBlueprint(blueprintId);
      setTimeout(() => {
        reactFlowInstance?.fitView({ padding: 0.18, duration: 0 });
      }, 80);
    },
    [loadBlueprint, reactFlowInstance],
  );

  // Node & Edge types registration
  const nodeTypes: NodeTypes = useMemo(
    () => ({
      producerNode: SimProducerNode,
      kafkaBrokerNode: SimKafkaBrokerNode,
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

  // View-only wrapper rectangles grouping consumers of the same consumer group
  const canvasNodes = useMemo(
    () => [...buildGroupContainerNodes(nodes, engine), ...nodes],
    [nodes, engine],
  );

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
      className="simulation-workspace"
      style={{
        display: 'flex',
        flexDirection: 'column',
        height: 'max(820px, calc(100dvh - 72px))',
        minHeight: '620px',
        background: '#ffffff',
        borderRadius: '12px',
        border: '1px solid #cbd5e1',
        overflow: 'auto',
        boxShadow: 'var(--shadow-card)',
        position: 'relative',
      }}
    >
      <div className="sim-control-panel">
        <SimulationControls
          store={store}
          builderOpen={paletteOpen}
          onToggleBuilder={() => setPaletteOpen((open) => !open)}
          onSelectScenario={handleSelectBlueprint}
          onPoison={(producerId) => {
            setPoisonProducerId(producerId);
            setPoisonPillModalOpen(true);
          }}
        />
      </div>
      <div className="sim-session-notice" role="status">
        {store.sessionNotice} {store.storageMessage}
        {store.storageMessage && !store.storageBlocked && (
          <button onClick={store.retryAutosave}>Retry autosave</button>
        )}
        {store.storageBlocked && (
          <>
            <button onClick={store.downloadRecoveryDocument} disabled={!store.hasRecoveryDocument}>
              Download saved document
            </button>
            <button
              onClick={() => {
                if (
                  window.confirm(
                    'Save this diagram instead? The original saved document will be backed up first.',
                  )
                )
                  store.replaceSavedDiagram();
              }}
            >
              Replace saved diagram
            </button>
          </>
        )}
      </div>
      {(validationMessage || topologyIssues.length > 0) && (
        <section
          aria-label="Simulation setup feedback"
          style={{
            padding: '8px 16px',
            background: '#fffbeb',
            color: '#92400e',
            borderBottom: '1px solid #fde68a',
            maxHeight: 130,
            overflow: 'auto',
          }}
        >
          {validationMessage && <p role="status">{validationMessage}</p>}
          {topologyIssues.length > 0 && (
            <>
              <strong>Draft needs attention before Run or Step:</strong>
              <ul>
                {topologyIssues.map((issue) => (
                  <li key={issue}>{issue}</li>
                ))}
              </ul>
            </>
          )}
        </section>
      )}
      {/* ── CENTER WORKSPACE: PALETTE + CANVAS + INSPECTOR ── */}
      <div
        style={{
          display: 'flex',
          flex: 1,
          minHeight: 180,
          position: 'relative',
          overflow: 'hidden',
        }}
      >
        {/* Left Palette */}
        <ComponentPalette
          isOpen={paletteOpen}
          onToggle={() => setPaletteOpen(!paletteOpen)}
          onDragStart={onDragStart}
          onAdd={(kind) => {
            const point = reactFlowInstance?.screenToFlowPosition({
              x: window.innerWidth / 2,
              y: window.innerHeight / 2,
            }) ?? { x: 100, y: 100 };
            addNodeFromPalette(kind, point);
          }}
          onAddProjection={store.addIndependentProjection}
        />

        {/* Main React Flow Canvas */}
        <div
          style={{ flex: 1, height: '100%', position: 'relative' }}
          onDragOver={onDragOver}
          onDrop={onDrop}
        >
          <SimulationClockContext.Provider value={frameClock}>
            <ReactFlow<SimCustomNode, SimCustomEdge>
              nodes={canvasNodes}
              edges={edges}
              onNodesChange={handleNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              nodeTypes={nodeTypes}
              edgeTypes={edgeTypes}
              onNodeClick={(_, node) =>
                selectNode(
                  node.id.startsWith(GROUP_BOX_PREFIX)
                    ? (nodes.find(
                        (item) =>
                          item.data.kind === 'consumer' && item.data.groupId === node.data.groupId,
                      )?.id ?? null)
                    : node.id,
                )
              }
              onPaneClick={() => selectNode(null)}
              onInit={setReactFlowInstance}
              defaultViewport={store.viewport ?? undefined}
              fitView={!store.viewport}
              onMoveEnd={(_, next) => store.setViewport(next)}
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
          </SimulationClockContext.Provider>

          {/* Context-Aware Node Inspector Drawer */}
          <SimNodeInspector
            selectedNode={selectedNode}
            onClose={() => selectNode(null)}
            onUpdateData={updateNodeData}
            onDuplicateNode={duplicateNode}
            onDeleteNode={deleteNode}
            onSetPartitionCount={(count) => setTopicPartitionCount(count, inspectionTopic)}
            onSetReplicationFactor={(rf) => setTopicReplicationFactor(rf, inspectionTopic)}
            onSetMinIsr={(minIsr) => setTopicMinIsr(minIsr, inspectionTopic)}
            onSetAssignorStrategy={(strategy) =>
              setAssignorStrategy(strategy, String(selectedNode?.data.groupId))
            }
            onCrashBroker={crashBroker}
            onReviveBroker={reviveBroker}
            onCrashConsumer={crashConsumer}
            onReviveConsumer={reviveConsumer}
            onTogglePauseConsumer={togglePauseConsumer}
            onTogglePauseConsumerPartition={togglePauseConsumerPartition}
            onScaleGroup={scaleGroup}
            consumerReplicaCount={
              engine.consumerGroups.get(String(selectedNode?.data.groupId))?.members.size ?? 0
            }
            onCreateClusterTopic={createClusterTopic}
            onSetActiveClusterTopic={setActiveClusterTopic}
            brokers={Array.from(engine.kafkaCluster.brokers.values())}
            minInsyncReplicas={
              engine.kafkaCluster.topics.get(inspectionTopic)?.minInsyncReplicas ?? 2
            }
            replicationFactor={
              engine.kafkaCluster.topics.get(inspectionTopic)?.replicationFactor ?? 3
            }
            assignorStrategy={
              engine.consumerGroups.get(String(selectedNode?.data.groupId))?.assignorStrategy ||
              'round-robin'
            }
            partitionLogs={engine.kafkaCluster.getAllPartitions(
              String(
                selectedNode?.data.subscribedTopic ?? selectedNode?.data.activeTopic ?? 'purchases',
              ),
            )}
            committedOffsets={
              engine.consumerGroups.get((selectedNode?.data.groupId as string) || 'loyalty-v1')
                ?.committedNextOffsets || new Map()
            }
            consumerGroups={Array.from(engine.consumerGroups.values())}
            allTopics={Array.from(engine.kafkaCluster.topics.values())}
          />
        </div>
      </div>

      {/* ── BOTTOM DISCRETE-EVENT TIMELINE ── */}
      <SimEventTimeline events={eventLog} total={engine.totalEventsRecorded} />

      {/* ── TARGETED POISON PILL INJECTION MODAL ── */}
      <PoisonPillModal
        isOpen={poisonPillModalOpen}
        onClose={() => setPoisonPillModalOpen(false)}
        consumerNodes={consumerNodes}
        onInject={(options) => dispatchPoisonPill({ ...options, producerId: poisonProducerId })}
      />
    </div>
  );
};

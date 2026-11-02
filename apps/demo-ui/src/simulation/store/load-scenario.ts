import type { SystemBlueprint } from '../blueprints/types';
import type { SimEngine } from '../engine/sim-engine';
import { applyDiagram } from './diagram-state';

export function loadScenario(engine: SimEngine, blueprint: SystemBlueprint) {
  engine.nodes.clear();
  engine.edges.clear();
  engine.reset(false);
  const nodes = structuredClone(blueprint.nodes),
    edges = structuredClone(blueprint.edges);
  const brokers = nodes.filter((node) => node.data.kind === 'kafka-broker').length;
  engine.kafkaCluster.initializeBrokers(brokers);
  engine.kafkaCluster.topics.clear();
  const config = blueprint.topicConfiguration;
  engine.kafkaCluster.createTopic(
    'purchases',
    config.partitionCount,
    config.replicationFactor,
    config.minInsyncReplicas,
  );
  engine.setMechanicsConfig({ producerAcks: config.producerAcks });
  applyDiagram(engine, nodes, edges);
  engine.clock.pause();
  return { nodes, edges };
}

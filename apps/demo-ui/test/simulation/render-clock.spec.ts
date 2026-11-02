import { expect, it } from 'vitest';
import { SimEngine } from '../../src/simulation/engine/sim-engine';
import { SimClock } from '../../src/simulation/engine/sim-clock';
import { createSimEvent } from '../../src/simulation/domain/events';
import { minimalSystemBlueprint } from '../../src/simulation/blueprints/catalog/minimalSystemBlueprint';
import {
  applyDiagram,
  captureNodeData,
  configurationData,
} from '../../src/simulation/store/diagram-state';
import { tokenProgress, VirtualClockStore } from '../../src/simulation/store/virtual-clock-store';

function fixture() {
  const engine = new SimEngine();
  const nodes = structuredClone(minimalSystemBlueprint.nodes);
  const edges = structuredClone(minimalSystemBlueprint.edges);
  applyDiagram(engine, nodes, edges);
  engine.setMechanicsConfig({ autoOffsetReset: 'earliest' });
  return { engine, nodes, edges };
}

it('keeps diagram, engine runtime and captured view data independent', () => {
  const { engine, nodes, edges } = fixture();
  const db = nodes.find((node) => node.data.kind === 'database')!;
  const initial = captureNodeData(engine, new Map());
  engine.nodes.get(db.id)!.data.committedRecordCount = 3;
  expect(db.data.committedRecordCount).toBe(0);
  expect(initial.get(db.id)?.committedRecordCount).toBe(0);
  const updated = captureNodeData(engine, initial);
  expect(updated.get(db.id)?.committedRecordCount).toBe(3);
  db.position.x += 30;
  applyDiagram(engine, nodes, edges);
  expect(engine.nodes.get(db.id)?.data.committedRecordCount).toBe(3);
  expect(engine.nodes.get(db.id)?.position.x).toBe(db.position.x);
  expect(configurationData(engine.nodes.get(db.id)!.data)).not.toHaveProperty(
    'committedRecordCount',
  );
});

it('reuses all unchanged node snapshots during 600 idle frame captures', () => {
  const { engine } = fixture();
  const initial = captureNodeData(engine, new Map());
  engine.clock.start();
  const version = engine.stateVersion;
  for (let frame = 0; frame < 600; frame++) {
    engine.tick(16);
    expect(captureNodeData(engine, initial)).toBe(initial);
  }
  expect(engine.stateVersion).toBe(version);
  expect(engine.clock.virtualTimeMs).toBe(9600);
});

it('dispatches without implicit playback, including an empty Step', () => {
  const { engine } = fixture();
  expect(engine.step()).toBeNull();
  expect(engine.clock.status).toBe('PAUSED');
  engine.dispatchProducerMessage('node-producer-1', 'U001');
  expect(engine.clock.status).toBe('PAUSED');
  engine.tick(10000);
  expect(engine.clock.virtualTimeMs).toBe(0);
  engine.clock.start();
  engine.step();
  expect(engine.clock.status).toBe('PAUSED');
  expect(engine.clock.virtualTimeMs).toBe(400);
});

it('uses identical scheduled event boundaries for continuous playback and discrete steps', () => {
  const continuous = fixture().engine,
    stepped = fixture().engine;
  for (const engine of [continuous, stepped]) engine.dispatchOrderedBurst('U001', 15);
  continuous.clock.start();
  continuous.tick(10000);
  while (stepped.queue.size()) stepped.step();
  expect(continuous.queue.size()).toBe(0);
  const summary = (engine: SimEngine) =>
    engine.eventLog.map((event) => [event.type, event.scheduledTime]);
  expect(summary(continuous)).toEqual(summary(stepped));
  expect(continuous.processedMessageCount).toBe(15);
  expect(stepped.processedMessageCount).toBe(15);
});

it('bounds work per tick without advancing past unexecuted logical events', () => {
  const engine = new SimEngine();
  for (let i = 0; i < 501; i++)
    engine.queue.push(createSimEvent(10 + i, 'DISPATCH_MESSAGE', 'test', 'test'));
  engine.clock.start();
  engine.tick(1000);
  expect(engine.queue.size()).toBe(301);
  expect(engine.clock.virtualTimeMs).toBe(209);
  engine.tick(1000);
  engine.tick(1000);
  expect(engine.queue.size()).toBe(0);
  expect(engine.eventLog).toHaveLength(100);
});

it('particles use pause, speed and Step on the same virtual clock and ignore future/expired transits', () => {
  const clock = new SimClock(),
    view = new VirtualClockStore();
  let updates = 0;
  const detach = view.subscribe(() => updates++);
  clock.start();
  clock.setSpeed(2);
  view.publish(clock.advance(25));
  expect(tokenProgress(0, 100, view.getSnapshot())).toBe(0.5);
  clock.pause();
  view.publish(clock.advance(50));
  expect(updates).toBe(1);
  expect(tokenProgress(0, 100, view.getSnapshot())).toBe(0.5);
  clock.jumpTo(75);
  view.publish(clock.virtualTimeMs);
  expect(tokenProgress(0, 100, view.getSnapshot())).toBe(0.75);
  expect(tokenProgress(100, 200, 75)).toBeNull();
  expect(tokenProgress(0, 100, 100)).toBeNull();
  detach();
  view.publish(100);
  expect(updates).toBe(2);
});

it('publishes particle membership immediately and keeps inactive edge snapshots stable', () => {
  const view = new VirtualClockStore();
  const inactive = view.getEdgeSnapshot('idle');
  const token = {
    id: 'token',
    messageId: 'message',
    messageKey: 'key',
    edgeId: 'wire',
    sourceNodeId: 'source',
    targetNodeId: 'sink',
    startTime: 0,
    arriveTime: 100,
  };
  view.publish(0, [token]);
  const first = view.getEdgeSnapshot('wire');
  expect(first.tokens).toHaveLength(1);
  view.publish(16, [token]);
  expect(view.getEdgeSnapshot('wire').now).toBe(16);
  expect(view.getEdgeSnapshot('wire').tokens).toBe(first.tokens);
  expect(view.getEdgeSnapshot('idle')).toBe(inactive);
  view.publish(16, []);
  expect(view.getEdgeSnapshot('wire').tokens).toHaveLength(0);
  expect(first.tokens).toHaveLength(1);
});

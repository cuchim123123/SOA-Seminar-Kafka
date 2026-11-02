import { useEffect, useId, useRef, useState } from 'react';
import type { useSimulationStore } from '../../store/SimulationSession';
import { SIMULATION_LIMITS } from '../../domain/limits';
import { producerTopic } from '../../domain/producer-route';
import { CurrentOffsetMenu } from './CurrentOffsetMenu';
import { TopicSettings } from './TopicSettings';

type Store = ReturnType<typeof useSimulationStore>;
type Confirmation = { title: string; description: string; action: () => void };

export function SimulationControls({
  store,
  builderOpen,
  onToggleBuilder,
  onSelectScenario,
  onPoison,
}: {
  store: Store;
  builderOpen: boolean;
  onToggleBuilder: () => void;
  onSelectScenario: (id: string) => void;
  onPoison: (producerId: string) => void;
}) {
  const { engine, nodes, edges, blueprints, activeBlueprintId, mechanicsConfig: config } = store;
  const selectedProducerId = store.composer.producerId;
  const key = store.composer.key;
  const count = store.composer.burstCount;
  const setSelectedProducerId = (producerId: string) =>
    store.setComposer((previous) => ({ ...previous, producerId }));
  const setKey = (key: string | null) => store.setComposer((previous) => ({ ...previous, key }));
  const setCount = (burstCount: number) =>
    store.setComposer((previous) => ({ ...previous, burstCount }));
  const [confirmation, setConfirmation] = useState<Confirmation | null>(null);
  const dialog = useRef<HTMLDialogElement>(null);
  const titleId = useId();
  useEffect(() => {
    if (confirmation) dialog.current?.showModal();
  }, [confirmation]);
  const producers = nodes.filter((node) => node.data.kind === 'producer');
  const producer =
    producers.find((node) => node.id === selectedProducerId) ??
    (producers.length === 1 ? producers[0] : undefined);
  const customerKey = key ?? String(producer?.data.messageKey ?? 'U001');
  const topicName = producerTopic(producer, nodes, edges);
  const topic = engine.kafkaCluster.topics.get(topicName);
  const partition =
    topic && customerKey
      ? config.keyOrderingMode === 'ROUND_ROBIN'
        ? engine.roundRobinIndex % topic.partitions.size
        : engine.kafkaCluster.routeKeyToPartition(topicName, customerKey)
      : null;
  const invalid = store.topologyIssues.length > 0;
  const remaining = SIMULATION_LIMITS.messagesPerRun - engine.acceptedMessageCount;
  const canSend = !!producer && !!customerKey.trim() && !!topic && !invalid && remaining > 0;
  const scenario = blueprints.find((item) => item.id === activeBlueprintId);
  const replaceScenario = (id: string) => {
    const action = () => onSelectScenario(id);
    if (store.isModified || store.eventLog.length > 0)
      setConfirmation({
        title: 'Replace the current system?',
        description:
          'This replaces your diagram and settings with the selected scenario and starts a fresh paused run. Current messages and evidence will be cleared.',
        action,
      });
    else action();
  };

  return (
    <>
      <header className="sim-header">
        <div>
          <p className="sim-eyebrow">Interactive Kafka model · synthetic events</p>
          <h1>Simulation Playground</h1>
        </div>
        <div className="sim-header-actions">
          <label>
            Scenario
            <select
              aria-label="Scenario"
              value={activeBlueprintId}
              onChange={(event) => replaceScenario(event.target.value)}
            >
              {!scenario && <option value={activeBlueprintId}>Custom saved system</option>}
              {blueprints.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <span className="sim-badge">{store.isModified ? 'Customized' : 'Scenario defaults'}</span>
          <button aria-expanded={builderOpen} onClick={onToggleBuilder}>
            Build system
          </button>
          <details className="sim-overflow">
            <summary>Run options</summary>
            <div>
              <button
                onClick={() =>
                  setConfirmation({
                    title: 'Reset this run?',
                    description:
                      'Clear messages, offsets, counters and faults. Keep your diagram and effective settings. The new run starts paused.',
                    action: store.reset,
                  })
                }
              >
                Reset run
              </button>
              <button onClick={() => replaceScenario(activeBlueprintId)}>Restore scenario</button>
              <button
                onClick={() =>
                  setConfirmation({
                    title: 'Clear the canvas?',
                    description:
                      'Remove every component and wire and start a fresh paused run. You can restore a scenario afterward.',
                    action: store.clearCanvas,
                  })
                }
              >
                Clear canvas
              </button>
            </div>
          </details>
        </div>
      </header>
      <div className="sim-playback" role="group" aria-label="Simulation playback">
        <button
          className="sim-primary"
          disabled={invalid && store.playbackStatus !== 'RUNNING'}
          onClick={store.playbackStatus === 'RUNNING' ? store.pause : store.play}
        >
          {store.playbackStatus === 'RUNNING'
            ? 'Pause'
            : store.virtualTimeMs > 0
              ? 'Resume'
              : 'Run'}
        </button>
        <button
          disabled={invalid || engine.queue.size() === 0}
          onClick={store.step}
          title="Process one queued event and remain paused"
        >
          Step event
        </button>
        <label>
          Speed
          <select
            aria-label="Playback speed"
            value={store.speedMultiplier}
            onChange={(event) => store.setSpeed(Number(event.target.value))}
          >
            {[0.25, 0.5, 1, 2, 4].map((speed) => (
              <option key={speed} value={speed}>
                {speed}×
              </option>
            ))}
          </select>
        </label>
        <output className="sim-clock" aria-label="Virtual clock">
          {(store.virtualTimeMs / 1000).toFixed(2)} s{' '}
          <span>{store.playbackStatus === 'RUNNING' ? 'Running' : 'Paused'}</span>
        </output>
        <span className="sim-muted">
          {engine.queue.size()} queued events · {engine.processedMessageCount} completed deliveries
        </span>
        <span className="sim-muted">
          {engine.acceptedMessageCount.toLocaleString()} /{' '}
          {SIMULATION_LIMITS.messagesPerRun.toLocaleString()} submitted events
        </span>
      </div>
      <form
        className="sim-composer"
        aria-label="Message composer"
        onSubmit={(event) => {
          event.preventDefault();
          if (canSend) store.dispatchMessage(producer!.id, customerKey);
        }}
      >
        <label>
          Source
          <select
            aria-label="Message source"
            value={producer?.id ?? ''}
            onChange={(event) => {
              setSelectedProducerId(event.target.value);
              setKey(null);
            }}
          >
            <option value="" disabled>
              Choose a producer
            </option>
            {producers.map((item) => (
              <option key={item.id} value={item.id}>
                {item.data.title}
              </option>
            ))}
          </select>
        </label>
        <label>
          Customer key
          <input
            aria-label="Customer key"
            value={customerKey}
            maxLength={100}
            onChange={(event) => setKey(event.target.value)}
          />
        </label>
        <button className="sim-primary" type="submit" disabled={!canSend}>
          Send event
        </button>
        <label>
          Burst size
          <input
            type="number"
            aria-label="Burst size"
            min={1}
            max={SIMULATION_LIMITS.messagesPerBurst}
            value={Number.isNaN(count) ? '' : count}
            onChange={(event) => setCount(event.target.valueAsNumber)}
          />
        </label>
        <button
          type="button"
          disabled={
            !canSend ||
            !Number.isInteger(count) ||
            count < 1 ||
            count > SIMULATION_LIMITS.messagesPerBurst ||
            count > remaining
          }
          onClick={() => store.dispatchOrderedBurst(count, customerKey, producer!.id)}
        >
          Send burst
        </button>
        <details className="sim-overflow">
          <summary>Message exercises</summary>
          <div>
            <button
              type="button"
              disabled={
                remaining < 1 ||
                !engine.lastDispatchedMessage ||
                !engine.nodes.has(engine.lastDispatchedProducerId ?? '') ||
                invalid
              }
              title="Retry the previous event ID through its original producer"
              onClick={store.dispatchDuplicateMessage}
            >
              Duplicate previous event
            </button>
            <button
              type="button"
              disabled={!canSend || !nodes.some((node) => node.data.kind === 'consumer')}
              onClick={() => onPoison(producer!.id)}
            >
              Inject poison event…
            </button>
            <small>
              Duplicates reuse the original source and event ID. Poison injection lets you select
              affected consumers/groups.
            </small>
          </div>
        </details>
        <p className="sim-routing">
          {remaining === 0 &&
            'Run limit reached; Reset run to send more. Existing records and pending work are preserved. '}
          {!producer
            ? 'Choose a source to send events.'
            : topic && partition !== null
              ? `Next destination: ${topicName} · P${partition} of ${topic.partitions.size} (${config.keyOrderingMode === 'ROUND_ROBIN' ? 'round robin; no key ordering' : 'Murmur2 key routing'})`
              : 'Connect a source to an existing topic.'}{' '}
          {store.playbackStatus !== 'RUNNING' &&
            'Events queue while paused; use Run, Resume or Step.'}
        </p>
      </form>
      <details className="sim-mechanics">
        <summary>Kafka mechanics &amp; offsets</summary>
        <div className="sim-settings">
          <label>
            Producer acknowledgments
            <select
              value={config.producerAcks}
              onChange={(event) =>
                store.setProducerAcks(event.target.value as typeof config.producerAcks)
              }
            >
              <option value="all">acks=all · ISR acknowledgment</option>
              <option value="1">acks=1 · leader only</option>
              <option value="0">acks=0 · no acknowledgment</option>
            </select>
          </label>
          <label>
            Partition routing
            <select
              value={config.keyOrderingMode}
              onChange={(event) =>
                store.setPartitioningStrategy(
                  event.target.value === 'ROUND_ROBIN' ? 'round-robin' : 'murmur2-key',
                )
              }
            >
              <option value="MURMUR2_KEYED">Murmur2 key</option>
              <option value="ROUND_ROBIN">Round robin (no key ordering)</option>
            </select>
          </label>
          <label className="sim-checkbox">
            <input
              type="checkbox"
              checked={config.consumerIdempotency}
              onChange={store.toggleConsumerIdempotency}
            />
            Consumer database idempotency
          </label>
          <CurrentOffsetMenu
            engine={engine}
            config={config}
            onSetCommitMode={store.setCommitMode}
            onSetAutoOffsetReset={store.setAutoOffsetReset}
            onSeek={store.seekPartitionOffset}
            onResetGroup={store.resetGroupOffsets}
          />
          <TopicSettings store={store} />
          <p className="sim-muted">
            Model controls are global unless a group is named. Manual mode persists the handler
            result before committing N+1. This is an educational model, not a real broker
            performance test.
          </p>
        </div>
      </details>
      <details className="sim-scenario-guide">
        <summary>About this scenario</summary>
        <p>{scenario?.description}</p>
        <p>{scenario?.highlights.join(' · ')}</p>
      </details>
      {confirmation && (
        <dialog
          ref={dialog}
          className="sim-confirm"
          aria-labelledby={titleId}
          onCancel={() => setConfirmation(null)}
        >
          <h2 id={titleId}>{confirmation.title}</h2>
          <p>{confirmation.description}</p>
          <div>
            <button autoFocus onClick={() => setConfirmation(null)}>
              Cancel
            </button>
            <button
              className="sim-primary"
              onClick={() => {
                confirmation.action();
                setConfirmation(null);
              }}
            >
              Confirm
            </button>
          </div>
        </dialog>
      )}
    </>
  );
}

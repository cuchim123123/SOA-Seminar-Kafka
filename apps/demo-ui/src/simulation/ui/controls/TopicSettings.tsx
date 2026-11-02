import { useState } from 'react';
import type { useSimulationStore } from '../../store/SimulationSession';
type Store = ReturnType<typeof useSimulationStore>;

export function TopicSettings({ store }: { store: Store }) {
  const topics = [...store.engine.kafkaCluster.topics.values()];
  const [selected, setSelected] = useState(topics[0]?.name ?? '');
  const [name, setName] = useState(''),
    [partitions, setPartitions] = useState('4'),
    [rf, setRf] = useState('1'),
    [minIsr, setMinIsr] = useState('1');
  const [editing, setEditing] = useState(false);
  const topic = store.engine.kafkaCluster.topics.get(selected);
  const choose = (name: string) => {
    const current = store.engine.kafkaCluster.topics.get(name);
    setSelected(name);
    setEditing(false);
    setPartitions(String(current?.partitions.size ?? 4));
    setRf(String(current?.replicationFactor ?? 1));
    setMinIsr(String(current?.minInsyncReplicas ?? 1));
  };
  return (
    <details className="sim-topic-settings">
      <summary>Topics &amp; replication</summary>
      <div className="sim-topic-list">
        {topics.map((item) => (
          <button
            key={item.name}
            onClick={() => choose(item.name)}
            aria-pressed={selected === item.name}
          >
            {item.name} · {item.partitions.size} partitions · RF {item.replicationFactor} · minISR{' '}
            {item.minInsyncReplicas}
          </button>
        ))}
      </div>
      <form
        className="sim-topic-form"
        onSubmit={(event) => {
          event.preventDefault();
          const config = {
            partitionCount: Number(partitions),
            replicationFactor: Number(rf),
            minInsyncReplicas: Number(minIsr),
          };
          if (editing && topic) store.configureTopic(topic.name, config);
          else if (store.addTopic(name, config)) {
            setSelected(name);
            setName('');
          }
        }}
      >
        <button
          type="button"
          onClick={() => {
            if (topic) {
              choose(topic.name);
              setEditing(true);
            }
          }}
          disabled={!topic}
        >
          Edit selected topic
        </button>
        <button
          type="button"
          onClick={() => {
            setEditing(false);
            setPartitions('4');
            setRf('1');
            setMinIsr('1');
          }}
        >
          New topic
        </button>
        <label>
          {editing ? 'Editing topic' : 'New topic name'}
          <input
            aria-label="Topic name"
            value={editing ? selected : name}
            readOnly={editing}
            onChange={(event) => setName(event.target.value)}
            required
          />
        </label>
        <label>
          Partitions
          <input
            type="number"
            min={editing ? topic?.partitions.size : 1}
            max={12}
            value={partitions}
            onChange={(event) => setPartitions(event.target.value)}
            required
          />
        </label>
        <label>
          Replication factor
          <input
            type="number"
            min={1}
            max={store.engine.kafkaCluster.brokers.size}
            value={rf}
            onChange={(event) => setRf(event.target.value)}
            required
          />
        </label>
        <label>
          MinISR
          <input
            type="number"
            min={1}
            max={100}
            value={minIsr}
            onChange={(event) => setMinIsr(event.target.value)}
            required
          />
        </label>
        <button className="sim-primary" type="submit">
          {editing ? 'Apply topic settings' : 'Create topic'}
        </button>
      </form>
      <p className="sim-muted">
        Applying settings pauses and preserves records. Partitions can only increase. A minISR above
        RF deliberately prevents acks=all writes. Set producer/relay target and group subscription
        in their inspectors to use a new topic.
      </p>
    </details>
  );
}

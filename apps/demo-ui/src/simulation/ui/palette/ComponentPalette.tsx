import { useState } from 'react';
import { COMPONENT_REGISTRY } from '../../registry/component-registry';
import type { ComponentKind } from '../../domain/types';

export function ComponentPalette({
  isOpen,
  onToggle,
  onDragStart,
  onAdd,
  onAddProjection,
}: {
  isOpen: boolean;
  onToggle: () => void;
  onDragStart: (event: React.DragEvent, kind: string) => void;
  onAdd: (kind: ComponentKind) => void;
  onAddProjection: () => void;
}) {
  const [search, setSearch] = useState('');
  if (!isOpen) return null;
  const filtered = COMPONENT_REGISTRY.filter((item) =>
    `${item.title} ${item.description}`.toLowerCase().includes(search.toLowerCase()),
  );
  return (
    <aside className="sim-builder" aria-label="Build system">
      <div className="sim-panel-heading">
        <h2>Build system</h2>
        <button onClick={onToggle} aria-label="Close builder">
          ×
        </button>
      </div>
      <p className="sim-muted">
        Click to add at the canvas center, or drag to place. Connect compatible handles. Structural
        edits pause the run.
      </p>
      <button onClick={onAddProjection}>Add independent projection</button>
      <p className="sim-muted">
        Adds a new group and its own database using the selected consumer, or the first group, as a
        template. Use Add replica in the inspector to share a group.
      </p>
      <input
        aria-label="Search components"
        placeholder="Search components"
        value={search}
        onChange={(event) => setSearch(event.target.value)}
      />
      {['messaging', 'application', 'storage'].map((category) => (
        <section key={category}>
          <h3>{category}</h3>
          {filtered
            .filter((item) => item.category === category)
            .map((item) => (
              <button
                className="sim-component"
                key={item.kind}
                draggable
                onDragStart={(event) => onDragStart(event, item.kind)}
                onClick={() => onAdd(item.kind)}
              >
                <strong>
                  {item.icon} {item.title}
                </strong>
                <small>{item.description}</small>
              </button>
            ))}
        </section>
      ))}
      {filtered.length === 0 && <p>No matching components.</p>}
      <details>
        <summary>Model limits</summary>
        <p>
          One processing slot per consumer; deterministic poison exercises; six visible particles
          per wire plus an overflow count. Database engine names are labels. This model uses
          simplified transport/replication timing and is not a capacity benchmark.
        </p>
      </details>
    </aside>
  );
}

import type { SimInFlightToken } from '../domain/types';
export interface EdgeFrame {
  readonly now: number;
  readonly tokens: readonly SimInFlightToken[];
}
export const EMPTY_EDGE_FRAME: EdgeFrame = { now: 0, tokens: [] };

/** Frame-only subscriptions keep particle movement/membership independent of panel snapshots. */
export class VirtualClockStore {
  private now = 0;
  private tokens: readonly SimInFlightToken[] = [];
  private edges = new Map<string, EdgeFrame>();
  private readonly listeners = new Set<() => void>();
  getSnapshot = () => this.now;
  getEdgeSnapshot = (id: string): EdgeFrame => this.edges.get(id) ?? EMPTY_EDGE_FRAME;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  publish(now: number, tokens: readonly SimInFlightToken[] = this.tokens) {
    const grouped = new Map<string, SimInFlightToken[]>();
    for (const token of tokens) {
      const edge = grouped.get(token.edgeId) ?? [];
      edge.push(token);
      grouped.set(token.edgeId, edge);
    }
    let changed = now !== this.now || grouped.size !== this.edges.size;
    const next = new Map<string, EdgeFrame>();
    for (const [id, items] of grouped) {
      const previous = this.edges.get(id);
      const same =
        previous?.tokens.length === items.length &&
        previous.tokens.every((token, index) => token === items[index]);
      const frame =
        previous && same && previous.now === now
          ? previous
          : { now, tokens: same ? previous!.tokens : items };
      changed ||= frame !== previous;
      next.set(id, frame);
    }
    this.tokens = tokens;
    if (!changed) return;
    this.now = now;
    this.edges = next;
    for (const listener of this.listeners) listener();
  }
}

export function tokenProgress(start: number, arrive: number, now: number): number | null {
  if (now < start || now >= arrive) return null;
  return Math.max(0, Math.min(1, (now - start) / Math.max(1, arrive - start)));
}

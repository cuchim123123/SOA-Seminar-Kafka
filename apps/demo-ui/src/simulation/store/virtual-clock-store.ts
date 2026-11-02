/** Small frame-only subscription: particles update without rerendering the inspector/timeline. */
export class VirtualClockStore {
  private now = 0;
  private readonly listeners = new Set<() => void>();
  getSnapshot = () => this.now;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  publish(now: number) {
    if (now === this.now) return;
    this.now = now;
    for (const listener of this.listeners) listener();
  }
}

export function tokenProgress(start: number, arrive: number, now: number): number | null {
  if (now < start || now >= arrive) return null;
  return Math.max(0, Math.min(1, (now - start) / Math.max(1, arrive - start)));
}

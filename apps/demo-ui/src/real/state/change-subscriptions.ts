export type LiveService = 'producer' | 'loyalty' | 'analytics' | 'replay';
export type ConnectionState = 'connecting' | 'live' | 'disconnected';
interface ChangeSource {
  addEventListener(type: string, listener: () => void): void;
  close(): void;
}
interface Subscriber {
  changed(): void;
  connection(state: ConnectionState): void;
}
const paths: Record<LiveService, string> = {
  producer: '/api/producer/api/v1/purchase-changes',
  loyalty: '/api/loyalty/api/v1/loyalty-changes',
  analytics: '/api/analytics/api/v1/analytics-changes',
  replay: '/api/replay/api/v1/replay-changes',
};

/** One EventSource per active service, shared by all mounted resource hooks. */
export class ChangeSubscriptionPool {
  private readonly entries = new Map<
    LiveService,
    { source: ChangeSource; state: ConnectionState; subscribers: Set<Subscriber> }
  >();
  constructor(private readonly create: (url: string) => ChangeSource) {}

  subscribe(service: LiveService, subscriber: Subscriber): () => void {
    let entry = this.entries.get(service);
    if (!entry) {
      const source = this.create(paths[service]);
      entry = { source, state: 'connecting', subscribers: new Set() };
      const current = entry;
      this.entries.set(service, current);
      source.addEventListener('resync-required', () => {
        if (this.entries.get(service) !== current) return;
        current.state = 'live';
        for (const item of current.subscribers) {
          item.connection('live');
          item.changed();
        }
      });
      source.addEventListener('error', () => {
        if (this.entries.get(service) !== current) return;
        current.state = 'disconnected';
        for (const item of current.subscribers) item.connection('disconnected');
      });
    }
    entry.subscribers.add(subscriber);
    subscriber.connection(entry.state);
    // Always establish subscription BEFORE reading; later notifications dirty the read.
    subscriber.changed();
    const owned = entry;
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      owned.subscribers.delete(subscriber);
      if (!owned.subscribers.size) {
        owned.source.close();
        this.entries.delete(service);
      }
    };
  }
}

export const changeSubscriptions = new ChangeSubscriptionPool((url) => new EventSource(url));

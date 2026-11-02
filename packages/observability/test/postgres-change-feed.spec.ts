import { afterEach, describe, expect, it, vi } from 'vitest';
import { EventEmitter } from 'node:events';
import { PostgresChangeFeed, EventStreamResponse } from '../src/live/postgres-change-feed';

class Connection extends EventEmitter {
  connect = vi.fn().mockResolvedValue(undefined);
  query = vi.fn().mockResolvedValue(undefined);
  end = vi.fn().mockResolvedValue(undefined);
}
const options = {
  channel: 'test_changes',
  heartbeatMs: 1000,
  reconnectMs: 100,
  coalesceMs: 10,
  maxClients: 1,
};
function response() {
  return {
    setHeader: vi.fn(),
    flushHeaders: vi.fn(),
    write: vi.fn().mockReturnValue(true),
    end: vi.fn(),
    destroy: vi.fn(),
    once: vi.fn(),
  };
}
afterEach(() => vi.useRealTimers());

describe('transient database change feed', () => {
  it('requires a ready listener and sends initial snapshot invalidation', async () => {
    const connection = new Connection();
    const feed = new PostgresChangeFeed(options, () => connection);
    const client = response();
    expect(() => feed.attach(client)).toThrow('unavailable');
    await feed.onModuleInit();
    expect(connection.query).toHaveBeenCalledWith('LISTEN "test_changes"');
    feed.attach(client);
    expect(client.write).toHaveBeenCalledWith('event: resync-required\ndata: {}\n\n');
    expect(() => feed.attach(response())).toThrow('limit');
    await feed.onModuleDestroy();
    expect(client.end).toHaveBeenCalledOnce();
  });
  it('coalesces bursts, ignores foreign channels and bounds a slow client', async () => {
    vi.useFakeTimers();
    const connection = new Connection();
    const feed = new PostgresChangeFeed(options, () => connection);
    await feed.onModuleInit();
    const client = response();
    feed.attach(client);
    connection.emit('notification', { channel: 'other' });
    for (let i = 0; i < 100; i++) connection.emit('notification', { channel: options.channel });
    await vi.advanceTimersByTimeAsync(10);
    expect(client.write).toHaveBeenCalledTimes(2);
    client.write.mockReturnValue(false);
    await vi.advanceTimersByTimeAsync(1000);
    expect(client.destroy).toHaveBeenCalledOnce();
    feed.attach(response()); // Slot released after backpressure.
    await feed.onModuleDestroy();
  });
  it('closes stale streams and reconnects the database listener without polling data', async () => {
    vi.useFakeTimers();
    const first = new Connection();
    const second = new Connection();
    const create = vi.fn().mockReturnValueOnce(first).mockReturnValue(second);
    const feed = new PostgresChangeFeed(options, create);
    await feed.onModuleInit();
    const client = response();
    feed.attach(client);
    first.emit('error', new Error('lost connection'));
    expect(client.end).toHaveBeenCalledOnce();
    expect(() => feed.attach(response())).toThrow('unavailable');
    await vi.advanceTimersByTimeAsync(100);
    const reconnected = response();
    feed.attach(reconnected);
    expect(reconnected.write).toHaveBeenCalledWith('event: resync-required\ndata: {}\n\n');
    expect(second.query).toHaveBeenCalledTimes(1);
    await feed.onModuleDestroy();
    await vi.advanceTimersByTimeAsync(10000);
    expect(create).toHaveBeenCalledTimes(2);
  });
  it('removes a closed response without retaining its connection slot', async () => {
    const feed = new PostgresChangeFeed(options, () => new Connection());
    await feed.onModuleInit();
    let close!: () => void;
    const client: EventStreamResponse = {
      ...response(),
      once: (_event, callback) => {
        close = callback;
      },
    };
    feed.attach(client);
    close();
    feed.attach(response());
    await feed.onModuleDestroy();
  });
});

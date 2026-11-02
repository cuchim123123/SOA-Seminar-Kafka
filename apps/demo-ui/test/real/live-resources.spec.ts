import { describe, expect, it, vi } from 'vitest';
import { ChangeSubscriptionPool } from '../../src/real/state/change-subscriptions';
import { observeResource } from '../../src/real/state/resource-observer';

describe('server-pushed resource reconciliation', () => {
  it('shares streams, reports disconnect and resyncs without treating open as fresh data', () => {
    const handlers = new Map<string, () => void>();
    const source = {
      addEventListener: (name: string, fn: () => void) => handlers.set(name, fn),
      close: vi.fn(),
    };
    const factory = vi.fn().mockReturnValue(source);
    const pool = new ChangeSubscriptionPool(factory);
    const a = { changed: vi.fn(), connection: vi.fn() };
    const b = { changed: vi.fn(), connection: vi.fn() };
    const stopA = pool.subscribe('loyalty', a);
    const stopB = pool.subscribe('loyalty', b);
    expect(factory).toHaveBeenCalledTimes(1);
    handlers.get('error')!();
    expect(a.connection).toHaveBeenLastCalledWith('disconnected');
    handlers.get('resync-required')!();
    expect(a.changed).toHaveBeenCalledTimes(2);
    stopA();
    expect(source.close).not.toHaveBeenCalled();
    stopB();
    expect(source.close).toHaveBeenCalledOnce();
    handlers.get('resync-required')!();
    expect(a.changed).toHaveBeenCalledTimes(2);
  });
  it('coalesces changes during a pending read without dropping the last invalidation', async () => {
    let resolve!: (value: number) => void;
    const load = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise<number>((done) => {
            resolve = done;
          }),
      )
      .mockResolvedValue(2);
    const update = vi.fn();
    const resource = observeResource(load, update);
    const first = resource.refresh();
    void resource.refresh();
    void resource.refresh();
    resolve(1);
    await first;
    expect(load).toHaveBeenCalledTimes(2);
    expect(update).toHaveBeenLastCalledWith(
      expect.objectContaining({ data: 2, error: null, loading: false }),
    );
    resource.stop();
  });
  it('keeps previous data visibly failed and ignores old responses after disposal', async () => {
    const update = vi.fn();
    const load = vi.fn().mockResolvedValueOnce(1).mockRejectedValueOnce(new Error('offline'));
    const resource = observeResource(load, update);
    await resource.refresh();
    await resource.refresh();
    expect(update).toHaveBeenLastCalledWith(expect.objectContaining({ data: 1, error: 'offline' }));
    resource.stop();
    let resolve!: (value: number) => void;
    const slow = observeResource(
      () =>
        new Promise<number>((done) => {
          resolve = done;
        }),
      update,
    );
    const pending = slow.refresh();
    slow.stop();
    const calls = update.mock.calls.length;
    resolve(99);
    await pending;
    expect(update).toHaveBeenCalledTimes(calls);
  });
});

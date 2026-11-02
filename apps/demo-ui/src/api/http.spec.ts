import { afterEach, expect, it, vi } from 'vitest';
import { request, ApiError } from './http';
import { uiConfig } from '../config/ui-config';

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});
it('bounds a request without retrying an uncertain mutation', async () => {
  vi.useFakeTimers();
  const fetcher = vi.fn(
    (_url: string, options: RequestInit) =>
      new Promise((_resolve, reject) => {
        options.signal?.addEventListener('abort', () => reject(options.signal?.reason));
      }),
  );
  vi.stubGlobal('fetch', fetcher);
  const result = request('/test', { method: 'POST', body: '{}' });
  const assertion = expect(result).rejects.toMatchObject({ name: 'TimeoutError' });
  await vi.advanceTimersByTimeAsync(uiConfig.VITE_HTTP_TIMEOUT_MS);
  await assertion;
  expect(fetcher).toHaveBeenCalledTimes(1);
});
it('preserves caller cancellation and HTTP conflict evidence', async () => {
  const caller = new AbortController();
  vi.stubGlobal(
    'fetch',
    vi.fn(
      (_url, options) =>
        new Promise((_resolve, reject) =>
          options.signal.addEventListener('abort', () => reject(options.signal.reason)),
        ),
    ),
  );
  const pending = request('/test', { signal: caller.signal });
  const assertion = expect(pending).rejects.toMatchObject({ name: 'AbortError' });
  caller.abort();
  await assertion;
  vi.stubGlobal(
    'fetch',
    vi
      .fn()
      .mockResolvedValue(
        new Response(JSON.stringify({ message: 'conflict', eventId: 'existing' }), { status: 409 }),
      ),
  );
  try {
    await request('/test');
    throw new Error('Expected conflict');
  } catch (error) {
    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({ status: 409, details: { eventId: 'existing' } });
  }
});

import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError, request } from '../../src/api/http';
import { api } from '../../src/api/client';
import { businessApi } from '../../src/api/business';

afterEach(() => vi.unstubAllGlobals());
describe('business HTTP evidence', () => {
  it('preserves conflict status and structured details without retrying a write', async () => {
    const details = { message: ['Conflicting payload'], code: 'CONFLICT' };
    const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify(details), { status: 409 }));
    vi.stubGlobal('fetch', fetch);
    await expect(request('/purchase', { method: 'POST', body: '{}' })).rejects.toMatchObject({
      status: 409,
      message: 'Conflicting payload',
      details,
    });
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('treats only an account404 as absence', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response('{}', { status: 404 }))
      .mockResolvedValueOnce(new Response('{}', { status: 503 }))
      .mockRejectedValueOnce(new TypeError('Network unavailable'));
    vi.stubGlobal('fetch', fetch);
    expect(await api.getLoyaltyAccount('a/b')).toBeNull();
    await expect(api.getLoyaltyAccount('a/b')).rejects.toBeInstanceOf(ApiError);
    await expect(api.getLoyaltyAccount('a/b')).rejects.toThrow('Network unavailable');
    expect(fetch.mock.calls[0][0]).toContain('a%2Fb');
  });
  it('encodes IDs and forwards cancellation for canonical reads', async () => {
    const fetch = vi.fn().mockResolvedValue(new Response('{}'));
    vi.stubGlobal('fetch', fetch);
    const controller = new AbortController();
    await businessApi.getPurchase('p/a?b', controller.signal);
    expect(fetch).toHaveBeenCalledWith(
      '/api/producer/api/v1/purchases/p%2Fa%3Fb',
      expect.objectContaining({ signal: controller.signal }),
    );
  });
});

import type { ProxyOptions } from 'vite';

/** An interrupted upstream SSE must terminate the downstream stream too.
 * Otherwise EventSource keeps a live-looking connection and never resnapshots.
 */
export const configureLiveProxy: NonNullable<ProxyOptions['configure']> = (proxy) => {
  proxy.on('proxyRes', (upstream, _request, downstream) => {
    if (!upstream.headers['content-type']?.includes('text/event-stream')) return;
    const disconnect = () => downstream.destroy();
    upstream.once('aborted', disconnect);
    upstream.once('error', disconnect);
    upstream.once('close', () => {
      if (!upstream.complete) disconnect();
    });
  });
};

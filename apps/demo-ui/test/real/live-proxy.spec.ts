import { createServer as createHttpServer, get } from 'node:http';
import type { AddressInfo } from 'node:net';
import { createServer } from 'vite';
import { expect, it } from 'vitest';
import { configureLiveProxy } from '../../live-proxy';

it('closes the browser-facing SSE when its upstream dies, allowing EventSource reconnect', async () => {
  let interrupt: () => void = () => undefined;
  const upstream = createHttpServer((_request, response) => {
    response.writeHead(200, { 'Content-Type': 'text/event-stream' });
    response.write('event: resync-required\ndata: {}\n\n');
    interrupt = () => response.destroy();
  });
  await new Promise<void>((resolve) => upstream.listen(0, '127.0.0.1', resolve));
  const vite = await createServer({
    configFile: false,
    optimizeDeps: { noDiscovery: true, include: [] },
    server: {
      host: '127.0.0.1',
      port: 0,
      proxy: {
        '/events': {
          target: `http://127.0.0.1:${(upstream.address() as AddressInfo).port}`,
          configure: configureLiveProxy,
        },
      },
    },
  });
  try {
    await vite.listen();
    const outcome = await new Promise<string>((resolve, reject) => {
      const client = get(
        `http://127.0.0.1:${(vite.httpServer!.address() as AddressInfo).port}/events`,
        (response) => {
          response.once('data', () => interrupt());
          response.once('aborted', () => resolve('disconnected'));
          response.once('error', () => resolve('disconnected'));
        },
      );
      client.once('error', reject);
      client.setTimeout(3000, () => {
        client.destroy();
        reject(new Error('Proxy kept interrupted stream open'));
      });
    });
    expect(outcome).toBe('disconnected');
  } finally {
    await vite.close();
    upstream.closeAllConnections();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
  }
}, 10000);

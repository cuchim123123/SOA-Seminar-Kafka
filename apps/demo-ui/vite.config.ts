import { defineConfig, loadEnv } from 'vite';
import { z } from 'zod';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, '../..', '');
  const origin = z
    .string()
    .url()
    .refine((value) => {
      const url = new URL(value);
      return ['http:', 'https:'].includes(url.protocol) && !url.username && !url.password;
    });
  const targets = z
    .object({
      UI_PRODUCER_ORIGIN: origin.default('http://localhost:3000'),
      UI_LOYALTY_ORIGIN: origin.default('http://localhost:3001'),
      UI_ANALYTICS_ORIGIN: origin.default('http://localhost:3002'),
      UI_REPLAY_ORIGIN: origin.default('http://localhost:3004'),
    })
    .parse(env);
  const connectUrl = env.KAFKA_CONNECT_URL || 'http://localhost:8083';
  const connector = env.CDC_CONNECTOR_NAME || 'purchases-outbox-connector';
  return {
    envDir: '../..',
    plugins: [],
    server: {
      port: 5173,
      proxy: {
        '/api/producer': {
          target: targets.UI_PRODUCER_ORIGIN,
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api\/producer/, ''),
        },
        '/api/loyalty': {
          target: targets.UI_LOYALTY_ORIGIN,
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api\/loyalty/, ''),
        },
        '/api/analytics': {
          target: targets.UI_ANALYTICS_ORIGIN,
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api\/analytics/, ''),
        },
        '/api/replay': {
          target: targets.UI_REPLAY_ORIGIN,
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api\/replay/, ''),
        },
        '/api/cdc/status': {
          target: connectUrl,
          changeOrigin: true,
          rewrite: () => `/connectors/${encodeURIComponent(connector)}/status`,
        },
        '/health/producer': {
          target: targets.UI_PRODUCER_ORIGIN,
          changeOrigin: true,
          rewrite: () => '/health',
        },
        '/health/loyalty': {
          target: targets.UI_LOYALTY_ORIGIN,
          changeOrigin: true,
          rewrite: () => '/health',
        },
        '/health/analytics': {
          target: targets.UI_ANALYTICS_ORIGIN,
          changeOrigin: true,
          rewrite: () => '/health',
        },
        '/health/replay': {
          target: targets.UI_REPLAY_ORIGIN,
          changeOrigin: true,
          rewrite: () => '/health',
        },
      },
    },
  };
});

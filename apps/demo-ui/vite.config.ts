import { defineConfig, loadEnv } from 'vite';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, '../..', '');
  const connectUrl = env.KAFKA_CONNECT_URL || 'http://localhost:8083';
  const connector = env.CDC_CONNECTOR_NAME || 'purchases-outbox-connector';
  return {
    plugins: [],
    server: {
      port: 5173,
      proxy: {
        '/api/producer': {
          target: 'http://localhost:3000',
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api\/producer/, ''),
        },
        '/api/loyalty': {
          target: 'http://localhost:3001',
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api\/loyalty/, ''),
        },
        '/api/analytics': {
          target: 'http://localhost:3002',
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api\/analytics/, ''),
        },
        '/api/replay': {
          target: 'http://localhost:3004',
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api\/replay/, ''),
        },
        '/api/cdc/status': {
          target: connectUrl,
          changeOrigin: true,
          rewrite: () => `/connectors/${encodeURIComponent(connector)}/status`,
        },
        '/health/producer': {
          target: 'http://localhost:3000',
          changeOrigin: true,
          rewrite: () => '/health',
        },
        '/health/loyalty': {
          target: 'http://localhost:3001',
          changeOrigin: true,
          rewrite: () => '/health',
        },
        '/health/analytics': {
          target: 'http://localhost:3002',
          changeOrigin: true,
          rewrite: () => '/health',
        },
        '/health/replay': {
          target: 'http://localhost:3004',
          changeOrigin: true,
          rewrite: () => '/health',
        },
      },
    },
  };
});

import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react()],
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
      '/health/publisher': {
        target: 'http://localhost:3003',
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
});

import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';

if (typeof process.loadEnvFile === 'function') {
  try {
    process.loadEnvFile('../../.env');
  } catch {
    try {
      process.loadEnvFile('.env');
    } catch {
      // Ignore if .env doesn't exist
    }
  }
}

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['test/**/*.spec.ts', 'src/**/*.spec.ts'],
  },
  plugins: [
    swc.vite({
      tsconfigFile: './tsconfig.json',
    }),
  ],
});

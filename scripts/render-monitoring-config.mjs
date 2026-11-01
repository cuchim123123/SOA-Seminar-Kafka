import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.resolve(__dirname, '..');

// Load .env if present
const envPath = path.join(rootDir, '.env');
if (fs.existsSync(envPath) && typeof process.loadEnvFile === 'function') {
  try {
    process.loadEnvFile(envPath);
  } catch (err) {
    console.warn(`[render-monitoring-config] Warning reading .env: ${err.message}`);
  }
}

const templatePath = path.join(
  rootDir,
  'docker',
  'monitoring',
  'prometheus',
  'prometheus.yml.template',
);
const outputPath = path.join(rootDir, 'docker', 'monitoring', 'prometheus', 'prometheus.yml');

if (!fs.existsSync(templatePath)) {
  console.error(`[render-monitoring-config] Template not found: ${templatePath}`);
  process.exit(1);
}

const template = fs.readFileSync(templatePath, 'utf8');

const replacements = {
  PROMETHEUS_SCRAPE_INTERVAL: process.env.PROMETHEUS_SCRAPE_INTERVAL || '15s',
  PROMETHEUS_EVALUATION_INTERVAL: process.env.PROMETHEUS_EVALUATION_INTERVAL || '15s',
  HOST_DOCKER_INTERNAL: process.env.HOST_DOCKER_INTERNAL || 'host.docker.internal',
  PORT: process.env.PORT || '3000',
  LOYALTY_WORKER_PORT: process.env.LOYALTY_WORKER_PORT || '3001',
  ANALYTICS_WORKER_PORT: process.env.ANALYTICS_WORKER_PORT || '3002',
  REPLAY_WORKER_PORT: process.env.REPLAY_WORKER_PORT || '3004',
  NODE_ENV: process.env.NODE_ENV || 'development',
  BENCH_SCRAPE_INTERVAL: process.env.BENCH_SCRAPE_INTERVAL || '2s',
  BENCH_KAFKA_EXPORTER_TARGET: process.env.BENCH_KAFKA_EXPORTER_TARGET || 'kafka-exporter:9308',
  BENCH_REDIS_EXPORTER_TARGET: process.env.BENCH_REDIS_EXPORTER_TARGET || 'redis-exporter:9121',
  BENCH_RABBITMQ_METRICS_TARGET:
    process.env.BENCH_RABBITMQ_METRICS_TARGET || 'rabbitmq-bench:15692',
};

let rendered = template;
for (const [key, value] of Object.entries(replacements)) {
  const regex = new RegExp(`\\$\\{${key}\\}`, 'g');
  rendered = rendered.replace(regex, value);
}

// Ensure parent dir exists
fs.mkdirSync(path.dirname(outputPath), { recursive: true });
fs.writeFileSync(outputPath, rendered, 'utf8');

console.log(
  `[render-monitoring-config] Successfully rendered ${path.relative(rootDir, outputPath)} with targets:`,
);
console.log(`  - producer-api:    ${replacements.HOST_DOCKER_INTERNAL}:${replacements.PORT}`);
console.log(
  `  - loyalty-worker:  ${replacements.HOST_DOCKER_INTERNAL}:${replacements.LOYALTY_WORKER_PORT}`,
);
console.log(
  `  - analytics-worker:${replacements.HOST_DOCKER_INTERNAL}:${replacements.ANALYTICS_WORKER_PORT}`,
);
console.log(
  `  - replay-worker:   ${replacements.HOST_DOCKER_INTERNAL}:${replacements.REPLAY_WORKER_PORT}`,
);

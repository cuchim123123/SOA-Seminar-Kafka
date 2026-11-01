import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { z } from 'zod';
import { benchmarkSchema, buildCommand } from './official-benchmark/config.mjs';
import { executeBenchmark } from './official-benchmark/execution.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

async function main() {
  const args = process.argv.slice(2).filter((arg) => arg !== '--');
  if (args.length === 0 || args.includes('--help')) {
    console.log(
      'Usage: pnpm benchmark:preview|benchmark:run <kafka|rabbitmq|redis|all> [--config file.json]\nEdit config/official-benchmark.json. Runs use existing benchmark brokers; start them with pnpm benchmark:up.',
    );
    return;
  }
  const action = z.enum(['preview', 'run']).parse(args.shift());
  const broker = z.enum(['kafka', 'rabbitmq', 'redis', 'all']).parse(args.shift());
  let configPath = path.join(root, 'config/official-benchmark.json');
  if (args.length) {
    if (args.length !== 2 || args[0] !== '--config')
      throw new Error('Only --config <file> is supported');
    configPath = path.resolve(args[1]);
  }
  const config = benchmarkSchema.parse(JSON.parse(await fs.readFile(configPath, 'utf8')));
  const brokers = broker === 'all' ? ['kafka', 'rabbitmq', 'redis'] : [broker];
  if (action === 'preview') {
    for (const selected of brokers)
      console.log(
        JSON.stringify(
          {
            broker: selected,
            config: config[selected],
            commandArgv: [
              'docker',
              ...buildCommand(
                config,
                selected,
                `perf-${selected}-preview`,
                `perf-${selected}-preview-client`,
              ),
            ],
            preparation:
              selected === 'kafka'
                ? { createPartitions: config.kafka.partitions, replicationFactor: 1 }
                : 'Tool creates the unique destination',
            cleanupOwnedData: config.safety.cleanupOwnedData,
          },
          null,
          2,
        ),
      );
    return;
  }
  if (typeof process.loadEnvFile === 'function') {
    try {
      process.loadEnvFile(path.join(root, '.env'));
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
  }
  const password = brokers.includes('rabbitmq')
    ? z
        .string()
        .min(1, `Set ${config.rabbitmq.passwordEnv} in .env`)
        .parse(process.env[config.rabbitmq.passwordEnv])
    : undefined;
  const controller = new AbortController();
  const cancel = () => controller.abort();
  process.once('SIGINT', cancel);
  process.once('SIGTERM', cancel);
  try {
    for (const selected of brokers) {
      if (controller.signal.aborted) {
        process.exitCode = 1;
        break;
      }
      const result = await executeBenchmark(config, selected, {
        resultsRoot: path.join(root, 'benchmark/official'),
        password,
        signal: controller.signal,
      });
      if (result.status === 'failed' || result.cleanupStatus.startsWith('failed')) {
        process.exitCode = 1;
        break;
      }
    }
  } finally {
    process.removeListener('SIGINT', cancel);
    process.removeListener('SIGTERM', cancel);
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});

import { z } from 'zod';

const positive = z.number().int().positive();
const name = z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/);
const image = z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_./:@-]*$/);
const host = z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/);
const rate = z.union([z.literal(-1), positive]);

export const benchmarkSchema = z
  .object({
    version: z.literal(1),
    network: name,
    payloadBytes: positive.min(8).max(1048576),
    client: z
      .object({ cpus: z.number().positive().max(16), memoryMiB: positive.max(8192) })
      .strict(),
    safety: z
      .object({
        minFreeDiskMiB: positive,
        pollSeconds: positive.max(30),
        timeoutSeconds: positive.max(3600),
        cleanupOwnedData: z.boolean(),
        additionalHostPaths: z.array(z.string().min(1)).default([]),
      })
      .strict(),
    kafka: z
      .object({
        image,
        container: name,
        host,
        port: positive.max(65535),
        dataPath: z.string().startsWith('/'),
        partitions: positive.max(64),
        records: positive.max(100000000),
        warmupRecords: z.number().int().nonnegative(),
        recordsPerSecond: rate,
        batchBytes: positive,
        lingerMs: z.number().int().nonnegative().max(1000),
        bufferBytes: positive,
      })
      .strict(),
    rabbitmq: z
      .object({
        image,
        container: name,
        host,
        port: positive.max(65535),
        dataPath: z.string().startsWith('/'),
        username: z.string().min(1),
        passwordEnv: name,
        durationSeconds: positive.max(1800),
        producers: positive.max(256),
        batchRecords: positive.max(10000),
        dynamicBatch: z.boolean(),
        outstandingConfirms: positive,
        recordsPerSecondPerProducer: rate,
      })
      .strict(),
    redis: z
      .object({
        image,
        container: name,
        host,
        port: positive.max(65535),
        dataPath: z.string().startsWith('/'),
        durationSeconds: positive.max(1800),
        threads: positive.max(64),
        clientsPerThread: positive.max(256),
        pipeline: positive.max(10000),
        requestsPerSecondPerConnection: positive.nullable(),
      })
      .strict(),
  })
  .strict()
  .superRefine((config, ctx) => {
    if (config.kafka.warmupRecords >= config.kafka.records) {
      ctx.addIssue({
        code: 'custom',
        path: ['kafka', 'warmupRecords'],
        message: 'Must be less than records (warmup is a subset).',
      });
    }
    for (const broker of ['rabbitmq', 'redis']) {
      if (config[broker].durationSeconds + 15 > config.safety.timeoutSeconds) {
        ctx.addIssue({
          code: 'custom',
          path: [broker, 'durationSeconds'],
          message: 'Reserve at least 15 seconds before the safety timeout.',
        });
      }
    }
    if (config.kafka.batchBytes > config.kafka.bufferBytes) {
      ctx.addIssue({
        code: 'custom',
        path: ['kafka', 'batchBytes'],
        message: 'Must not exceed producer bufferBytes.',
      });
    }
  });

export function buildCommand(config, broker, destination, clientName, password = '<from-env>') {
  if (!['kafka', 'rabbitmq', 'redis'].includes(broker)) throw new Error('Unknown broker');
  if (!/^perf-[a-z0-9-]+$/.test(destination)) throw new Error('Invalid owned destination');
  const tool = config[broker];
  const args = [
    'run',
    '--rm',
    '--pull=never',
    '--name',
    clientName,
    '--network',
    config.network,
    '--cpus',
    String(config.client.cpus),
    '--memory',
    `${config.client.memoryMiB}m`,
    '--log-driver',
    'none',
    tool.image,
  ];
  const s = String;
  if (broker === 'kafka') {
    args.push(
      '/opt/kafka/bin/kafka-producer-perf-test.sh',
      '--topic',
      destination,
      '--bootstrap-server',
      `${tool.host}:${tool.port}`,
      '--num-records',
      s(tool.records),
      '--record-size',
      s(config.payloadBytes),
      '--throughput',
      s(tool.recordsPerSecond),
      '--warmup-records',
      s(tool.warmupRecords),
      '--command-property',
      'acks=all',
      `batch.size=${tool.batchBytes}`,
      `linger.ms=${tool.lingerMs}`,
      `buffer.memory=${tool.bufferBytes}`,
    );
  } else if (broker === 'rabbitmq') {
    args.push(
      '--uris',
      `rabbitmq-stream://${encodeURIComponent(tool.username)}:${encodeURIComponent(password)}@${tool.host}:${tool.port}`,
      '--size',
      s(config.payloadBytes),
      '--producers',
      s(tool.producers),
      '--consumers',
      '0',
      '--time',
      s(tool.durationSeconds),
      '--streams',
      destination,
      '--batch-size',
      s(tool.batchRecords),
      '--dynamic-batch-size',
      s(tool.dynamicBatch),
      '--confirms',
      s(tool.outstandingConfirms),
      '--rate',
      s(tool.recordsPerSecondPerProducer),
      '--delete-streams',
      'false',
    );
  } else {
    args.push(
      '--server',
      tool.host,
      '--port',
      s(tool.port),
      '--protocol=redis',
      '--threads',
      s(tool.threads),
      '--clients',
      s(tool.clientsPerThread),
      '--pipeline',
      s(tool.pipeline),
      '--test-time',
      s(tool.durationSeconds),
      '--data-size',
      s(config.payloadBytes),
      '--command',
      `XADD ${destination} * payload __data__`,
    );
    if (tool.requestsPerSecondPerConnection !== null)
      args.push('--rate-limiting', s(tool.requestsPerSecondPerConnection));
  }
  return args;
}

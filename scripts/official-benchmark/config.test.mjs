import { test } from 'vitest';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { benchmarkSchema, buildCommand } from './config.mjs';
import { parseFreeKiB, verifyBroker } from './execution.mjs';

const base = JSON.parse(
  fs.readFileSync(new URL('../../config/official-benchmark.json', import.meta.url)),
);
const config = () => structuredClone(base);
const command = (c, broker) =>
  buildCommand(benchmarkSchema.parse(c), broker, 'perf-test-owned', 'perf-test-client');
const value = (args, flag) => args[args.indexOf(flag) + 1];

test('all common fields and Kafka producer controls reach actual argv', () => {
  const c = config();
  c.payloadBytes = 2048;
  c.client.cpus = 1;
  c.client.memoryMiB = 1024;
  c.network = 'test-network';
  c.kafka.records = 12345;
  c.kafka.warmupRecords = 5;
  c.kafka.recordsPerSecond = 250;
  c.kafka.batchBytes = 100000;
  c.kafka.lingerMs = 7;
  c.kafka.bufferBytes = 2000000;
  const a = command(c, 'kafka');
  for (const [flag, expected] of [
    ['--record-size', '2048'],
    ['--num-records', '12345'],
    ['--warmup-records', '5'],
    ['--throughput', '250'],
    ['--network', 'test-network'],
    ['--cpus', '1'],
    ['--memory', '1024m'],
  ])
    assert.equal(value(a, flag), expected);
  for (const property of ['batch.size=100000', 'linger.ms=7', 'buffer.memory=2000000', 'acks=all'])
    assert.ok(a.includes(property));
});

test('Rabbit settings are explicit, including dynamic batch and per-producer rate', () => {
  const c = config();
  Object.assign(c.rabbitmq, {
    producers: 3,
    durationSeconds: 20,
    batchRecords: 128,
    dynamicBatch: false,
    outstandingConfirms: 1234,
    recordsPerSecondPerProducer: 99,
  });
  const a = command(c, 'rabbitmq');
  for (const [flag, expected] of [
    ['--producers', '3'],
    ['--time', '20'],
    ['--batch-size', '128'],
    ['--confirms', '1234'],
    ['--rate', '99'],
    ['--consumers', '0'],
  ])
    assert.equal(value(a, flag), expected);
  assert.equal(value(a, '--dynamic-batch-size'), 'false');
  assert.equal(value(a, '--delete-streams'), 'false');
});

test('Redis uses XADD, separate threads/clients/pipeline and per-connection rate', () => {
  const c = config();
  Object.assign(c.redis, {
    threads: 2,
    clientsPerThread: 3,
    pipeline: 8,
    durationSeconds: 20,
    requestsPerSecondPerConnection: 45,
  });
  const a = command(c, 'redis');
  for (const [flag, expected] of [
    ['--threads', '2'],
    ['--clients', '3'],
    ['--pipeline', '8'],
    ['--test-time', '20'],
    ['--rate-limiting', '45'],
  ])
    assert.equal(value(a, flag), expected);
  assert.equal(value(a, '--command'), 'XADD perf-test-owned * payload __data__');
  c.redis.requestsPerSecondPerConnection = null;
  assert.ok(!command(c, 'redis').includes('--rate-limiting'));
});

test('invalid config is rejected rather than ignored', () => {
  for (const mutate of [
    (c) => {
      c.typo = 1;
    },
    (c) => {
      c.kafka.concurrency = 16;
    },
    (c) => {
      c.kafka.warmupRecords = c.kafka.records;
    },
    (c) => {
      c.redis.durationSeconds = 90;
    },
    (c) => {
      c.kafka.batchBytes = c.kafka.bufferBytes + 1;
    },
    (c) => {
      c.network = 'a; rm';
    },
  ]) {
    const c = config();
    mutate(c);
    assert.throws(() => benchmarkSchema.parse(c));
  }
});

test('destinations cannot select existing application names or shell expressions', () => {
  for (const destination of ['purchase.events', 'perf-x;bad', 'perf-$(bad)'])
    assert.throws(() => buildCommand(base, 'kafka', destination, 'client'));
  const a = buildCommand(base, 'rabbitmq', 'perf-test', 'client', 'a@b:/c');
  assert.ok(value(a, '--uris').includes('a%40b%3A%2Fc'));
});

test('df must contain a valid available-space column', () => {
  assert.equal(
    parseFreeKiB(
      'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/sda 10000 4000 6000 40% /data',
    ),
    6000,
  );
  for (const text of ['', 'error', 'header\n/dev/sda bad bad nope'])
    assert.throws(() => parseFreeKiB(text));
});

test('execution rejects application containers, stopped brokers and wrong network aliases', () => {
  const item = {
    State: { Running: true },
    Config: {
      Labels: {
        'com.docker.compose.project': 'ingress-benchmark',
        'com.docker.compose.service': 'kafka-bench',
      },
    },
    NetworkSettings: { Networks: { [base.network]: { Aliases: ['kafka-bench'] } } },
  };
  assert.doesNotThrow(() => verifyBroker(item, base, 'kafka'));
  for (const mutate of [
    (x) => {
      x.State.Running = false;
    },
    (x) => {
      x.Config.Labels['com.docker.compose.project'] = 'application';
    },
    (x) => {
      x.NetworkSettings.Networks = {};
    },
  ]) {
    const x = structuredClone(item);
    mutate(x);
    assert.throws(() => verifyBroker(x, base, 'kafka'));
  }
});

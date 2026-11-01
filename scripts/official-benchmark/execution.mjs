import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { randomUUID } from 'node:crypto';
import { benchmarkSchema, buildCommand } from './config.mjs';

const exec = promisify(execFile);
export async function docker(args) {
  const result = await exec('docker', args, {
    timeout: 20000,
    maxBuffer: 4 * 1024 * 1024,
    windowsHide: true,
  });
  return result.stdout.trim();
}

export function parseFreeKiB(output) {
  const lines = output.trim().split(/\r?\n/);
  const fields = lines.at(-1).trim().split(/\s+/);
  const value = Number(fields[3]);
  if (lines.length < 2 || !Number.isFinite(value) || value < 0)
    throw new Error('Cannot verify broker disk space');
  return value;
}

export async function checkDisk(config, broker, resultsRoot) {
  const required = config.safety.minFreeDiskMiB * 1024 * 1024;
  const roots = [
    resultsRoot,
    ...config.safety.additionalHostPaths,
    ...(process.platform === 'win32' ? [path.parse(os.homedir()).root] : []),
  ];
  for (const root of new Set(roots)) {
    const stats = await fs.statfs(root);
    if (stats.bavail * stats.bsize < required)
      throw new Error(`Insufficient host disk headroom: ${root}`);
  }
  const tool = config[broker];
  const free = parseFreeKiB(await docker(['exec', tool.container, 'df', '-Pk', tool.dataPath]));
  if (free * 1024 < required) throw new Error('Insufficient broker-volume disk headroom');
}

export function verifyBroker(inspected, config, broker) {
  const tool = config[broker];
  const labels = inspected.Config.Labels;
  if (
    labels?.['com.docker.compose.project'] !== 'ingress-benchmark' ||
    labels?.['com.docker.compose.service'] !== `${broker}-bench`
  ) {
    throw new Error('Refusing non-benchmark broker container');
  }
  if (!inspected.State.Running)
    throw new Error('Start the benchmark broker first: pnpm benchmark:up');
  const network = inspected.NetworkSettings.Networks[config.network];
  if (
    !network ||
    ![tool.container, ...(network.Aliases ?? []), ...(network.DNSNames ?? [])].includes(tool.host)
  ) {
    throw new Error(
      'Broker host must resolve to the inspected benchmark container on the configured network',
    );
  }
}

async function assertDestinationAbsent(config, broker, destination) {
  const tool = config[broker];
  if (broker === 'kafka') {
    const topics = await docker([
      'exec',
      tool.container,
      '/opt/kafka/bin/kafka-topics.sh',
      '--bootstrap-server',
      `${tool.host}:${tool.port}`,
      '--list',
    ]);
    if (topics.split(/\r?\n/).includes(destination)) throw new Error('Destination already exists');
  } else if (broker === 'redis') {
    const exists = await docker([
      'exec',
      tool.container,
      'redis-cli',
      '-p',
      String(tool.port),
      'EXISTS',
      destination,
    ]);
    if (exists !== '0') throw new Error('Destination exists or Redis preflight failed');
  } else {
    const queues = JSON.parse(
      await docker([
        'exec',
        tool.container,
        'rabbitmqctl',
        'list_queues',
        'name',
        '--formatter',
        'json',
      ]),
    );
    if (queues.some((queue) => queue.name === destination))
      throw new Error('Destination already exists');
  }
}

async function cleanup(config, broker, destination) {
  const tool = config[broker];
  if (broker === 'kafka')
    return docker([
      'exec',
      tool.container,
      '/opt/kafka/bin/kafka-topics.sh',
      '--bootstrap-server',
      `${tool.host}:${tool.port}`,
      '--delete',
      '--topic',
      destination,
    ]);
  if (broker === 'redis')
    return docker([
      'exec',
      tool.container,
      'redis-cli',
      '-p',
      String(tool.port),
      'UNLINK',
      destination,
    ]);
  return docker(['exec', tool.container, 'rabbitmqctl', 'delete_queue', destination]);
}

async function cleanupOwned(config, broker, destination, brokerId, clientStopped) {
  try {
    if (!clientStopped) return 'failed: client stop not verified';
    const current = JSON.parse(await docker(['inspect', config[broker].container]))[0];
    if (current.Id !== brokerId) return 'failed: broker identity changed';
    verifyBroker(current, config, broker);
    try {
      await cleanup(config, broker, destination);
      return 'requested';
    } catch {
      // Tools can fail before creating their destination. An already-absent resource needs no deletion.
      await assertDestinationAbsent(config, broker, destination);
      return 'already-absent';
    }
  } catch {
    return 'failed: owned destination may remain';
  }
}

/** A lock also protects future callers outside the CLI. A crash leaves a visible stale lock. */
export async function executeBenchmark(config, broker, options) {
  config = benchmarkSchema.parse(config);
  await fs.mkdir(options.resultsRoot, { recursive: true });
  const lock = path.join(options.resultsRoot, '.active-run');
  try {
    await fs.mkdir(lock);
  } catch (error) {
    if (error.code === 'EEXIST')
      throw new Error(
        `Another run or stale lock exists: ${lock}. Check Docker clients before removing the empty lock directory.`,
      );
    throw error;
  }
  try {
    return await runOne(config, broker, options);
  } finally {
    await fs.rmdir(lock);
  }
}

/** Run one official tool. No broker startup, unrelated cleanup, or shell command interpolation. */
async function runOne(config, broker, { resultsRoot, password, signal } = {}) {
  const runId = `perf-${broker}-${randomUUID()}`;
  const clientName = `${runId}-client`;
  const tool = config[broker];
  if (!tool) throw new Error('Unknown broker');
  if (broker === 'rabbitmq' && !password)
    throw new Error(`Set ${tool.passwordEnv} before running RabbitMQ`);
  await fs.mkdir(resultsRoot, { recursive: true });
  const folder = path.join(resultsRoot, runId);
  await fs.mkdir(folder);
  const write = (name, value) =>
    fs.writeFile(path.join(folder, name), JSON.stringify(value, null, 2) + '\n');
  const manifest = {
    version: 1,
    runId,
    broker,
    destination: runId,
    startedAt: new Date().toISOString(),
    config,
    command: ['docker', ...buildCommand(config, broker, runId, clientName)],
    resultSemantics: 'Official output; process success does not certify zero publish errors.',
  };
  await write('manifest.json', manifest);
  let ownsDestination = false;
  let child;
  let timer;
  let guard;
  let monitoring = false;
  let monitorPromise;
  let stopReason;
  let exitCode = null;
  let cleanupStatus = 'not-needed';
  let brokerId;
  const stop = async (reason) => {
    stopReason ??= reason;
    if (child) {
      // Removal targets only this unique client. Killing the Docker CLI alone leaves a container running.
      await docker(['rm', '-f', clientName]).catch(() => {});
      child.kill();
    }
  };
  const abort = () => {
    void stop('cancelled');
  };
  signal?.addEventListener('abort', abort, { once: true });
  try {
    const inspected = JSON.parse(await docker(['inspect', tool.container]))[0];
    verifyBroker(inspected, config, broker);
    brokerId = inspected.Id;
    manifest.imageId = JSON.parse(await docker(['image', 'inspect', tool.image]))[0].Id;
    // Resolve mutable tags once, so the recorded image is the one actually executed.
    const runConfig = { ...config, [broker]: { ...tool, image: manifest.imageId } };
    const helpArgs =
      broker === 'kafka' ? ['/opt/kafka/bin/kafka-producer-perf-test.sh', '--help'] : ['--help'];
    let help;
    try {
      help = await docker([
        'run',
        '--rm',
        '--pull=never',
        '--name',
        `${clientName}-help`,
        '--memory',
        '256m',
        '--cpus',
        '1',
        '--network',
        'none',
        manifest.imageId,
        ...helpArgs,
      ]);
    } catch (error) {
      // Installed Kafka exits1 and memtier exits2 for valid help. Accept only those signatures.
      help = `${error.stdout ?? ''}\n${error.stderr ?? ''}`;
      const validHelp =
        (broker === 'kafka' &&
          error.code === 1 &&
          help.includes('usage: kafka-producer-perf-test')) ||
        (broker === 'redis' &&
          error.code === 2 &&
          help.includes('Usage: memtier_benchmark [options]'));
      if (!validHelp) throw new Error('Tool help check failed');
    } finally {
      await docker(['rm', '-f', `${clientName}-help`]).catch(() => {});
    }
    const requiredFlags = {
      kafka: ['--num-records', '--warmup-records', '--command-property'],
      rabbitmq: ['--dynamic-batch-size', '--delete-streams', '--confirms'],
      redis: ['--command', '--test-time', '--pipeline', '--rate-limiting'],
    };
    if (!requiredFlags[broker].every((flag) => help.includes(flag)))
      throw new Error('Installed tool does not support required options');
    await fs.writeFile(path.join(folder, 'tool-help.txt'), help);
    await write('manifest.json', manifest);
    await checkDisk(config, broker, resultsRoot);
    await assertDestinationAbsent(config, broker, runId);
    if (signal?.aborted) throw new Error('cancelled');
    ownsDestination = true;
    if (broker === 'kafka') {
      await docker([
        'exec',
        tool.container,
        '/opt/kafka/bin/kafka-topics.sh',
        '--bootstrap-server',
        `${tool.host}:${tool.port}`,
        '--create',
        '--topic',
        runId,
        '--partitions',
        String(tool.partitions),
        '--replication-factor',
        '1',
      ]);
      await fs.writeFile(
        path.join(folder, 'topology.txt'),
        await docker([
          'exec',
          tool.container,
          '/opt/kafka/bin/kafka-topics.sh',
          '--bootstrap-server',
          `${tool.host}:${tool.port}`,
          '--describe',
          '--topic',
          runId,
        ]),
      );
    }
    if (signal?.aborted) throw new Error('cancelled');
    const stdout = await fs.open(path.join(folder, 'stdout.log'), 'w');
    const stderr = await fs.open(path.join(folder, 'stderr.log'), 'w');
    // File descriptors stream unbounded tool output to disk without retaining it in RAM.
    try {
      child = spawn('docker', buildCommand(runConfig, broker, runId, clientName, password), {
        stdio: ['ignore', stdout.fd, stderr.fd],
        windowsHide: true,
        shell: false,
      });
      timer = setTimeout(() => {
        void stop('safety-timeout');
      }, config.safety.timeoutSeconds * 1000);
      guard = setInterval(() => {
        if (monitoring) return;
        monitoring = true;
        monitorPromise = checkDisk(config, broker, resultsRoot)
          .catch((error) => stop(`disk-guard: ${error.message}`))
          .finally(() => {
            monitoring = false;
          });
      }, config.safety.pollSeconds * 1000);
      console.log(`Running ${broker}: ${runId}\nOutput: ${folder}`);
      exitCode = await new Promise((resolve, reject) => {
        child.once('error', reject);
        child.once('close', resolve);
      });
    } finally {
      clearTimeout(timer);
      clearInterval(guard);
      await monitorPromise;
      await stdout.close();
      await stderr.close();
    }
  } catch (error) {
    // Do not copy execFile errors: they can contain credential-bearing commands.
    stopReason ??= error.message.startsWith('Command failed')
      ? 'Docker operation failed; inspect broker readiness and logs.'
      : error.message;
  } finally {
    clearTimeout(timer);
    clearInterval(guard);
    signal?.removeEventListener('abort', abort);
    // Also covers cancellation races while docker run was starting.
    let clientStopped = true;
    if (child) {
      await docker(['rm', '-f', clientName]).catch(() => {});
      try {
        clientStopped = (await docker(['ps', '-aq', '--filter', `name=^/${clientName}$`])) === '';
      } catch {
        clientStopped = false;
      }
      if (!clientStopped)
        stopReason ??= 'Could not verify owned client stopped; data cleanup withheld';
    }
    if (ownsDestination && config.safety.cleanupOwnedData) {
      cleanupStatus = await cleanupOwned(config, broker, runId, brokerId, clientStopped);
    } else if (ownsDestination) cleanupStatus = 'retained-by-configuration';
  }
  const result = {
    runId,
    broker,
    endedAt: new Date().toISOString(),
    exitCode,
    stopReason,
    status: exitCode === 0 && !stopReason ? 'tool-exited-zero' : 'failed',
    cleanupStatus,
  };
  await write('result.json', result);
  console.log(JSON.stringify(result, null, 2));
  return { ...result, folder };
}

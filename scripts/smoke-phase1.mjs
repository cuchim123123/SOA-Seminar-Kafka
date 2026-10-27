import { spawn, exec } from 'node:child_process';
import { promisify } from 'node:util';

const execAsync = promisify(exec);

const PRODUCER_PORT = process.env.PORT || '3000';
const LOYALTY_PORT = process.env.LOYALTY_WORKER_PORT || '3001';
const TOPIC = process.env.PURCHASE_TOPIC || 'purchase.events';

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function isPortHealthy(url) {
  try {
    const res = await fetch(url);
    return res.ok;
  } catch {
    return false;
  }
}

async function ensureServiceRunning(name, url, startCmd, startArgs, envOverrides = {}) {
  const healthy = await isPortHealthy(url);
  if (healthy) {
    console.log(`[Smoke P1] ${name} is already running and healthy at ${url}`);
    return null;
  }

  console.log(`[Smoke P1] Starting ${name} in background...`);
  const child = spawn(startCmd, startArgs, {
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: true,
    env: { ...process.env, ...envOverrides },
  });

  child.stdout?.on('data', (data) => {
    const str = data.toString().trim();
    if (str.includes('[Nest]') || str.includes('listening')) {
      console.log(`[${name}] ${str}`);
    }
  });

  child.stderr?.on('data', (data) => {
    console.error(`[${name} Error] ${data.toString().trim()}`);
  });

  // Wait up to 15s for service to be healthy
  const startTime = Date.now();
  while (Date.now() - startTime < 15000) {
    if (await isPortHealthy(url)) {
      console.log(`[Smoke P1] ${name} became healthy at ${url}`);
      return child;
    }
    await sleep(500);
  }

  child.kill();
  throw new Error(`${name} failed to become healthy at ${url} within 15s`);
}

async function main() {
  const spawnedProcesses = [];

  try {
    console.log('=== Phase 1 Pipeline Smoke Test ===');

    // 1. Check Kafka container health
    console.log('[Smoke P1] Checking Kafka broker status...');
    const { stdout: containerStatus } = await execAsync(
      'docker inspect --format="{{json .State.Health.Status}}" kafka-broker-1',
    );
    if (!containerStatus.includes('healthy')) {
      throw new Error(`Kafka broker container is not healthy: ${containerStatus.trim()}`);
    }
    console.log('[Smoke P1] Kafka broker is healthy.');

    // 2. Ensure topic exists
    console.log(`[Smoke P1] Verifying topic '${TOPIC}' on Kafka...`);
    try {
      await execAsync(
        `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ${TOPIC}`,
      );
      console.log(`[Smoke P1] Topic '${TOPIC}' exists.`);
    } catch {
      console.log(`[Smoke P1] Creating topic '${TOPIC}'...`);
      await execAsync(
        `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ${TOPIC} --partitions 1 --replication-factor 1`,
      );
    }

    // 3. Ensure Producer API is running
    const producerChild = await ensureServiceRunning(
      'Producer-API',
      `http://localhost:${PRODUCER_PORT}/health/live`,
      'node',
      ['apps/producer-api/dist/main.js'],
      { PORT: PRODUCER_PORT },
    );
    if (producerChild) spawnedProcesses.push(producerChild);

    // 4. Ensure Loyalty Worker is running
    const loyaltyChild = await ensureServiceRunning(
      'Loyalty-Worker',
      `http://localhost:${LOYALTY_PORT}/health/live`,
      'node',
      ['apps/loyalty-worker/dist/main.js'],
      { PORT: LOYALTY_PORT, KAFKA_GROUP_ID: 'loyalty-v1' },
    );
    if (loyaltyChild) spawnedProcesses.push(loyaltyChild);

    // 5. Send POST /api/v1/demo/purchases request
    const testCustomerId = 'U001';
    const testAmountCents = 12000; // $120.00
    console.log(
      `[Smoke P1] Sending POST /api/v1/demo/purchases (customerId: ${testCustomerId}, amountCents: ${testAmountCents})...`,
    );

    const postResponse = await fetch(`http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        customerId: testCustomerId,
        amountCents: testAmountCents,
      }),
    });

    if (postResponse.status !== 202) {
      const errText = await postResponse.text();
      throw new Error(`Expected HTTP 202, received ${postResponse.status}: ${errText}`);
    }

    const responseBody = await postResponse.json();
    console.log('[Smoke P1] Received HTTP 202 Accepted response:');
    console.log(JSON.stringify(responseBody, null, 2));

    if (responseBody.requestStatus !== 'ACCEPTED') {
      throw new Error(`Expected requestStatus 'ACCEPTED', got '${responseBody.requestStatus}'`);
    }

    // 6. Inspect Kafka record using Kafka CLI
    console.log(
      `[Smoke P1] Verifying record on Kafka broker partition ${responseBody.partition} offset ${responseBody.offset}...`,
    );
    const { stdout: recordOutput } = await execAsync(
      `docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ${TOPIC} --partition ${responseBody.partition} --offset ${responseBody.offset} --max-messages 1 --property print.key=true --property key.separator=" | "`,
    );

    console.log(`[Smoke P1] Broker record output: ${recordOutput.trim()}`);
    if (!recordOutput.includes(testCustomerId)) {
      throw new Error(`Kafka record key does not contain expected customerId '${testCustomerId}'`);
    }

    // 7. Inspect Loyalty Consumer Group status on broker
    console.log('[Smoke P1] Inspecting consumer group offset progress on broker...');
    const { stdout: groupOutput } = await execAsync(
      'docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group loyalty-v1',
    );
    console.log('[Smoke P1] Consumer Group Details:\n' + groupOutput.trim());

    console.log('\n--- ALL PHASE 1 SMOKE CHECKS PASSED ---');
  } catch (err) {
    console.error('\n[Smoke P1 FAILED]:', err);
    process.exitCode = 1;
  } finally {
    for (const child of spawnedProcesses) {
      console.log(`[Smoke P1] Stopping spawned background process (pid: ${child.pid})...`);
      if (process.platform === 'win32' && child.pid) {
        try {
          await execAsync(`taskkill /pid ${child.pid} /T /F`);
        } catch {
          child.kill('SIGKILL');
        }
      } else {
        child.kill('SIGTERM');
      }
    }
    process.exit(process.exitCode || 0);
  }
}

main();

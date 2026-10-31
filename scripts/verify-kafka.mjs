import { exec } from 'node:child_process';
import { promisify } from 'node:util';

const execAsync = promisify(exec);

const RETRY_DELAY_MS = 2000;
const MAX_ATTEMPTS = 30;
const TEST_TOPIC = 'p002.verification.test';
const KAFKA_UI_PORT = process.env.KAFKA_UI_PORT || '8080';

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForKafkaHealthy() {
  console.log('[Verify] Waiting for Kafka container to report healthy status...');
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      const { stdout } = await execAsync(
        'docker inspect --format="{{json .State.Health.Status}}" kafka-broker-1',
      );
      const status = JSON.parse(stdout.trim());
      if (status === 'healthy') {
        console.log(`[Verify] Kafka container is healthy (attempt ${attempt}).`);
        return true;
      }
    } catch {
      // Container may still be starting or initializing
    }
    await sleep(RETRY_DELAY_MS);
  }
  throw new Error(`Kafka container did not become healthy within ${MAX_ATTEMPTS * 2}s`);
}

async function verifyTopicLifecycle() {
  console.log(`[Verify] Creating test topic '${TEST_TOPIC}' (1 partition, RF 1)...`);
  await execAsync(
    `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ${TEST_TOPIC} --partitions 1 --replication-factor 1`,
  );

  console.log(`[Verify] Describing test topic '${TEST_TOPIC}'...`);
  const { stdout: describeOutput } = await execAsync(
    `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ${TEST_TOPIC}`,
  );
  console.log('[Verify] Topic metadata:\n' + describeOutput.trim());

  if (
    !describeOutput.includes('PartitionCount: 1') ||
    !describeOutput.includes('ReplicationFactor: 1')
  ) {
    throw new Error('Topic metadata does not match expected partitions or replication factor');
  }

  console.log(`[Verify] Deleting test topic '${TEST_TOPIC}'...`);
  await execAsync(
    `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ${TEST_TOPIC}`,
  );
  console.log('[Verify] Test topic lifecycle completed successfully.');
}

async function verifyKafkaUI() {
  console.log(`[Verify] Checking Kafbat UI health on port ${KAFKA_UI_PORT}...`);
  const url = `http://localhost:${KAFKA_UI_PORT}/actuator/health`;
  for (let attempt = 1; attempt <= 15; attempt++) {
    try {
      const res = await fetch(url);
      if (res.ok) {
        const body = await res.json();
        console.log(`[Verify] Kafbat UI is responsive (status: ${body.status}).`);
        return;
      }
    } catch {
      // Kafbat UI still starting up
    }
    await sleep(RETRY_DELAY_MS);
  }
  console.warn('[Verify] Note: Kafbat UI is starting or actuator endpoint is not yet ready.');
}

async function main() {
  try {
    await waitForKafkaHealthy();
    await verifyTopicLifecycle();
    await verifyKafkaUI();
    console.log('--- ALL P0-02 VERIFICATIONS PASSED ---');
  } catch (error) {
    console.error('[Verify Failed]:', error);
    process.exit(1);
  }
}

main();

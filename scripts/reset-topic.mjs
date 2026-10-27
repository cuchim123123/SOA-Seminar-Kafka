import { exec } from 'node:child_process';
import { promisify } from 'node:util';

const execAsync = promisify(exec);

const TOPIC = process.env.PURCHASE_TOPIC || 'purchase.events';
const PARTITIONS = parseInt(process.env.PURCHASE_TOPIC_PARTITIONS || '6', 10);
const REPLICATION_FACTOR = parseInt(process.env.PURCHASE_TOPIC_RF || '1', 10);

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function verifyKafkaHealthy() {
  const { stdout } = await execAsync(
    'docker inspect --format="{{json .State.Health.Status}}" kafka-broker-1',
  );
  if (!stdout.includes('healthy')) {
    throw new Error(
      'Kafka container is not healthy. Start infrastructure with `pnpm kafka:up` first.',
    );
  }
}

async function topicExists(topicName) {
  try {
    const { stdout } = await execAsync(
      `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`,
    );
    const topics = stdout.split(/\r?\n/).map((t) => t.trim());
    return topics.includes(topicName);
  } catch {
    return false;
  }
}

async function deleteTopic(topicName) {
  console.log(`[Reset Topic] Deleting existing topic '${topicName}'...`);
  await execAsync(
    `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ${topicName}`,
  );

  // Poll until deletion is propagated in KRaft metadata
  for (let attempt = 1; attempt <= 15; attempt++) {
    const exists = await topicExists(topicName);
    if (!exists) {
      console.log(`[Reset Topic] Topic '${topicName}' successfully deleted.`);
      return;
    }
    await sleep(1000);
  }
  throw new Error(`Timeout waiting for topic '${topicName}' deletion to propagate.`);
}

async function createTopic(topicName, numPartitions, rf) {
  console.log(
    `[Reset Topic] Creating fresh topic '${topicName}' with ${numPartitions} partitions (RF=${rf})...`,
  );
  await execAsync(
    `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ${topicName} --partitions ${numPartitions} --replication-factor ${rf}`,
  );
}

async function describeTopic(topicName) {
  const { stdout } = await execAsync(
    `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ${topicName}`,
  );
  return stdout.trim();
}

async function main() {
  const args = process.argv.slice(2);
  const isConfirmed = args.includes('--confirm') || process.env.CONFIRM_RESET === 'true';

  console.log('=== Kafka Topic Reset & Migration Tool (P2-01) ===');

  if (!isConfirmed) {
    console.error(`
[SAFETY GUARD]: DESTRUCTIVE OPERATION REFUSED!
---------------------------------------------------------------------------------
Resetting topic '${TOPIC}' will permanently erase all retained events and committed
offsets on your local Kafka broker.

To proceed with this destructive reset, explicitly supply the '--confirm' flag:
  pnpm demo:reset -- --confirm
or set environment variable:
  CONFIRM_RESET=true pnpm demo:reset
---------------------------------------------------------------------------------
`);
    process.exit(1);
  }

  try {
    await verifyKafkaHealthy();

    const exists = await topicExists(TOPIC);
    if (exists) {
      await deleteTopic(TOPIC);
    } else {
      console.log(`[Reset Topic] Topic '${TOPIC}' does not exist yet. Proceeding to creation.`);
    }

    await createTopic(TOPIC, PARTITIONS, REPLICATION_FACTOR);

    const description = await describeTopic(TOPIC);
    console.log('[Reset Topic] Verified Topic Metadata:\n' + description);

    if (!description.includes(`PartitionCount: ${PARTITIONS}`)) {
      throw new Error(`Expected PartitionCount: ${PARTITIONS}, but got:\n${description}`);
    }

    console.log(
      `\n[SUCCESS] Topic '${TOPIC}' successfully created with ${PARTITIONS} partitions. Ready for Phase 2 ordered workload.`,
    );
  } catch (err) {
    console.error('\n[Reset Topic FAILED]:', err.message || err);
    process.exit(1);
  }
}

main();

import { spawn } from 'node:child_process';
import { Kafka } from 'kafkajs';

const PRODUCER_PORT = process.env.PORT || '3000';
const LOYALTY_PORT = process.env.LOYALTY_WORKER_PORT || '3001';
const ANALYTICS_PORT = process.env.ANALYTICS_WORKER_PORT || '3002';
const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const TOPIC = process.env.PURCHASE_TOPIC || 'purchase.events';
const LOYALTY_GROUP = process.env.KAFKA_LOYALTY_GROUP_ID || 'loyalty-v1';
const ANALYTICS_GROUP = process.env.KAFKA_ANALYTICS_GROUP_ID || 'analytics-v1';

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
    console.log(`[P3-02 Proof] ${name} is already healthy at ${url}`);
    return null;
  }

  console.log(`[P3-02 Proof] Starting ${name} in background...`);
  const child = spawn(startCmd, startArgs, {
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: true,
    env: { ...process.env, ...envOverrides },
  });

  const startTime = Date.now();
  while (Date.now() - startTime < 20000) {
    if (await isPortHealthy(url)) {
      console.log(`[P3-02 Proof] ${name} became healthy at ${url}`);
      return child;
    }
    await sleep(500);
  }

  child.kill();
  throw new Error(`${name} failed to become healthy at ${url} within 20s`);
}

async function main() {
  console.log('='.repeat(75));
  console.log('  PHASE 3 TICKET P3-02: MANUAL COMMIT (N+1) & ADMIN LAG READER PROOF');
  console.log('='.repeat(75));

  // 1. Check Kafka broker connection
  const kafka = new Kafka({ clientId: 'p3-02-verifier', brokers: BROKERS });
  const admin = kafka.admin();
  try {
    await admin.connect();
    console.log(`[P3-02 Proof] Connected to Kafka brokers: ${BROKERS.join(', ')}`);
  } catch (err) {
    console.error(`[P3-02 Proof] Cannot connect to Kafka:`, err.message);
    process.exit(1);
  }

  const spawned = [];
  try {
    // 2. Ensure Producer API and Workers are running
    const producerChild = await ensureServiceRunning(
      'producer-api',
      `http://localhost:${PRODUCER_PORT}/health/ready`,
      'node',
      ['apps/producer-api/dist/main.js'],
      { PORT: PRODUCER_PORT },
    );
    if (producerChild) spawned.push(producerChild);

    const loyaltyChild = await ensureServiceRunning(
      'loyalty-worker',
      `http://localhost:${LOYALTY_PORT}/health/ready`,
      'node',
      ['apps/loyalty-worker/dist/main.js'],
      { PORT: LOYALTY_PORT, KAFKA_GROUP_ID: LOYALTY_GROUP },
    );
    if (loyaltyChild) spawned.push(loyaltyChild);

    const analyticsChild = await ensureServiceRunning(
      'analytics-worker',
      `http://localhost:${ANALYTICS_PORT}/health/ready`,
      'node',
      ['apps/analytics-worker/dist/main.js'],
      { PORT: ANALYTICS_PORT, KAFKA_GROUP_ID: ANALYTICS_GROUP },
    );
    if (analyticsChild) spawned.push(analyticsChild);

    // Wait for group partitions to stabilize
    await sleep(2500);

    // ─────────────────────────────────────────────────────────────────────────
    // PART A: PROOF OF UNCOMMITTED HANDLING
    // ─────────────────────────────────────────────────────────────────────────
    console.log('\n--- PART A: Verifying Lag Reader on Fresh Uncommitted Group ---');
    const freshGroupId = `proof-uncommitted-${Date.now()}`;
    const uncommittedRes = await fetch(
      `http://localhost:${PRODUCER_PORT}/api/v1/ops/kafka/groups/${freshGroupId}/lag?topic=${TOPIC}`,
    );

    if (!uncommittedRes.ok) {
      throw new Error(`Failed to query lag for uncommitted group: ${uncommittedRes.statusText}`);
    }

    const uncommittedData = await uncommittedRes.json();
    console.log(`Group ID: ${uncommittedData.groupId}`);
    console.log(`Total Lag: ${uncommittedData.totalLag}`);
    console.log(`Partitions Count: ${uncommittedData.partitions.length}`);

    const hasExplicitUncommitted = uncommittedData.partitions.every(
      (p) => p.isUncommitted === true && p.lag === 'UNCOMMITTED' && p.committedOffset === null,
    );

    if (hasExplicitUncommitted) {
      console.log(
        '  >>> SUCCESS: Fresh consumer group correctly reports "UNCOMMITTED" with null offset (not fake zero). <<<',
      );
    } else {
      console.warn('  >>> WARN: Unexpected uncommitted lag payload:', uncommittedData);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PART B: PROOF OF MANUAL COMMIT N + 1 DISCIPLINE
    // ─────────────────────────────────────────────────────────────────────────
    console.log('\n--- PART B: Publishing Workload to Verify Manual Commit N + 1 ---');
    const beforeOffsets = await admin.fetchTopicOffsets(TOPIC);

    const customerId = `P3_CUST_${Date.now()}`;
    const purchasePayload = { customerId, amountCents: 12500 };

    const produceRes = await fetch(`http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(purchasePayload),
    });

    if (!produceRes.ok) {
      throw new Error(`Failed to post purchase: ${produceRes.statusText}`);
    }

    const produceBody = await produceRes.json();
    console.log(
      `  -> Accepted purchaseId=${produceBody.purchaseId}, eventId=${produceBody.eventId}`,
    );

    // Allow outbox relay to publish to Kafka and consumers to commit
    console.log('  Waiting 3s for outbox relay to publish and consumers to commit N + 1...');
    await sleep(3000);

    const afterOffsets = await admin.fetchTopicOffsets(TOPIC);
    let targetPartition = -1;
    let consumedOffset = '0';
    for (const after of afterOffsets) {
      const before = beforeOffsets.find((b) => b.partition === after.partition);
      if (before && BigInt(after.offset) > BigInt(before.offset)) {
        targetPartition = after.partition;
        consumedOffset = (BigInt(after.offset) - 1n).toString();
        break;
      }
    }

    if (targetPartition === -1) {
      throw new Error('Could not identify published record partition from topic offsets');
    }

    const expectedCommittedOffset = (BigInt(consumedOffset) + 1n).toString();

    console.log(
      `  -> Published record to Topic='${TOPIC}', Partition=${targetPartition}, ConsumedOffset=${consumedOffset}`,
    );
    console.log(`  -> Expected Committed Offset (N + 1): ${expectedCommittedOffset}`);

    // ─────────────────────────────────────────────────────────────────────────
    // PART C: INSPECT COMMITTED OFFSETS & LAG VIA OPS API
    // ─────────────────────────────────────────────────────────────────────────
    console.log('\n--- PART C: Live Lag Reader Inspection via Ops API ---');
    const loyaltyLagRes = await fetch(
      `http://localhost:${PRODUCER_PORT}/api/v1/ops/kafka/groups/${LOYALTY_GROUP}/lag?topic=${TOPIC}`,
    );
    const analyticsLagRes = await fetch(
      `http://localhost:${PRODUCER_PORT}/api/v1/ops/kafka/groups/${ANALYTICS_GROUP}/lag?topic=${TOPIC}`,
    );

    const loyaltyLag = await loyaltyLagRes.json();
    const analyticsLag = await analyticsLagRes.json();

    console.log(`\n1. Group: ${LOYALTY_GROUP} (Loyalty Worker):`);
    console.table(
      loyaltyLag.partitions.map((p) => ({
        Partition: p.partition,
        'Log Start': p.logStartOffset,
        'Log End (High)': p.logEndOffset,
        'Committed (Next)': p.committedOffset ?? 'UNCOMMITTED',
        Lag: p.lag,
        Status: p.isUncommitted ? 'UNCOMMITTED' : 'COMMITTED',
      })),
    );

    console.log(`\n2. Group: ${ANALYTICS_GROUP} (Analytics Worker):`);
    console.table(
      analyticsLag.partitions.map((p) => ({
        Partition: p.partition,
        'Log Start': p.logStartOffset,
        'Log End (High)': p.logEndOffset,
        'Committed (Next)': p.committedOffset ?? 'UNCOMMITTED',
        Lag: p.lag,
        Status: p.isUncommitted ? 'UNCOMMITTED' : 'COMMITTED',
      })),
    );

    // ─────────────────────────────────────────────────────────────────────────
    // PART D: ASSERTIONS
    // ─────────────────────────────────────────────────────────────────────────
    const loyaltyTarget = loyaltyLag.partitions.find((p) => p.partition === targetPartition);
    const analyticsTarget = analyticsLag.partitions.find((p) => p.partition === targetPartition);

    const loyaltyCommitOk =
      loyaltyTarget && loyaltyTarget.committedOffset === expectedCommittedOffset;
    const analyticsCommitOk =
      analyticsTarget && analyticsTarget.committedOffset === expectedCommittedOffset;

    console.log('\n' + '='.repeat(75));
    console.log('  TICKET P3-02 EVIDENCE VERIFICATION SUMMARY');
    console.log('='.repeat(75));
    console.log(`Target Partition                  : ${targetPartition}`);
    console.log(`Message Consumed Offset (N)       : ${consumedOffset}`);
    console.log(`Expected Next Committed Offset    : ${expectedCommittedOffset}`);
    console.log(`Loyalty Group Committed Offset    : ${loyaltyTarget?.committedOffset}`);
    console.log(`Analytics Group Committed Offset  : ${analyticsTarget?.committedOffset}`);
    console.log(`Loyalty Group Lag on Partition    : ${loyaltyTarget?.lag}`);
    console.log(`Analytics Group Lag on Partition  : ${analyticsTarget?.lag}`);

    if (loyaltyCommitOk && analyticsCommitOk && hasExplicitUncommitted) {
      console.log(
        '\n  >>> PROOF CONFIRMED: Manual offset commit (N + 1) and UNCOMMITTED lag reader verified! <<<',
      );
    } else {
      console.error(
        '\n  >>> PROOF FAILED: Offset commit or lag assertion did not match expectations. <<<',
      );
      process.exitCode = 1;
    }
  } finally {
    await admin.disconnect();
    for (const child of spawned) {
      child.kill();
    }
    process.exit(process.exitCode ?? 0);
  }
}

main().catch((err) => {
  console.error('[P3-02 Fatal Error]:', err);
  process.exit(1);
});

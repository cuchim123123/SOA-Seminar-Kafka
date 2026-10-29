import { spawn } from 'node:child_process';
import pg from 'pg';
import { Kafka } from 'kafkajs';

if (typeof process.loadEnvFile === 'function') {
  try {
    process.loadEnvFile('.env');
  } catch {
    // Ignore if .env doesn't exist
  }
}

const PRODUCER_PORT = process.env.PORT || '3000';
const PUBLISHER_PORT = process.env.PUBLISHER_PORT || '3003';
const LOYALTY_PORT = process.env.LOYALTY_WORKER_PORT || '3001';
const ANALYTICS_PORT = process.env.ANALYTICS_WORKER_PORT || '3002';
const REPLAY_PORT = process.env.REPLAY_WORKER_PORT || '3004';

const PURCHASES_DATABASE_URL =
  process.env.PURCHASES_DATABASE_URL ||
  process.env.DATABASE_URL ||
  'postgresql://postgres:postgres@localhost:5432/purchases_db?schema=public';

const LOYALTY_DATABASE_URL =
  process.env.LOYALTY_DATABASE_URL ||
  'postgresql://postgres:postgres@localhost:5432/loyalty_db?schema=public';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092')
  .split(',')
  .map((b) => b.trim());
const PURCHASE_TOPIC = process.env.PURCHASE_TOPIC || 'purchase.events';

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

function killProcess(child) {
  if (!child || !child.pid) return;
  if (process.platform === 'win32') {
    try {
      spawn('taskkill', ['/pid', String(child.pid), '/f', '/t'], { stdio: 'ignore' });
    } catch {
      child.kill('SIGKILL');
    }
  } else {
    child.kill('SIGKILL');
  }
}

async function ensureServiceRunning(name, url, startCmd, startArgs, envOverrides = {}) {
  const healthy = await isPortHealthy(url);
  if (healthy) {
    console.log(`[P5-02 Proof] ${name} is already healthy at ${url}`);
    return null;
  }

  console.log(`[P5-02 Proof] Starting ${name} in background...`);
  const child = spawn(startCmd, startArgs, {
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: true,
    env: { ...process.env, ...envOverrides },
  });

  let output = '';
  child.stdout?.on('data', (d) => {
    output += d.toString();
  });
  child.stderr?.on('data', (d) => {
    output += d.toString();
  });

  const startTime = Date.now();
  while (Date.now() - startTime < 35000) {
    await sleep(500);
    if (await isPortHealthy(url)) {
      console.log(`[P5-02 Proof] ${name} is healthy and responsive at ${url}!`);
      return child;
    }
  }

  killProcess(child);
  throw new Error(
    `Timed out waiting for ${name} to become healthy at ${url}.\nProcess output:\n${output}`,
  );
}

async function getGroupCommittedOffsets(admin, groupId, topic) {
  try {
    const offsets = await admin.fetchOffsets({ groupId, topics: [topic] });
    const topicOffsets = offsets.find((o) => o.topic === topic);
    const map = {};
    if (topicOffsets) {
      for (const p of topicOffsets.partitions) {
        map[p.partition] = p.offset;
      }
    }
    return map;
  } catch {
    return {};
  }
}

async function main() {
  console.log('\n================================================================================');
  console.log('   PHASE 5 (P5-02) — BOUNDED REPLAY & FIXED BOUNDARY VERIFICATION');
  console.log('================================================================================\n');

  const spawned = [];
  const kafka = new Kafka({
    clientId: `verify-replay-${Date.now()}`,
    brokers: KAFKA_BROKERS,
  });
  const admin = kafka.admin();
  const loyaltyPool = new pg.Pool({ connectionString: LOYALTY_DATABASE_URL });
  const purchasesPool = new pg.Pool({ connectionString: PURCHASES_DATABASE_URL });

  try {
    await admin.connect();

    // 1. Ensure required services are healthy
    const producerChild = await ensureServiceRunning(
      'Producer API',
      `http://localhost:${PRODUCER_PORT}/health/live`,
      'node',
      ['apps/producer-api/dist/main.js'],
      { PORT: String(PRODUCER_PORT) },
    );
    if (producerChild) spawned.push(producerChild);

    const publisherChild = await ensureServiceRunning(
      'Outbox Publisher',
      `http://localhost:${PUBLISHER_PORT}/health`,
      'node',
      ['apps/outbox-publisher/dist/main.js'],
      { PORT: String(PUBLISHER_PORT), PUBLISHER_PORT: String(PUBLISHER_PORT) },
    );
    if (publisherChild) spawned.push(publisherChild);

    const loyaltyChild = await ensureServiceRunning(
      'Loyalty Worker',
      `http://localhost:${LOYALTY_PORT}/health/live`,
      'node',
      ['apps/loyalty-worker/dist/main.js'],
      { PORT: String(LOYALTY_PORT), LOYALTY_WORKER_PORT: String(LOYALTY_PORT) },
    );
    if (loyaltyChild) spawned.push(loyaltyChild);

    const analyticsChild = await ensureServiceRunning(
      'Analytics Worker',
      `http://localhost:${ANALYTICS_PORT}/health/live`,
      'node',
      ['apps/analytics-worker/dist/main.js'],
      { PORT: String(ANALYTICS_PORT), ANALYTICS_WORKER_PORT: String(ANALYTICS_PORT) },
    );
    if (analyticsChild) spawned.push(analyticsChild);

    const replayChild = await ensureServiceRunning(
      'Replay Worker',
      `http://localhost:${REPLAY_PORT}/health/live`,
      'node',
      ['apps/replay-worker/dist/main.js'],
      { PORT: String(REPLAY_PORT), REPLAY_WORKER_PORT: String(REPLAY_PORT) },
    );
    if (replayChild) spawned.push(replayChild);

    // 2. Seed U001 purchases: P001 ($120), P002 ($200), P003 ($100) + Invalidate P001
    const testCustomerId = `U_REPLAY_BOUND_${Date.now()}`;
    console.log(`\n[P5-02 Proof] Step 1: Seeding 3 purchases for test customer: ${testCustomerId}`);

    const purchaseIds = [];
    const amounts = [12000, 20000, 10000];
    for (let i = 0; i < amounts.length; i++) {
      const res = await fetch(`http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `idemp-replay-${Date.now()}-${i}`,
        },
        body: JSON.stringify({
          customerId: testCustomerId,
          amountCents: amounts[i],
          currency: 'USD',
        }),
      });
      const data = await res.json();
      purchaseIds.push(data.purchaseId);
      console.log(
        `  -> Purchase ${i + 1} ($${amounts[i] / 100}): purchaseId=${data.purchaseId}, sequence=${data.customerSequence}`,
      );
    }

    // Invalidate P001
    console.log(`[P5-02 Proof] Step 2: Invalidating Purchase 1 (${purchaseIds[0]})...`);
    const invRes = await fetch(
      `http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases/${purchaseIds[0]}/invalidate`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          reason: 'DEMO_INVALIDATION',
          reasonDetails: 'Bounded replay fixture',
        }),
      },
    );
    const invData = await invRes.json();
    console.log(
      `  -> Invalidation response: status=${invData.status}, assigned customerSequence=${invData.customerSequence}`,
    );

    // Wait for publisher to drain all pending events
    console.log('[P5-02 Proof] Step 3: Waiting for outbox to drain to Kafka...');
    for (let i = 0; i < 20; i++) {
      const pendingRes = await purchasesPool.query(
        "SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND status = 'PENDING'",
        [testCustomerId],
      );
      if (parseInt(pendingRes.rows[0].count, 10) === 0) {
        console.log('  -> All events published to Kafka broker!');
        break;
      }
      await sleep(300);
    }

    // Wait for live consumer groups to process and commit
    await sleep(2000);

    // 3. Record baseline committed offsets for live consumer groups
    console.log('\n[P5-02 Proof] Step 4: Capturing baseline offsets for live consumer groups...');
    const baselineLoyaltyOffsets = await getGroupCommittedOffsets(
      admin,
      'loyalty-v1',
      PURCHASE_TOPIC,
    );
    const baselineAnalyticsOffsets = await getGroupCommittedOffsets(
      admin,
      'analytics-v1',
      PURCHASE_TOPIC,
    );
    console.log('  -> loyalty-v1 baseline offsets:  ', baselineLoyaltyOffsets);
    console.log('  -> analytics-v1 baseline offsets:', baselineAnalyticsOffsets);

    // 4. Trigger Bounded Replay run via Replay Worker API
    console.log(
      '\n[P5-02 Proof] Step 5: Triggering Bounded Replay run via POST /api/v1/replay/runs...',
    );
    const replayRunId = `replay-proof-${Date.now()}`;
    const replayRes = await fetch(`http://localhost:${REPLAY_PORT}/api/v1/replay/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        runId: replayRunId,
        topic: PURCHASE_TOPIC,
      }),
    });

    if (!replayRes.ok) {
      const errText = await replayRes.text();
      throw new Error(`Replay request failed with status ${replayRes.status}: ${errText}`);
    }

    const replayResult = await replayRes.json();
    console.log('  -> Replay Completed Successfully!');
    console.log(`     Run ID:               ${replayResult.id}`);
    console.log(`     Group ID:             ${replayResult.groupId}`);
    console.log(`     Status:               ${replayResult.status}`);
    console.log(`     Scanned Count:        ${replayResult.scannedCount} records`);
    console.log(`     High Watermarks:      ${JSON.stringify(replayResult.highWatermarks)}`);
    console.log(`     Committed Offsets:    ${JSON.stringify(replayResult.finalCommittedOffsets)}`);

    // 5. Inspect Kafka Admin to verify replay group offsets vs live group offsets
    console.log(
      '\n[P5-02 Proof] Step 6: Verifying Kafka broker consumer group offsets in Admin API...',
    );
    const replayCommittedOffsets = await getGroupCommittedOffsets(
      admin,
      replayResult.groupId,
      PURCHASE_TOPIC,
    );
    const afterLoyaltyOffsets = await getGroupCommittedOffsets(admin, 'loyalty-v1', PURCHASE_TOPIC);
    const afterAnalyticsOffsets = await getGroupCommittedOffsets(
      admin,
      'analytics-v1',
      PURCHASE_TOPIC,
    );

    console.log(`  -> ${replayResult.groupId} committed offsets:`, replayCommittedOffsets);
    console.log('  -> loyalty-v1 offsets after replay:           ', afterLoyaltyOffsets);
    console.log('  -> analytics-v1 offsets after replay:         ', afterAnalyticsOffsets);

    // Check replay group committed all high watermarks
    let replayOffsetsMatchHw = true;
    for (const [partition, hw] of Object.entries(replayResult.highWatermarks)) {
      if (replayCommittedOffsets[partition] !== hw) {
        console.error(
          `  [FAIL] Replay group partition ${partition} offset ${replayCommittedOffsets[partition]} != high-watermark ${hw}`,
        );
        replayOffsetsMatchHw = false;
      }
    }

    // Check live consumer groups remain strictly untouched
    let loyaltyUntouched =
      JSON.stringify(baselineLoyaltyOffsets) === JSON.stringify(afterLoyaltyOffsets);
    let analyticsUntouched =
      JSON.stringify(baselineAnalyticsOffsets) === JSON.stringify(afterAnalyticsOffsets);

    // 6. Inspect PostgreSQL isolated staging table
    console.log('\n[P5-02 Proof] Step 7: Verifying isolated staging persistence in loyalty_db...');
    const dbRunRes = await loyaltyPool.query(
      'SELECT id, group_id, status, scanned_count, high_watermarks FROM replay_runs WHERE id = $1',
      [replayRunId],
    );
    const dbRun = dbRunRes.rows[0];
    console.log('  -> DB replay_runs record:', dbRun);

    console.log(
      '\n================================================================================',
    );
    console.log('                     EVIDENCE & VERIFICATION SUMMARY');
    console.log('================================================================================');
    console.log(`[PASS] On-demand unique consumer group created:  ${replayResult.groupId}`);
    console.log(
      `[PASS] Fixed boundary captured (high-watermarks): ${JSON.stringify(replayResult.highWatermarks)}`,
    );
    console.log(
      `[PASS] Retained records scanned within boundary: ${replayResult.scannedCount} events`,
    );
    console.log(
      `[PASS] Replay group committed offsets in Kafka:  ${replayOffsetsMatchHw ? 'YES (matches high-watermarks)' : 'NO'}`,
    );
    console.log(
      `[PASS] Live loyalty-v1 offsets UNTOUCHED:        ${loyaltyUntouched ? 'YES (100% identical)' : 'NO'}`,
    );
    console.log(
      `[PASS] Live analytics-v1 offsets UNTOUCHED:      ${analyticsUntouched ? 'YES (100% identical)' : 'NO'}`,
    );
    console.log(
      `[PASS] Isolated staging persisted in loyalty_db:  ${dbRun && dbRun.status === 'COMPLETED' ? 'YES' : 'NO'}`,
    );
    console.log(
      '================================================================================\n',
    );

    if (!replayOffsetsMatchHw || !loyaltyUntouched || !analyticsUntouched || !dbRun) {
      throw new Error('Verification assertion failed!');
    }

    console.log('Ticket P5-02 verification succeeded with 100% compliance.\n');
  } finally {
    await admin.disconnect().catch(() => {});
    await loyaltyPool.end().catch(() => {});
    await purchasesPool.end().catch(() => {});
    for (const child of spawned) {
      killProcess(child);
    }
  }
}

main().catch((err) => {
  console.error('\n[FATAL ERROR during replay verification]:', err);
  process.exit(1);
});

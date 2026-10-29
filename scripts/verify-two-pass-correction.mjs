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
    console.log(`[P5-03 Proof] ${name} is already healthy at ${url}`);
    return null;
  }

  console.log(`[P5-03 Proof] Starting ${name} in background...`);
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
      console.log(`[P5-03 Proof] ${name} is healthy and responsive at ${url}!`);
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
  console.log('   PHASE 5 (P5-03) — TWO-PASS CORRECTION CALCULATION (82 -> 40 POINTS) PROOF');
  console.log('================================================================================\n');

  const spawned = [];
  const kafka = new Kafka({
    clientId: `verify-correction-${Date.now()}`,
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

    // 2. Seed U001 purchases: P001 ($120), P002 ($200), P003 ($100)
    const testCustomerId = `U_P503_${Date.now()}`;
    console.log(
      `\n[P5-03 Proof] Step 1: Seeding canonical 3 purchases for test customer: ${testCustomerId}`,
    );

    const purchaseIds = [];
    const amounts = [12000, 20000, 10000];
    for (let i = 0; i < amounts.length; i++) {
      const res = await fetch(`http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `idemp-p503-${Date.now()}-${i}`,
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

    // Step 2: Invalidate P001 ($120)
    console.log(
      `\n[P5-03 Proof] Step 2: Invalidating Purchase 1 ($120, ${purchaseIds[0]}) via Producer API...`,
    );
    const invRes = await fetch(
      `http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases/${purchaseIds[0]}/invalidate`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          reason: 'DEMO_INVALIDATION',
        }),
      },
    );
    const invData = await invRes.json();
    console.log(
      `  -> Invalidation response: status=${invData.status}, assigned customerSequence=${invData.customerSequence}`,
    );

    // Step 3: Wait for outbox publisher to drain all pending events
    console.log('\n[P5-03 Proof] Step 3: Waiting for outbox publisher to drain events to Kafka...');
    for (let i = 0; i < 20; i++) {
      const pendingRes = await purchasesPool.query(
        "SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND status = 'PENDING'",
        [testCustomerId],
      );
      if (parseInt(pendingRes.rows[0].count, 10) === 0) {
        console.log('  -> All 4 events published to Kafka broker!');
        break;
      }
      await sleep(300);
    }

    // Wait for live loyalty worker to process all 4 events
    console.log('[P5-03 Proof] Step 4: Waiting for live loyalty worker to process events...');
    let liveAccount = null;
    for (let i = 0; i < 25; i++) {
      const dbLoyaltyRes = await loyaltyPool.query(
        'SELECT customer_id, points, tier, eligible_spend_cents, last_customer_sequence, correction_required FROM loyalty_accounts WHERE customer_id = $1',
        [testCustomerId],
      );
      if (
        dbLoyaltyRes.rows.length > 0 &&
        parseInt(dbLoyaltyRes.rows[0].last_customer_sequence, 10) >= 4 &&
        dbLoyaltyRes.rows[0].correction_required === true
      ) {
        liveAccount = dbLoyaltyRes.rows[0];
        break;
      }
      await sleep(300);
    }

    if (!liveAccount) {
      throw new Error(
        `Live loyalty worker did not complete processing sequence 4 for customer ${testCustomerId}`,
      );
    }

    const livePoints = parseInt(liveAccount.points, 10);
    const liveSpend = parseInt(liveAccount.eligible_spend_cents, 10);

    console.log('  -> Live Loyalty Projection:');
    console.log(`     Points Balance:       ${livePoints} points`);
    console.log(`     Tier:                 ${liveAccount.tier}`);
    console.log(`     Eligible Spend:       $${liveSpend / 100}`);
    console.log(`     Last Sequence:        ${liveAccount.last_customer_sequence}`);
    console.log(`     Correction Required:  ${liveAccount.correction_required}`);

    // Capture baseline committed offsets for live groups
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

    // Step 5: Execute Bounded Replay run
    console.log(
      '\n[P5-03 Proof] Step 5: Executing Bounded Replay with two-pass correction via Replay Worker...',
    );
    const runId = `corr-proof-${Date.now()}`;
    const replayRes = await fetch(`http://localhost:${REPLAY_PORT}/api/v1/replay/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        runId,
        topic: PURCHASE_TOPIC,
      }),
    });

    if (!replayRes.ok) {
      const errText = await replayRes.text();
      throw new Error(`Replay request failed with status ${replayRes.status}: ${errText}`);
    }

    const replayData = await replayRes.json();
    console.log('  -> Replay Completed!');
    console.log(`     Run ID:               ${replayData.id}`);
    console.log(`     Status:               ${replayData.status}`);
    console.log(`     Scanned Count:        ${replayData.scannedCount} events`);

    // Step 6: Verify Isolated Staging table in loyalty_db
    console.log(
      '\n[P5-03 Proof] Step 6: Querying isolated replay staging (GET /api/v1/replay/runs/:runId/staging)...',
    );
    const stagingRes = await fetch(
      `http://localhost:${REPLAY_PORT}/api/v1/replay/runs/${runId}/staging`,
    );
    const stagingRecords = await stagingRes.json();
    const stagedRecord = stagingRecords.find((r) => r.customerId === testCustomerId);

    if (!stagedRecord) {
      throw new Error(`Customer ${testCustomerId} not found in replay staging for run ${runId}`);
    }

    console.log('  -> Staged Customer Recomputed Projection:');
    console.log(`     Recomputed Points:    ${stagedRecord.points} points`);
    console.log(`     Recomputed Tier:      ${stagedRecord.tier}`);
    console.log(`     Recomputed Spend:     $${stagedRecord.eligibleSpendCents / 100}`);
    console.log(`     Last Sequence:        ${stagedRecord.lastCustomerSequence}`);

    // Step 7: Query Comparison API
    console.log(
      '\n[P5-03 Proof] Step 7: Querying Comparison API (GET /api/v1/replay/runs/:runId/comparison/:customerId)...',
    );
    const compRes = await fetch(
      `http://localhost:${REPLAY_PORT}/api/v1/replay/runs/${runId}/comparison/${testCustomerId}`,
    );
    const comparison = await compRes.json();

    console.log('  -> Comparison Result:');
    console.log(`     Customer:             ${comparison.customerId}`);
    console.log(`     Live Points:          ${comparison.live?.points} pts`);
    console.log(`     Staged Points:        ${comparison.staged?.points} pts`);
    console.log(`     Points Discrepancy:   ${comparison.discrepancy?.pointsDifference} pts`);
    console.log(
      `     Spend Discrepancy:    $${comparison.discrepancy?.spendDifferenceCents / 100}`,
    );
    console.log(`     Tier Changed:         ${comparison.discrepancy?.tierChanged}`);

    // Step 8: Verify Live Projection was NOT altered and live offsets are untouched
    console.log(
      '\n[P5-03 Proof] Step 8: Verifying live projections and consumer group offsets remain untouched...',
    );
    const dbLiveAfter = await loyaltyPool.query(
      'SELECT points, correction_required FROM loyalty_accounts WHERE customer_id = $1',
      [testCustomerId],
    );
    const liveAfter = dbLiveAfter.rows[0];

    const afterLoyaltyOffsets = await getGroupCommittedOffsets(admin, 'loyalty-v1', PURCHASE_TOPIC);
    const afterAnalyticsOffsets = await getGroupCommittedOffsets(
      admin,
      'analytics-v1',
      PURCHASE_TOPIC,
    );

    const livePointsUntouched = parseInt(liveAfter.points, 10) === 82;
    const offsetsUntouched =
      JSON.stringify(baselineLoyaltyOffsets) === JSON.stringify(afterLoyaltyOffsets) &&
      JSON.stringify(baselineAnalyticsOffsets) === JSON.stringify(afterAnalyticsOffsets);

    // Assertions for P5-03:
    // 1. Live points was 82
    // 2. Live correction_required was true
    // 3. Staged points is exactly 40 (NOT naive 70)
    // 4. Staged spend is $300 (30,000 cents)
    // 5. Staged tier is GOLD
    // 6. Discrepancy pointsDifference is -42
    // 7. Live projection points balance remained 82 (never overwritten by replay)
    // 8. Live consumer group offsets remained untouched
    const p503Success =
      livePoints === 82 &&
      liveAccount.correction_required === true &&
      stagedRecord.points === 40 &&
      stagedRecord.eligibleSpendCents === 30000 &&
      stagedRecord.tier === 'GOLD' &&
      comparison.discrepancy?.pointsDifference === -42 &&
      livePointsUntouched &&
      offsetsUntouched;

    console.log(
      '\n================================================================================',
    );
    console.log('           TWO-PASS CORRECTION (P5-03) EVIDENCE & AUDIT SUMMARY');
    console.log('================================================================================');
    console.log(
      `[PASS] Live projection before replay:              ${livePoints} pts (GOLD, $${liveSpend / 100} spend, correction_required=true)`,
    );
    console.log(
      `[PASS] Replay two-pass recomputed points:         ${stagedRecord.points} pts (CORRECT: 40 pts, NOT naive 70)`,
    );
    console.log(
      `[PASS] Replay two-pass recomputed spend:          $${stagedRecord.eligibleSpendCents / 100} (CORRECT: $300 spend)`,
    );
    console.log(
      `[PASS] Replay two-pass recomputed tier:           ${stagedRecord.tier} (CORRECT: GOLD)`,
    );
    console.log(
      `[PASS] Discrepancy audit reported:                ${comparison.discrepancy?.pointsDifference} pts discrepancy`,
    );
    console.log(
      `[PASS] Live loyalty_accounts UNTOUCHED:            ${livePointsUntouched ? 'YES (still 82 pts)' : 'NO'}`,
    );
    console.log(
      `[PASS] Live consumer group offsets UNTOUCHED:      ${offsetsUntouched ? 'YES (100% untouched)' : 'NO'}`,
    );
    console.log(
      '================================================================================\n',
    );

    if (!p503Success) {
      throw new Error('Ticket P5-03 verification failed assertions!');
    }

    console.log('Ticket P5-03 verification succeeded with 100% mathematical and system proof.\n');
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
  console.error('\n[FATAL ERROR during two-pass verification]:', err);
  process.exit(1);
});

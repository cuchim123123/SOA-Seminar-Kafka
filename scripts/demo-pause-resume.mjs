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
    console.log(`[P3-03 Pause/Resume] ${name} is already healthy at ${url}`);
    return null;
  }

  console.log(`[P3-03 Pause/Resume] Starting ${name} in background...`);
  const child = spawn(startCmd, startArgs, {
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: true,
    env: { ...process.env, ...envOverrides },
  });

  const startTime = Date.now();
  while (Date.now() - startTime < 20000) {
    if (await isPortHealthy(url)) {
      console.log(`[P3-03 Pause/Resume] ${name} became healthy at ${url}`);
      return child;
    }
    await sleep(500);
  }

  child.kill();
  throw new Error(`${name} failed to become healthy at ${url} within 20s`);
}

async function fetchGroupLag(groupId) {
  const res = await fetch(
    `http://localhost:${PRODUCER_PORT}/api/v1/ops/kafka/groups/${groupId}/lag?topic=${TOPIC}`,
  );
  if (!res.ok) throw new Error(`Failed to fetch lag for ${groupId}: ${res.statusText}`);
  return res.json();
}

async function main() {
  console.log('='.repeat(75));
  console.log('  PHASE 3 TICKET P3-03: PAUSE / RESUME SCENARIO & LAG RECOVERY PROOF');
  console.log('='.repeat(75));

  const kafka = new Kafka({ clientId: 'p3-03-verifier', brokers: BROKERS });
  const admin = kafka.admin();
  try {
    await admin.connect();
    console.log(`[P3-03 Pause/Resume] Connected to Kafka brokers: ${BROKERS.join(', ')}`);
  } catch (err) {
    console.error(`[P3-03 Pause/Resume] Cannot connect to Kafka:`, err.message);
    process.exit(1);
  }

  const spawned = [];
  try {
    // 1. Ensure all services are healthy
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

    await sleep(2500);

    // 2. Initial State Check
    console.log('\n--- STEP 1: Verifying Initial Active State ---');
    const initialLoyaltyLag = await fetchGroupLag(LOYALTY_GROUP);
    const initialAnalyticsLag = await fetchGroupLag(ANALYTICS_GROUP);
    console.log(`  Loyalty Group Lag  : ${initialLoyaltyLag.totalLag}`);
    console.log(`  Analytics Group Lag: ${initialAnalyticsLag.totalLag}`);

    // 3. Pause Analytics Consumer
    console.log('\n--- STEP 2: PAUSING Analytics Consumer Worker ---');
    const pauseRes = await fetch(
      `http://localhost:${ANALYTICS_PORT}/api/v1/analytics/consumer/pause`,
      {
        method: 'POST',
      },
    );
    if (!pauseRes.ok) throw new Error('Failed to pause Analytics consumer');
    const pauseData = await pauseRes.json();
    console.log(
      `  Analytics Consumer Status: ${pauseData.status} (isPaused: ${pauseData.isPaused})`,
    );

    // 4. Publish Workload while Analytics is Paused
    console.log('\n--- STEP 3: Publishing 4 Purchases while Analytics is Paused ---');
    const customerPrefix = `P3_PAUSE_${Date.now()}`;
    const testAmounts = [10000, 15000, 20000, 25000]; // 4 events

    for (let i = 0; i < testAmounts.length; i++) {
      const pRes = await fetch(`http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          customerId: `${customerPrefix}_${i}`,
          amountCents: testAmounts[i],
        }),
      });
      const pData = await pRes.json();
      console.log(
        `  -> Sent Purchase #${i + 1}: amount=${testAmounts[i]}c (Kafka partition=${pData.kafka.partition}, offset=${pData.kafka.offset})`,
      );
    }

    // Allow time for loyalty-v1 to process while analytics-v1 is paused
    console.log('\n  Waiting 3s for Loyalty to process and Analytics to stay paused...');
    await sleep(3000);

    // 5. Inspect Divergence
    console.log('\n--- STEP 4: Observing Divergent Lag (Proof Point 1) ---');
    const midLoyaltyLag = await fetchGroupLag(LOYALTY_GROUP);
    const midAnalyticsLag = await fetchGroupLag(ANALYTICS_GROUP);

    console.log('\nLoyalty Consumer Group (Active — Draining normally):');
    console.table(
      midLoyaltyLag.partitions.map((p) => ({
        Partition: p.partition,
        'High Watermark': p.logEndOffset,
        Committed: p.committedOffset ?? 'UNCOMMITTED',
        Lag: p.lag,
      })),
    );

    console.log('\nAnalytics Consumer Group (PAUSED — Accumulating Lag):');
    console.table(
      midAnalyticsLag.partitions.map((p) => ({
        Partition: p.partition,
        'High Watermark': p.logEndOffset,
        Committed: p.committedOffset ?? 'UNCOMMITTED',
        Lag: p.lag,
      })),
    );

    const analyticsLagNum = parseInt(midAnalyticsLag.totalLag, 10);
    const lagAccumulated = !isNaN(analyticsLagNum) && analyticsLagNum >= 4;
    console.log(`\n  Observed Analytics Total Lag: ${midAnalyticsLag.totalLag}`);
    console.log(`  Observed Loyalty Total Lag  : ${midLoyaltyLag.totalLag}`);

    if (lagAccumulated) {
      console.log(
        '  >>> SUCCESS: Analytics lag accumulated as expected while Loyalty progressed! <<<',
      );
    } else {
      console.warn('  >>> WARN: Analytics lag accumulation was less than expected workload count.');
    }

    // 6. Resume Analytics Consumer
    console.log('\n--- STEP 5: RESUMING Analytics Consumer Worker ---');
    const resumeRes = await fetch(
      `http://localhost:${ANALYTICS_PORT}/api/v1/analytics/consumer/resume`,
      {
        method: 'POST',
      },
    );
    if (!resumeRes.ok) throw new Error('Failed to resume Analytics consumer');
    const resumeData = await resumeRes.json();
    console.log(
      `  Analytics Consumer Status: ${resumeData.status} (isPaused: ${resumeData.isPaused})`,
    );

    // 7. Verify Catch-Up
    console.log('\n--- STEP 6: Observing Catch-Up (Proof Point 2) ---');
    console.log('  Waiting 3s for Analytics consumer to catch up from committed offsets...');
    await sleep(3000);

    const finalAnalyticsLag = await fetchGroupLag(ANALYTICS_GROUP);
    const analyticsSummaryRes = await fetch(
      `http://localhost:${ANALYTICS_PORT}/api/v1/analytics/summary`,
    );
    const analyticsSummary = await analyticsSummaryRes.json();

    console.log('\nAnalytics Consumer Group (RESUMED — Drained back to 0):');
    console.table(
      finalAnalyticsLag.partitions.map((p) => ({
        Partition: p.partition,
        'High Watermark': p.logEndOffset,
        Committed: p.committedOffset ?? 'UNCOMMITTED',
        Lag: p.lag,
      })),
    );

    console.log('\nAnalytics Sales Summary Projection:');
    console.log(`  Total Revenue Dollars : $${analyticsSummary.totalRevenueDollars}`);
    console.log(`  Total Purchase Count  : ${analyticsSummary.purchaseCount}`);
    console.log(`  Unique Customer Count : ${analyticsSummary.uniqueCustomerCount}`);

    const lagDrained = finalAnalyticsLag.totalLag === '0';

    console.log('\n' + '='.repeat(75));
    console.log('  TICKET P3-03 PAUSE / RESUME SCENARIO VERIFICATION SUMMARY');
    console.log('='.repeat(75));
    console.log(`Initial Lag                      : ${initialAnalyticsLag.totalLag}`);
    console.log(`Lag while Paused (Workload Sent) : ${midAnalyticsLag.totalLag}`);
    console.log(`Lag after Resume (Catch-up)      : ${finalAnalyticsLag.totalLag}`);

    if (lagAccumulated && lagDrained) {
      console.log(
        '\n  >>> PROOF CONFIRMED: Analytics lag rose while paused and drained to 0 upon resume! <<<',
      );
    } else {
      console.error('\n  >>> PROOF FAILED: Lag did not accumulate or drain as expected. <<<');
      process.exitCode = 1;
    }
  } finally {
    await admin.disconnect();
    for (const child of spawned) {
      child.kill();
    }
  }
}

main().catch((err) => {
  console.error('[P3-03 Fatal Error]:', err);
  process.exit(1);
});

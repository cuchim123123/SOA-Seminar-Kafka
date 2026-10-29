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
    console.log(`[P3-01 Verification] ${name} is already healthy at ${url}`);
    return null;
  }

  console.log(`[P3-01 Verification] Starting ${name} in background...`);
  const child = spawn(startCmd, startArgs, {
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: true,
    env: { ...process.env, ...envOverrides },
  });

  const startTime = Date.now();
  while (Date.now() - startTime < 20000) {
    if (await isPortHealthy(url)) {
      console.log(`[P3-01 Verification] ${name} became healthy at ${url}`);
      return child;
    }
    await sleep(500);
  }

  child.kill();
  throw new Error(`${name} failed to become healthy at ${url} within 20s`);
}

async function main() {
  console.log('='.repeat(70));
  console.log('  PHASE 3 TICKET P3-01: INDEPENDENT ANALYTICS CONSUMER GROUP PROOF');
  console.log('='.repeat(70));

  // 1. Check Kafka broker connection
  const kafka = new Kafka({ clientId: 'p3-verifier', brokers: BROKERS });
  const admin = kafka.admin();
  try {
    await admin.connect();
    console.log(`[P3-01 Verification] Connected to Kafka brokers: ${BROKERS.join(', ')}`);
    console.log(
      `[P3-01 Verification] Target topic: ${TOPIC} | Groups: ${LOYALTY_GROUP}, ${ANALYTICS_GROUP}`,
    );
  } catch (err) {
    console.error(`[P3-01 Verification] Cannot connect to Kafka:`, err.message);
    process.exit(1);
  }

  const spawned = [];
  try {
    // 2. Ensure all 3 services are running
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

    // Allow consumer groups to join topic partitions
    console.log('[P3-01 Verification] Waiting 3s for consumer group partition assignment...');
    await sleep(3000);

    // 3. Post test purchases through Producer API
    const testCustomerId = `P3_TEST_${Date.now()}`;
    const purchases = [
      { customerId: testCustomerId, amountCents: 10000 }, // $100.00
      { customerId: testCustomerId, amountCents: 15000 }, // $150.00
    ];

    const dispatchedEvents = [];
    console.log(
      `\n[P3-01 Verification] Publishing ${purchases.length} test events for customer ${testCustomerId}...`,
    );
    for (const p of purchases) {
      const res = await fetch(`http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(p),
      });

      if (!res.ok) {
        throw new Error(`Failed to post purchase: ${res.statusText}`);
      }

      const body = await res.json();
      console.log(
        `  -> Published: purchaseId=${body.purchaseId}, eventId=${body.eventId}, amount=${p.amountCents}c`,
      );
      dispatchedEvents.push(body);
    }

    // 4. Poll for processing across both consumer groups
    console.log('[P3-01 Verification] Polling workers for processing completion (up to 10s)...');
    let loyaltyData = null;
    let analyticsData = null;
    const pollStart = Date.now();
    while (Date.now() - pollStart < 10000) {
      if (!loyaltyData) {
        const res = await fetch(
          `http://localhost:${LOYALTY_PORT}/api/v1/demo/loyalty/${testCustomerId}`,
        );
        if (res.ok) loyaltyData = await res.json();
      }
      const res = await fetch(`http://localhost:${ANALYTICS_PORT}/api/v1/analytics/summary`);
      if (res.ok) analyticsData = await res.json();

      if (loyaltyData && analyticsData && analyticsData.purchaseCount >= 2) {
        break;
      }
      await sleep(500);
    }

    // 7. Inspect Kafka Group Metadata
    const groups = await admin.describeGroups([LOYALTY_GROUP, ANALYTICS_GROUP]);

    console.log('\n' + '='.repeat(70));
    console.log('  EVIDENCE OF INDEPENDENT CONSUMER GROUPS AND DIVERGENT PROJECTIONS');
    console.log('='.repeat(70));

    console.log('\n1. Kafka Consumer Groups Membership:');
    for (const g of groups.groups) {
      console.log(`   Group ID: ${g.groupId}`);
      console.log(`   State   : ${g.state}`);
      console.log(`   Protocol: ${g.protocolType}`);
      console.log(`   Members : ${g.members.length}`);
      for (const m of g.members) {
        console.log(
          `     - MemberId: ${m.memberId} (clientId: ${m.clientId}, host: ${m.clientHost})`,
        );
      }
    }

    console.log('\n2. Loyalty Worker Projection (Group: loyalty-v1):');
    if (loyaltyData) {
      console.log(`   Customer ID   : ${loyaltyData.customerId}`);
      console.log(`   Points Balance: ${loyaltyData.pointsBalance}`);
      console.log(`   Tier          : ${loyaltyData.tier}`);
    } else {
      console.log(`   [WARN] No loyalty data recorded for ${testCustomerId}`);
    }

    console.log('\n3. Sales Analytics Projection (Group: analytics-v1):');
    if (analyticsData) {
      console.log(
        `   Total Revenue : $${analyticsData.totalRevenueDollars} (${analyticsData.totalRevenueCents} cents)`,
      );
      console.log(`   Total Orders  : ${analyticsData.purchaseCount}`);
      console.log(`   Unique Custs  : ${analyticsData.uniqueCustomerCount}`);
      console.log(`   Last Event ID : ${analyticsData.lastEventId}`);
    } else {
      console.log(`   [WARN] No analytics summary returned`);
    }

    console.log('\n4. Verification Assertion:');
    const loyaltyOk = loyaltyData && loyaltyData.pointsBalance > 0;
    const analyticsOk = analyticsData && analyticsData.purchaseCount >= 2;
    const bothGroupsRegistered = groups.groups.length === 2;

    if (loyaltyOk && analyticsOk && bothGroupsRegistered) {
      console.log(
        '  >>> SUCCESS: Both consumer groups independently consumed identical stream records! <<<',
      );
    } else {
      console.error('  >>> FAILURE: Independent consumption assertion failed. <<<');
      process.exitCode = 1;
    }
  } finally {
    await admin.disconnect();
    for (const child of spawned) {
      console.log(`[P3-01 Verification] Shutting down spawned process PID ${child.pid}...`);
      child.kill();
    }
    process.exit(process.exitCode ?? 0);
  }
}

main().catch((err) => {
  console.error('[P3-01 Verification Fatal Error]:', err);
  process.exit(1);
});

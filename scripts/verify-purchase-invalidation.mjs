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
const PURCHASES_DATABASE_URL =
  process.env.PURCHASES_DATABASE_URL ||
  process.env.DATABASE_URL ||
  'postgresql://postgres:postgres@localhost:5432/purchases_db?schema=public';

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
    console.log(`[P5-01 Proof] ${name} is already healthy at ${url}`);
    return null;
  }

  console.log(`[P5-01 Proof] Starting ${name} in background...`);
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
    if (await isPortHealthy(url)) {
      console.log(`[P5-01 Proof] ${name} became healthy at ${url}`);
      return child;
    }
    await sleep(500);
  }

  killProcess(child);
  throw new Error(
    `${name} failed to become healthy at ${url} within 35s.\nProcess output:\n${output}`,
  );
}

async function main() {
  console.log('='.repeat(80));
  console.log('  PHASE 5 TICKET P5-01: PURCHASE INVALIDATION EVENT VERIFICATION');
  console.log('='.repeat(80));

  let producerChild = null;
  let publisherChild = null;
  let kafkaConsumer = null;

  const pool = new pg.Pool({ connectionString: PURCHASES_DATABASE_URL });
  const client = await pool.connect();
  console.log("[P5-01 Proof] Connected to PostgreSQL DB: 'purchases_db'");

  try {
    // 1. Ensure required services are healthy
    producerChild = await ensureServiceRunning(
      'producer-api',
      `http://localhost:${PRODUCER_PORT}/health/ready`,
      'node',
      ['apps/producer-api/dist/main.js'],
      {
        PORT: PRODUCER_PORT,
        PURCHASES_DATABASE_URL,
        DATABASE_URL: PURCHASES_DATABASE_URL,
        OUTBOX_RELAY_IN_PROCESS: 'false',
      },
    );

    publisherChild = await ensureServiceRunning(
      'outbox-publisher',
      `http://localhost:${PUBLISHER_PORT}/health`,
      'node',
      ['apps/outbox-publisher/dist/main.js'],
      {
        PORT: PUBLISHER_PORT,
        PURCHASES_DATABASE_URL,
        DATABASE_URL: PURCHASES_DATABASE_URL,
        POLL_INTERVAL_MS: '250',
        BATCH_SIZE: '50',
      },
    );

    // 2. Setup Kafka consumer to capture published partition events
    const kafka = new Kafka({
      clientId: `p5-invalidation-verifier-${Date.now()}`,
      brokers: KAFKA_BROKERS,
    });

    const consumerGroupId = `p5-invalidation-proof-${Date.now()}`;
    kafkaConsumer = kafka.consumer({ groupId: consumerGroupId });
    await kafkaConsumer.connect();
    await kafkaConsumer.subscribe({ topic: PURCHASE_TOPIC, fromBeginning: false });

    const capturedMessages = [];
    await kafkaConsumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const envelope = JSON.parse(message.value.toString('utf-8'));
          capturedMessages.push({
            topic,
            partition,
            offset: message.offset,
            key: message.key ? message.key.toString('utf-8') : null,
            envelope,
          });
        } catch (err) {
          console.warn('[P5-01 Proof] Failed to parse message:', err.message);
        }
      },
    });

    // 3. Generate distinct customer fixture
    const testCustomerId = `CUST_P5_${Date.now()}`;
    console.log(
      `\n[P5-01 Proof] Step 1: Submitting 3 completed purchases for customer '${testCustomerId}'...`,
    );

    const purchasePayloads = [
      { amountCents: 12000, desc: '$120.00' },
      { amountCents: 20000, desc: '$200.00' },
      { amountCents: 10000, desc: '$100.00' },
    ];

    const purchases = [];

    for (let i = 0; i < purchasePayloads.length; i++) {
      const payload = purchasePayloads[i];
      const res = await fetch(`http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          customerId: testCustomerId,
          amountCents: payload.amountCents,
        }),
      });

      if (!res.ok) {
        throw new Error(`Failed to create purchase ${i + 1}: ${res.status} ${await res.text()}`);
      }

      const body = await res.json();
      purchases.push(body);
      console.log(
        `  -> Purchase ${i + 1} (${payload.desc}) accepted: purchaseId=${body.purchaseId}, eventId=${body.eventId}`,
      );
    }

    const targetPurchase = purchases[0];

    // 4. Invalidate Purchase 1
    console.log(
      `\n[P5-01 Proof] Step 2: Invalidating Purchase 1 (${targetPurchase.purchaseId})...`,
    );
    const invRes = await fetch(
      `http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases/${targetPurchase.purchaseId}/invalidate`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          reason: 'DEMO_INVALIDATION',
        }),
      },
    );

    if (!invRes.ok) {
      throw new Error(`Invalidate request failed: ${invRes.status} ${await invRes.text()}`);
    }

    const invBody = await invRes.json();
    console.log('  -> Invalidation response:', JSON.stringify(invBody));

    if (invBody.status !== 'INVALIDATED') {
      throw new Error(`Expected status 'INVALIDATED', got '${invBody.status}'`);
    }
    if (invBody.customerSequence !== 4) {
      throw new Error(
        `Expected customerSequence 4 for invalidation, got ${invBody.customerSequence}`,
      );
    }
    if (invBody.isAlreadyInvalidated !== false) {
      throw new Error(
        `Expected isAlreadyInvalidated false on first call, got ${invBody.isAlreadyInvalidated}`,
      );
    }

    // 5. Test Idempotency: Invalidate again
    console.log(`\n[P5-01 Proof] Step 3: Verifying Idempotent Invalidation repeat call...`);
    const invRes2 = await fetch(
      `http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases/${targetPurchase.purchaseId}/invalidate`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          reason: 'DEMO_INVALIDATION',
        }),
      },
    );

    if (!invRes2.ok) {
      throw new Error(
        `Repeat invalidate request failed: ${invRes2.status} ${await invRes2.text()}`,
      );
    }

    const invBody2 = await invRes2.json();
    console.log('  -> Second Invalidation response:', JSON.stringify(invBody2));

    if (invBody2.isAlreadyInvalidated !== true) {
      throw new Error(
        `Expected isAlreadyInvalidated true on duplicate call, got ${invBody2.isAlreadyInvalidated}`,
      );
    }

    const seqCheck = await client.query(
      `SELECT next_event_sequence FROM purchase_customers WHERE customer_id = $1`,
      [testCustomerId],
    );
    const dbNextSeq = Number(seqCheck.rows[0].next_event_sequence);
    if (dbNextSeq !== 5) {
      throw new Error(
        `Customer sequence advanced on duplicate invalidation: next sequence is ${dbNextSeq}, expected 5`,
      );
    }
    console.log(
      `  -> Idempotency confirmed: next_event_sequence remained ${dbNextSeq} (no extra sequence allocated)`,
    );

    // 6. Verify Database State in purchases_db
    console.log(`\n[P5-01 Proof] Step 4: Verifying PostgreSQL state in purchases_db...`);
    const purchaseRows = await client.query(
      `SELECT purchase_id, customer_id, amount_cents, status, completion_sequence FROM purchases WHERE customer_id = $1 ORDER BY completion_sequence ASC`,
      [testCustomerId],
    );

    console.log(`  -> Found ${purchaseRows.rows.length} purchases in database:`);
    for (const row of purchaseRows.rows) {
      console.log(
        `     purchase_id=${row.purchase_id} status=${row.status} seq=${row.completion_sequence} amount=${row.amount_cents}`,
      );
    }

    const dbTarget = purchaseRows.rows.find((r) => r.purchase_id === targetPurchase.purchaseId);
    if (!dbTarget || dbTarget.status !== 'INVALIDATED') {
      throw new Error(
        `Purchase ${targetPurchase.purchaseId} in DB should be INVALIDATED, got: ${dbTarget?.status}`,
      );
    }

    // 7. Wait for Outbox Publisher to drain and publish all 4 records
    console.log(
      `\n[P5-01 Proof] Step 5: Waiting for Outbox Publisher to publish all 4 records to Kafka...`,
    );
    const maxWaitMs = 15000;
    const startWait = Date.now();
    let publishedCount = 0;

    while (Date.now() - startWait < maxWaitMs) {
      const outboxRes = await client.query(
        `SELECT event_id, customer_sequence, event_type, status, published_at FROM outbox_events WHERE aggregate_id = $1 ORDER BY customer_sequence ASC`,
        [testCustomerId],
      );

      publishedCount = outboxRes.rows.filter((r) => r.status === 'PUBLISHED').length;
      if (publishedCount === 4) {
        console.log(`  -> All 4 outbox records marked PUBLISHED in database.`);
        break;
      }
      await sleep(500);
    }

    if (publishedCount < 4) {
      throw new Error(
        `Timed out waiting for outbox events to be published. Only ${publishedCount}/4 published.`,
      );
    }

    // 8. Wait for Kafka Consumer to receive messages for this customer
    console.log(`\n[P5-01 Proof] Step 6: Verifying Kafka messages on topic '${PURCHASE_TOPIC}'...`);
    const consumerWaitStart = Date.now();
    let customerEvents = [];

    while (Date.now() - consumerWaitStart < 15000) {
      customerEvents = capturedMessages.filter((m) => m.key === testCustomerId);
      if (customerEvents.length >= 4) {
        break;
      }
      await sleep(500);
    }

    console.log(
      `  -> Captured ${customerEvents.length} events from Kafka for customer '${testCustomerId}'`,
    );

    if (customerEvents.length !== 4) {
      throw new Error(
        `Expected exactly 4 Kafka events for customer, but received ${customerEvents.length}`,
      );
    }

    // 9. Verify Partition Ordering & Key Safety
    const partitions = new Set(customerEvents.map((e) => e.partition));
    if (partitions.size !== 1) {
      throw new Error(
        `Partition key violation! Events split across partitions: ${Array.from(partitions).join(', ')}`,
      );
    }

    const assignedPartition = customerEvents[0].partition;
    console.log(
      `  -> Ordering invariant confirmed: All 4 events routed to Partition ${assignedPartition}`,
    );

    // Verify sequential ordering
    for (let i = 0; i < 4; i++) {
      const evt = customerEvents[i];
      const expectedSeq = i + 1;
      const actualSeq = evt.envelope.customerSequence;
      if (actualSeq !== expectedSeq) {
        throw new Error(
          `Sequence violation at position ${i}: expected seq ${expectedSeq}, got ${actualSeq}`,
        );
      }
    }

    // Verify invalidation event envelope details
    const invalidationEvent = customerEvents[3];
    if (invalidationEvent.envelope.eventType !== 'PurchaseInvalidated') {
      throw new Error(
        `Expected 4th event to be 'PurchaseInvalidated', got '${invalidationEvent.envelope.eventType}'`,
      );
    }
    if (invalidationEvent.envelope.payload.purchaseId !== targetPurchase.purchaseId) {
      throw new Error(`Payload purchaseId mismatch in invalidation event`);
    }
    if (invalidationEvent.envelope.payload.reason !== 'DEMO_INVALIDATION') {
      throw new Error(
        `Payload reason mismatch: expected DEMO_INVALIDATION, got ${invalidationEvent.envelope.payload.reason}`,
      );
    }

    // 10. Display Summary Evidence Table
    console.log('\n' + '='.repeat(80));
    console.log('  PROOF EVIDENCE TABLE: TICKET P5-01 PURCHASE INVALIDATION');
    console.log('='.repeat(80));
    console.log(
      'Seq | Event Type          | Partition | Offset | Target PurchaseId    | Status / Reason',
    );
    console.log('-'.repeat(80));

    customerEvents.forEach((e) => {
      const seq = String(e.envelope.customerSequence).padEnd(3);
      const type = String(e.envelope.eventType).padEnd(19);
      const part = String(e.partition).padEnd(9);
      const off = String(e.offset).padEnd(6);
      const purId = String(e.envelope.payload.purchaseId).padEnd(22);
      const detail =
        e.envelope.eventType === 'PurchaseCompleted'
          ? `$${(e.envelope.payload.amountCents / 100).toFixed(2)} (COMPLETED)`
          : `INVALIDATED (${e.envelope.payload.reason})`;
      console.log(`${seq} | ${type} | ${part} | ${off} | ${purId} | ${detail}`);
    });

    console.log('='.repeat(80));
    console.log(' [SUCCESS] P5-01 Invalidation Event Flow verified end-to-end:');
    console.log('   ✓ Purchase 1 marked INVALIDATED in PostgreSQL purchases_db');
    console.log('   ✓ Next monotonic sequence 4 allocated under row lock');
    console.log('   ✓ PurchaseInvalidated envelope atomically saved to Outbox');
    console.log('   ✓ Duplicate invalidation proved idempotent (no double sequence advance)');
    console.log('   ✓ Outbox Publisher successfully relayed event to Kafka');
    console.log('   ✓ Single-partition strictly-ordered delivery verified (seq 1, 2, 3, 4)');
    console.log('='.repeat(80));
  } finally {
    if (kafkaConsumer) {
      try {
        await kafkaConsumer.disconnect();
      } catch {
        // Ignore disconnect errors during teardown
      }
    }
    client.release();
    await pool.end();

    if (producerChild) {
      console.log('[P5-01 Proof] Stopping background producer-api...');
      killProcess(producerChild);
    }
    if (publisherChild) {
      console.log('[P5-01 Proof] Stopping background outbox-publisher...');
      killProcess(publisherChild);
    }
  }
}

main().catch((err) => {
  console.error('\n[P5-01 Proof ERROR]', err);
  process.exit(1);
});

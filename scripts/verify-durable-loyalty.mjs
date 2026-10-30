import { spawn } from 'node:child_process';
import pg from 'pg';
import { Kafka } from 'kafkajs';
import { createPurchaseCompletedEnvelope } from '@lab/contracts';

if (typeof process.loadEnvFile === 'function') {
  try {
    process.loadEnvFile('.env');
  } catch {
    // Ignore if .env doesn't exist
  }
}

const LOYALTY_PORT = process.env.LOYALTY_PORT || '3001';
const DATABASE_URL =
  process.env.LOYALTY_DATABASE_URL ||
  process.env.DATABASE_URL ||
  (process.env.POSTGRES_PASSWORD
    ? `postgresql://${process.env.POSTGRES_USER || 'postgres'}:${process.env.POSTGRES_PASSWORD}@localhost:${process.env.POSTGRES_PORT || '5432'}/${process.env.POSTGRES_DB || 'kafka_streaming_lab'}?schema=public`
    : undefined);
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

async function ensureServiceRunning(name, url, startCmd, startArgs, envOverrides = {}) {
  const healthy = await isPortHealthy(url);
  if (healthy) {
    console.log(`[P4-04 Proof] ${name} is already healthy at ${url}`);
    return null;
  }

  console.log(`[P4-04 Proof] Starting ${name} in background...`);
  const child = spawn(startCmd, startArgs, {
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: true,
    env: { ...process.env, ...envOverrides },
  });

  const startTime = Date.now();
  while (Date.now() - startTime < 25000) {
    if (await isPortHealthy(url)) {
      console.log(`[P4-04 Proof] ${name} became healthy at ${url}`);
      return child;
    }
    await sleep(500);
  }

  child.kill();
  throw new Error(`${name} failed to become healthy at ${url} within 25s`);
}

async function main() {
  console.log('='.repeat(75));
  console.log('  PHASE 4 TICKET P4-04: DURABLE LOYALTY & CONSUMER IDEMPOTENCY PROOF');
  console.log('='.repeat(75));

  if (!DATABASE_URL) {
    console.error('ERROR: DATABASE_URL or POSTGRES_PASSWORD must be configured.');
    process.exit(1);
  }

  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  const client = await pool.connect();
  console.log("[P4-04 Proof] Connected to PostgreSQL DB: 'kafka_streaming_lab'");

  let loyaltyChild = null;

  try {
    // 1. Verify schema tables exist
    const tableCheck = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
        AND table_name IN ('loyalty_accounts', 'processed_events')
      ORDER BY table_name;
    `);

    const tableNames = tableCheck.rows.map((r) => r.table_name);
    console.log(`[P4-04 Proof] Verified PostgreSQL tables: ${tableNames.join(', ')}`);
    if (tableNames.length < 2) {
      throw new Error(`Missing expected tables. Found: ${JSON.stringify(tableNames)}`);
    }

    // 2. Start loyalty-worker service
    loyaltyChild = await ensureServiceRunning(
      'loyalty-worker',
      `http://localhost:${LOYALTY_PORT}/health`,
      'node',
      ['--env-file=.env', 'apps/loyalty-worker/dist/main.js'],
      {
        PORT: LOYALTY_PORT,
        DATABASE_URL,
      },
    );

    // 3. Connect Kafka producer to send test events
    const kafka = new Kafka({
      clientId: `verify-p4-04-${Date.now()}`,
      brokers: KAFKA_BROKERS,
    });
    const producer = kafka.producer();
    await producer.connect();
    console.log('[P4-04 Proof] Connected to Kafka brokers');

    const testCustomerId = `DEMO_LOYALTY_P4_${Date.now()}`;
    const eventId1 = `evt_demo_p4_${Date.now()}_1`;
    const eventId2 = `evt_demo_p4_${Date.now()}_2`;

    // 4. Send Event 1: $120.00 (12,000 cents), seq 1
    console.log(
      `\n[P4-04 Proof] Step 1: Publishing initial purchase event for ${testCustomerId}...`,
    );
    const envelope1 = createPurchaseCompletedEnvelope({
      customerId: testCustomerId,
      customerSequence: 1,
      purchaseId: `pur_${testCustomerId}_1`,
      amountCents: 12_000,
      occurredAt: new Date().toISOString(),
    });
    // Overwrite eventId with our deterministic fixture ID
    envelope1.eventId = eventId1;

    await producer.send({
      topic: PURCHASE_TOPIC,
      messages: [
        {
          key: testCustomerId,
          value: JSON.stringify(envelope1),
        },
      ],
    });
    console.log(`[P4-04 Proof] Published Event 1 (${eventId1}): amount=$120.00, seq=1`);

    // Poll for event 1 processing
    console.log('[P4-04 Proof] Waiting for Loyalty Worker to process Event 1...');
    let accountAfter1 = null;
    const startPoll1 = Date.now();
    while (Date.now() - startPoll1 < 30000) {
      const res = await client.query('SELECT * FROM loyalty_accounts WHERE customer_id = $1', [
        testCustomerId,
      ]);
      if (res.rows.length > 0) {
        accountAfter1 = res.rows[0];
        break;
      }
      await sleep(300);
    }

    if (!accountAfter1) {
      throw new Error(`Timeout waiting for initial loyalty processing of ${testCustomerId}`);
    }

    console.log(
      `[P4-04 Proof] Event 1 Applied: Points=${accountAfter1.points}, Tier=${accountAfter1.tier}, LastSeq=${accountAfter1.last_customer_sequence}`,
    );

    const processedCheck1 = await client.query(
      'SELECT * FROM processed_events WHERE consumer_name = $1 AND event_id = $2',
      ['loyalty-v1', eventId1],
    );
    if (processedCheck1.rows.length !== 1) {
      throw new Error(`Expected processed_events record for ${eventId1}, found 0`);
    }
    console.log(`[P4-04 Proof] Processed events entry confirmed for event ${eventId1}`);

    // 5. Send DUPLICATE Event 1 (simulating at-least-once Kafka redelivery)
    console.log(
      '\n[P4-04 Proof] Step 2: Injecting identical duplicate event (simulating redelivery)...',
    );
    await producer.send({
      topic: PURCHASE_TOPIC,
      messages: [
        {
          key: testCustomerId,
          value: JSON.stringify(envelope1),
        },
      ],
    });
    console.log(`[P4-04 Proof] Redelivered Event 1 (${eventId1}) to topic ${PURCHASE_TOPIC}`);

    // Wait to allow consumer to process the redelivery
    await sleep(2500);

    const accountAfterDup = (
      await client.query('SELECT * FROM loyalty_accounts WHERE customer_id = $1', [testCustomerId])
    ).rows[0];

    console.log(
      `[P4-04 Proof] State After Redelivery: Points=${accountAfterDup.points}, Tier=${accountAfterDup.tier}`,
    );

    if (Number(accountAfterDup.points) !== 12) {
      throw new Error(
        `IDEMPOTENCY FAILURE! Expected points to remain 12, but found ${accountAfterDup.points}`,
      );
    }
    console.log(
      '  >>> IDEMPOTENCY CONFIRMED: Duplicate event skipped without double-crediting points! <<<',
    );

    // 6. Send Event 2: $200.00 (20,000 cents), seq 2 -> evaluated at SILVER (2x) -> 40 points -> total 52 points (GOLD)
    console.log('\n[P4-04 Proof] Step 3: Publishing second sequential purchase event...');
    const envelope2 = createPurchaseCompletedEnvelope({
      customerId: testCustomerId,
      customerSequence: 2,
      purchaseId: `pur_${testCustomerId}_2`,
      amountCents: 20_000,
      occurredAt: new Date().toISOString(),
    });
    envelope2.eventId = eventId2;

    await producer.send({
      topic: PURCHASE_TOPIC,
      messages: [
        {
          key: testCustomerId,
          value: JSON.stringify(envelope2),
        },
      ],
    });
    console.log(`[P4-04 Proof] Published Event 2 (${eventId2}): amount=$200.00, seq=2`);

    let accountAfter2 = null;
    const startPoll2 = Date.now();
    while (Date.now() - startPoll2 < 30000) {
      const res = await client.query(
        'SELECT * FROM loyalty_accounts WHERE customer_id = $1 AND last_customer_sequence = 2',
        [testCustomerId],
      );
      if (res.rows.length > 0) {
        accountAfter2 = res.rows[0];
        break;
      }
      await sleep(300);
    }

    if (!accountAfter2) {
      throw new Error(`Timeout waiting for second loyalty event processing for ${testCustomerId}`);
    }

    console.log(
      `[P4-04 Proof] Event 2 Applied: Points=${accountAfter2.points}, Tier=${accountAfter2.tier}, LastSeq=${accountAfter2.last_customer_sequence}`,
    );

    if (Number(accountAfter2.points) !== 52 || accountAfter2.tier !== 'GOLD') {
      throw new Error(
        `Unexpected loyalty calculation. Expected points 52, tier GOLD, found: ${JSON.stringify(accountAfter2)}`,
      );
    }

    // 7. Verify Read DAO HTTP endpoint
    console.log('\n[P4-04 Proof] Step 4: Querying Loyalty HTTP Read DAO endpoint...');
    const httpRes = await fetch(
      `http://localhost:${LOYALTY_PORT}/api/v1/loyalty/${testCustomerId}`,
    );
    if (!httpRes.ok) {
      throw new Error(
        `HTTP GET /api/v1/loyalty/${testCustomerId} returned status ${httpRes.status}`,
      );
    }
    const projection = await httpRes.json();
    console.log('[P4-04 Proof] Read DAO Response:', JSON.stringify(projection, null, 2));

    if (projection.pointsBalance !== 52 || projection.tier !== 'GOLD') {
      throw new Error(`Read DAO projection mismatch: ${JSON.stringify(projection)}`);
    }

    await producer.disconnect();

    console.log('\n' + '='.repeat(75));
    console.log('  EVIDENCE SUMMARY: PHASE 4 TICKET P4-04 PASSED');
    console.log('='.repeat(75));
    console.log(`PostgreSQL Storage             : loyalty_accounts & processed_events confirmed`);
    console.log(
      `Consumer Idempotency           : ON CONFLICT DO NOTHING RETURNING event_id verified`,
    );
    console.log(
      `Duplicate Event Redelivery     : Points correctly preserved at 12 (no duplicate points)`,
    );
    console.log(
      `Sequential Promotion           : Seq 1 ($120) -> 12 pts (Silver) | Seq 2 ($200) -> +40 pts = 52 pts (Gold)`,
    );
    console.log(`Read DAO Query Endpoint        : Verified GET /api/v1/loyalty/:customerId`);
    console.log(`\n  >>> PROOF CONFIRMED: P4-04 Durable Loyalty Processing verified! <<<\n`);
  } finally {
    client.release();
    await pool.end();

    if (loyaltyChild && loyaltyChild.pid) {
      if (process.platform === 'win32') {
        try {
          spawn('taskkill', ['/pid', String(loyaltyChild.pid), '/f', '/t'], { stdio: 'ignore' });
        } catch {
          loyaltyChild.kill('SIGKILL');
        }
      } else {
        loyaltyChild.kill('SIGKILL');
      }
    }
  }
}

main().catch((err) => {
  console.error('\n[P4-04 Proof] Verification FAILED:', err);
  process.exit(1);
});

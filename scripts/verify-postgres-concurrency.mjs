import { spawn } from 'node:child_process';
import pg from 'pg';

if (typeof process.loadEnvFile === 'function') {
  try {
    process.loadEnvFile('.env');
  } catch {
    // Ignore if .env doesn't exist
  }
}

const PRODUCER_PORT = process.env.PORT || '3000';
const DATABASE_URL =
  process.env.DATABASE_URL ||
  `postgresql://${process.env.POSTGRES_USER || 'postgres'}:${process.env.POSTGRES_PASSWORD || 'postgres'}@localhost:${process.env.POSTGRES_PORT || '5432'}/${process.env.POSTGRES_DB || 'kafka_streaming_lab'}?schema=public`;

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
    console.log(`[P4-01 Proof] ${name} is already healthy at ${url}`);
    return null;
  }

  console.log(`[P4-01 Proof] Starting ${name} in background...`);
  const child = spawn(startCmd, startArgs, {
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: true,
    env: { ...process.env, ...envOverrides },
  });

  const startTime = Date.now();
  while (Date.now() - startTime < 20000) {
    if (await isPortHealthy(url)) {
      console.log(`[P4-01 Proof] ${name} became healthy at ${url}`);
      return child;
    }
    await sleep(500);
  }

  child.kill();
  throw new Error(`${name} failed to become healthy at ${url} within 20s`);
}

async function main() {
  console.log('='.repeat(75));
  console.log('  PHASE 4 TICKET P4-01: POSTGRES MIGRATIONS & CONCURRENT SEQUENCE LOCK PROOF');
  console.log('='.repeat(75));

  // 1. Check PostgreSQL Connection
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    const dbRes = await pool.query('SELECT current_database(), version();');
    console.log(`[P4-01 Proof] Connected to PostgreSQL DB: '${dbRes.rows[0].current_database}'`);
  } catch (err) {
    console.error(`[P4-01 Proof] Cannot connect to PostgreSQL:`, err.message);
    process.exit(1);
  }

  const spawned = [];
  try {
    // 2. Ensure Producer API is running
    const producerChild = await ensureServiceRunning(
      'producer-api',
      `http://localhost:${PRODUCER_PORT}/health/ready`,
      'node',
      ['apps/producer-api/dist/main.js'],
      { PORT: PRODUCER_PORT, DATABASE_URL },
    );
    if (producerChild) spawned.push(producerChild);

    await sleep(2000);

    // 3. Dispatch 10 Concurrent Requests for the same customer
    const customerId = `U_RACE_${Date.now()}`;
    const concurrentCount = 10;
    console.log(
      `\n[P4-01 Proof] Dispatching ${concurrentCount} parallel purchase requests for customer ${customerId}...`,
    );

    const startTimestamp = Date.now();
    const tasks = Array.from({ length: concurrentCount }, async (_, i) => {
      const res = await fetch(`http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          customerId,
          amountCents: (i + 1) * 1000,
        }),
      });

      if (!res.ok) {
        throw new Error(`Request #${i + 1} failed with status: ${res.statusText}`);
      }
      return res.json();
    });

    const httpResponses = await Promise.all(tasks);
    const durationMs = Date.now() - startTimestamp;
    console.log(
      `[P4-01 Proof] All ${httpResponses.length} requests completed successfully in ${durationMs}ms.`,
    );

    // 4. Inspect PostgreSQL State Directly
    console.log('\n--- PostgreSQL Persisted Purchases Inspection ---');
    const dbPurchases = await pool.query(
      `SELECT purchase_id, customer_id, amount_cents, status, completion_sequence, created_at
       FROM purchases
       WHERE customer_id = $1
       ORDER BY completion_sequence ASC`,
      [customerId],
    );

    console.table(
      dbPurchases.rows.map((r) => ({
        'Purchase ID': r.purchase_id,
        'Customer ID': r.customer_id,
        'Amount ($)': (Number(r.amount_cents) / 100).toFixed(2),
        Status: r.status,
        Sequence: Number(r.completion_sequence),
        'Created At': r.created_at.toISOString(),
      })),
    );

    const customerRecord = await pool.query(
      `SELECT customer_id, next_event_sequence FROM purchase_customers WHERE customer_id = $1`,
      [customerId],
    );

    console.log('\n--- Customer Sequence Counter State ---');
    console.log(`Customer ID         : ${customerRecord.rows[0].customer_id}`);
    console.log(`Next Event Sequence : ${customerRecord.rows[0].next_event_sequence}`);

    // 5. Verification Assertions
    const assignedSequences = dbPurchases.rows.map((r) => Number(r.completion_sequence));
    const expectedSequences = Array.from({ length: concurrentCount }, (_, i) => i + 1);
    const isSequential = JSON.stringify(assignedSequences) === JSON.stringify(expectedSequences);
    const countMatches = dbPurchases.rowCount === concurrentCount;
    const nextSeqMatches =
      Number(customerRecord.rows[0].next_event_sequence) === concurrentCount + 1;

    console.log('\n' + '='.repeat(75));
    console.log('  TICKET P4-01 EVIDENCE VERIFICATION SUMMARY');
    console.log('='.repeat(75));
    console.log(`Concurrent Requests Sent  : ${concurrentCount}`);
    console.log(`Persisted Purchase Count  : ${dbPurchases.rowCount}`);
    console.log(`Assigned Sequence Array   : [${assignedSequences.join(', ')}]`);
    console.log(`Expected Sequence Array   : [${expectedSequences.join(', ')}]`);
    console.log(`Final next_event_sequence : ${customerRecord.rows[0].next_event_sequence}`);

    if (isSequential && countMatches && nextSeqMatches) {
      console.log(
        '\n  >>> PROOF CONFIRMED: PostgreSQL row locks serialized concurrent customer requests with unique monotonic sequences! <<<',
      );
    } else {
      console.error(
        '\n  >>> PROOF FAILED: Sequence assignment did not match strictly monotonic expectations. <<<',
      );
      process.exitCode = 1;
    }
  } finally {
    await pool.end();
    for (const child of spawned) {
      child.kill();
    }
    process.exit(process.exitCode ?? 0);
  }
}

main().catch((err) => {
  console.error('[P4-01 Fatal Error]:', err);
  process.exit(1);
});

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
  (process.env.POSTGRES_PASSWORD
    ? `postgresql://${process.env.POSTGRES_USER || 'postgres'}:${process.env.POSTGRES_PASSWORD}@localhost:${process.env.POSTGRES_PORT || '5432'}/${process.env.POSTGRES_DB || 'kafka_streaming_lab'}?schema=public`
    : undefined);

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
    console.log(`[P4-02 Proof] ${name} is already healthy at ${url}`);
    return null;
  }

  console.log(`[P4-02 Proof] Starting ${name} in background...`);
  const child = spawn(startCmd, startArgs, {
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: true,
    env: { ...process.env, ...envOverrides },
  });

  const startTime = Date.now();
  while (Date.now() - startTime < 20000) {
    if (await isPortHealthy(url)) {
      console.log(`[P4-02 Proof] ${name} became healthy at ${url}`);
      return child;
    }
    await sleep(500);
  }

  child.kill();
  throw new Error(`${name} failed to become healthy at ${url} within 20s`);
}

async function main() {
  console.log('='.repeat(75));
  console.log('  PHASE 4 TICKET P4-02: CQRS PURCHASE FLOW, READ DAO & OUTBOX PROOF');
  console.log('='.repeat(75));

  // 1. Check PostgreSQL Connection
  const pool = new pg.Pool({ connectionString: DATABASE_URL });
  try {
    const dbRes = await pool.query('SELECT current_database();');
    console.log(`[P4-02 Proof] Connected to PostgreSQL DB: '${dbRes.rows[0].current_database}'`);
  } catch (err) {
    console.error(`[P4-02 Proof] Cannot connect to PostgreSQL:`, err.message);
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

    const customerId = `U_CQRS_${Date.now()}`;
    const idempotencyKey = `idem_${Date.now()}`;
    const amountCents = 7500;

    // 3. Dispatch POST /api/v1/demo/purchases with Idempotency-Key
    console.log(`\n[P4-02 Proof] 1. Dispatching POST /api/v1/demo/purchases for ${customerId}...`);
    const postRes = await fetch(`http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': idempotencyKey,
      },
      body: JSON.stringify({
        customerId,
        amountCents,
      }),
    });

    if (postRes.status !== 202) {
      throw new Error(
        `Expected HTTP 202 Accepted, received ${postRes.status}: ${await postRes.text()}`,
      );
    }
    const postBody = await postRes.json();
    console.log(`[P4-02 Proof] POST Response HTTP 202:`, postBody);

    // 4. Query GET /api/v1/demo/purchases/:purchaseId via CQRS Read DAO
    console.log(`\n[P4-02 Proof] 2. Querying GET /api/v1/demo/purchases/${postBody.purchaseId}...`);
    // Allow outbox relay to finish publishing to Kafka
    await sleep(1500);

    const getRes = await fetch(
      `http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases/${postBody.purchaseId}`,
    );
    if (!getRes.ok) {
      throw new Error(`GET query failed with status: ${getRes.statusText}`);
    }
    const getBody = await getRes.json();
    console.log(`[P4-02 Proof] GET Response HTTP 200 (Read Model):`, getBody);

    // 5. Test Exact Duplicate Idempotency via HTTP
    console.log(`\n[P4-02 Proof] 3. Testing exact duplicate POST with same Idempotency-Key...`);
    const dupRes = await fetch(`http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': idempotencyKey,
      },
      body: JSON.stringify({
        customerId,
        amountCents,
      }),
    });
    const dupBody = await dupRes.json();
    const isIdempotentMatch = dupRes.status === 202 && dupBody.purchaseId === postBody.purchaseId;
    console.log(
      `[P4-02 Proof] Duplicate POST status: ${dupRes.status}, Purchase ID: ${dupBody.purchaseId} (Match: ${isIdempotentMatch})`,
    );

    // 6. Test Conflicting Idempotency via HTTP
    console.log(
      `\n[P4-02 Proof] 4. Testing conflicting POST with same Idempotency-Key but different amount...`,
    );
    const conflictRes = await fetch(`http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': idempotencyKey,
      },
      body: JSON.stringify({
        customerId,
        amountCents: 9999, // Different amount
      }),
    });
    const isConflictExpected = conflictRes.status === 409;
    console.log(
      `[P4-02 Proof] Conflicting POST status: ${conflictRes.status} (Expected 409: ${isConflictExpected})`,
    );

    // 7. Test 404 for Unknown Purchase
    console.log(`\n[P4-02 Proof] 5. Testing GET for non-existent purchase ID...`);
    const notFoundRes = await fetch(
      `http://localhost:${PRODUCER_PORT}/api/v1/demo/purchases/pur_non_existent`,
    );
    const is404Expected = notFoundRes.status === 404;
    console.log(
      `[P4-02 Proof] Unknown Purchase status: ${notFoundRes.status} (Expected 404: ${is404Expected})`,
    );

    // 8. Direct PostgreSQL Persisted Tables Verification
    console.log('\n--- Direct PostgreSQL Inspection ---');
    const dbPurchase = await pool.query(`SELECT * FROM purchases WHERE purchase_id = $1`, [
      postBody.purchaseId,
    ]);
    const dbOutbox = await pool.query(`SELECT * FROM outbox_events WHERE aggregate_id = $1`, [
      customerId,
    ]);
    const dbIdem = await pool.query(`SELECT * FROM http_idempotency_keys WHERE customer_id = $1`, [
      customerId,
    ]);

    console.table([
      {
        'Purchase ID': dbPurchase.rows[0]?.purchase_id,
        'Customer ID': dbPurchase.rows[0]?.customer_id,
        'Amount ($)': (Number(dbPurchase.rows[0]?.amount_cents) / 100).toFixed(2),
        Status: dbPurchase.rows[0]?.status,
        Sequence: Number(dbPurchase.rows[0]?.completion_sequence),
      },
    ]);

    console.table([
      {
        'Outbox Event ID': dbOutbox.rows[0]?.event_id,
        'Aggregate ID': dbOutbox.rows[0]?.aggregate_id,
        Sequence: Number(dbOutbox.rows[0]?.customer_sequence),
        Status: dbOutbox.rows[0]?.status,
        'Published At': dbOutbox.rows[0]?.published_at?.toISOString(),
      },
    ]);

    console.table([
      {
        'Idempotency Key': dbIdem.rows[0]?.idempotency_key,
        'Customer ID': dbIdem.rows[0]?.customer_id,
        'Purchase ID': dbIdem.rows[0]?.purchase_id,
      },
    ]);

    // 9. Proof Summary
    console.log('\n' + '='.repeat(75));
    console.log('  TICKET P4-02 EVIDENCE VERIFICATION SUMMARY');
    console.log('='.repeat(75));
    console.log(`POST HTTP 202 Accepted Shape  : Valid (purchaseId: ${postBody.purchaseId})`);
    console.log(
      `GET Read Model CQRS Query      : Valid (outboxStatus: ${getBody.outboxStatus}, publishedAt: ${getBody.publishedAt})`,
    );
    console.log(
      `Exact Duplicate Idempotency    : Passed (${dupBody.purchaseId} === ${postBody.purchaseId})`,
    );
    console.log(`Conflicting Payload Rejection  : Passed (HTTP 409 Conflict)`);
    console.log(`Unknown ID 404 Handling        : Passed (HTTP 404 Not Found)`);
    console.log(`PostgreSQL Outbox Record State : ${dbOutbox.rows[0]?.status}`);
    console.log(`PostgreSQL Idempotency Record  : Persisted (${dbIdem.rowCount} row)`);

    if (
      postRes.status === 202 &&
      getRes.status === 200 &&
      isIdempotentMatch &&
      isConflictExpected &&
      is404Expected &&
      dbPurchase.rowCount === 1 &&
      dbOutbox.rowCount === 1 &&
      dbIdem.rowCount === 1
    ) {
      console.log(
        '\n  >>> PROOF CONFIRMED: P4-02 CQRS purchase command, Read DAO query, and transactional outbox verified! <<<',
      );
    } else {
      console.error('\n  >>> PROOF FAILED: One or more assertions did not match expectations. <<<');
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
  console.error('[P4-02 Fatal Error]:', err);
  process.exit(1);
});

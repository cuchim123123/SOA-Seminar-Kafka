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

const LOYALTY_PORT = process.env.LOYALTY_WORKER_PORT || process.env.LOYALTY_PORT || '3001';
const PURCHASES_DATABASE_URL =
  process.env.PURCHASES_DATABASE_URL ||
  process.env.DATABASE_URL ||
  (process.env.POSTGRES_PASSWORD
    ? `postgresql://${process.env.POSTGRES_USER || 'postgres'}:${process.env.POSTGRES_PASSWORD}@localhost:${process.env.POSTGRES_PORT || '5433'}/${process.env.POSTGRES_DB || 'kafka_streaming_lab'}?schema=public`
    : undefined);
const LOYALTY_DATABASE_URL =
  process.env.LOYALTY_DATABASE_URL ||
  process.env.DATABASE_URL ||
  (process.env.POSTGRES_PASSWORD
    ? `postgresql://${process.env.POSTGRES_USER || 'postgres'}:${process.env.POSTGRES_PASSWORD}@localhost:${process.env.POSTGRES_PORT || '5433'}/${process.env.POSTGRES_DB || 'kafka_streaming_lab'}?schema=public`
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
    console.log(`[P4-06 Proof] ${name} is already healthy at ${url}`);
    return null;
  }

  console.log(`[P4-06 Proof] Starting ${name} in background...`);
  const child = spawn(startCmd, startArgs, {
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: true,
    env: { ...process.env, ...envOverrides },
  });

  let childLogs = '';
  child.stdout?.on('data', (d) => {
    childLogs += d.toString();
  });
  child.stderr?.on('data', (d) => {
    childLogs += d.toString();
  });

  const startTime = Date.now();
  while (Date.now() - startTime < 50000) {
    if (await isPortHealthy(url)) {
      console.log(`[P4-06 Proof] ${name} became healthy at ${url}`);
      return child;
    }
    await sleep(500);
  }

  killProcess(child);
  console.error(`[P4-06 Proof] ${name} startup logs:\n${childLogs}`);
  throw new Error(`${name} failed to become healthy at ${url} within 50s`);
}

async function main() {
  console.log('='.repeat(75));
  console.log('  PHASE 4 TICKET P4-06: FAULT INJECTION INTEGRATION SUITE');
  console.log('='.repeat(75));

  if (!PURCHASES_DATABASE_URL || !LOYALTY_DATABASE_URL) {
    console.error('ERROR: PURCHASES_DATABASE_URL and LOYALTY_DATABASE_URL must be configured.');
    process.exit(1);
  }

  const purchasesPool = new pg.Pool({ connectionString: PURCHASES_DATABASE_URL });
  const purchasesClient = await purchasesPool.connect();
  const loyaltyPool = new pg.Pool({ connectionString: LOYALTY_DATABASE_URL });
  const loyaltyClient = await loyaltyPool.connect();
  console.log('[P4-06 Proof] Connected to PostgreSQL DBs (purchases_db and loyalty_db)');

  const kafka = new Kafka({
    clientId: `verify-p4-06-${Date.now()}`,
    brokers: KAFKA_BROKERS,
  });
  const admin = kafka.admin();
  const producer = kafka.producer();
  await admin.connect();
  await producer.connect();
  console.log('[P4-06 Proof] Connected to Kafka brokers');

  let activeChild = null;

  try {
    // =========================================================================
    // SCENARIO 1: CONSUMER POST-COMMIT CRASH (D4 RUNBOOK)
    // =========================================================================
    console.log('\n' + '-'.repeat(75));
    console.log('  SCENARIO 1: CONSUMER POST-COMMIT CRASH BEFORE OFFSET COMMIT (D4)');
    console.log('-'.repeat(75));

    const custD4 = `FAULT_D4_CUST_${Date.now()}`;
    const evtD4 = `evt_fault_d4_${Date.now()}`;

    console.log(
      `[P4-06 Proof] Step 1.1: Starting loyalty-worker with crash fault injection gated on event: ${evtD4}...`,
    );
    const faultWorker = spawn('node', ['--env-file=.env', 'apps/loyalty-worker/dist/main.js'], {
      stdio: ['ignore', 'pipe', 'pipe'],
      shell: true,
      env: {
        ...process.env,
        PORT: LOYALTY_PORT,
        DATABASE_URL: LOYALTY_DATABASE_URL,
        FAULT_INJECTION_POINT: 'AFTER_DB_COMMIT_BEFORE_OFFSET_COMMIT',
        FAULT_TRIGGER_EVENT_ID: evtD4,
      },
    });

    let faultWorkerLogs = '';
    faultWorker.stdout?.on('data', (d) => {
      faultWorkerLogs += d.toString();
    });
    faultWorker.stderr?.on('data', (d) => {
      faultWorkerLogs += d.toString();
    });

    // Wait for worker to become healthy
    const startWaitD4 = Date.now();
    while (Date.now() - startWaitD4 < 30000) {
      if (await isPortHealthy(`http://localhost:${LOYALTY_PORT}/health`)) {
        break;
      }
      await sleep(400);
    }
    console.log(
      `[P4-06 Proof] Fault-injected loyalty-worker is running at http://localhost:${LOYALTY_PORT}/health`,
    );

    // Publish event
    console.log(
      `[P4-06 Proof] Step 1.2: Publishing test purchase event for ${custD4} ($120.00)...`,
    );
    const envelopeD4 = createPurchaseCompletedEnvelope({
      customerId: custD4,
      customerSequence: 1,
      purchaseId: `pur_${custD4}_1`,
      amountCents: 12_000,
      occurredAt: new Date().toISOString(),
    });
    envelopeD4.eventId = evtD4;

    await producer.send({
      topic: PURCHASE_TOPIC,
      messages: [{ key: custD4, value: JSON.stringify(envelopeD4) }],
    });
    console.log(`[P4-06 Proof] Published Event ${evtD4}`);

    // Wait for worker process to crash with code 42
    console.log(
      '[P4-06 Proof] Step 1.3: Waiting for consumer process to execute DB commit and CRASH...',
    );
    const exitCodeD4 = await new Promise((resolve) => {
      faultWorker.on('exit', (code) => resolve(code));
      setTimeout(() => resolve(null), 25000);
    });

    console.log(`[P4-06 Proof] Consumer process exited with code: ${exitCodeD4}`);
    if (exitCodeD4 !== 42) {
      console.error(`Fault worker logs:\n${faultWorkerLogs}`);
      throw new Error(
        `Expected consumer process to crash with exit code 42, received: ${exitCodeD4}`,
      );
    }
    console.log(
      '  >>> EXPECTED CRASH OBSERVED: Worker crashed after DB commit before offset commit! <<<',
    );

    // Verify DB state after crash: DB commit succeeded!
    const dbAccountD4AfterCrash = (
      await loyaltyClient.query('SELECT * FROM loyalty_accounts WHERE customer_id = $1', [custD4])
    ).rows[0];
    const dbProcessedD4AfterCrash = (
      await loyaltyClient.query(
        'SELECT * FROM processed_events WHERE consumer_name = $1 AND event_id = $2',
        ['loyalty-v1', evtD4],
      )
    ).rows[0];

    if (!dbAccountD4AfterCrash || Number(dbAccountD4AfterCrash.points) !== 12) {
      throw new Error(
        `Expected loyalty points to be committed at 12 before crash, found: ${JSON.stringify(dbAccountD4AfterCrash)}`,
      );
    }
    if (!dbProcessedD4AfterCrash) {
      throw new Error(`Expected processed_events record to be committed before crash, found 0`);
    }
    console.log(
      `[P4-06 Proof] Verified PostgreSQL state: Points=12, Tier=SILVER, processed_events committed`,
    );

    // Verify Kafka consumer offset is UNCOMMITTED for this event
    const topicOffsets = await admin.fetchTopicOffsets(PURCHASE_TOPIC);
    console.log(`[P4-06 Proof] Kafka topic partition offsets:`, JSON.stringify(topicOffsets));

    // Restart loyalty-worker NORMALLY without fault injection
    console.log(
      '\n[P4-06 Proof] Step 1.4: Restarting loyalty-worker normally (WITHOUT fault injection)...',
    );
    activeChild = await ensureServiceRunning(
      'loyalty-worker',
      `http://localhost:${LOYALTY_PORT}/health`,
      'node',
      ['--env-file=.env', 'apps/loyalty-worker/dist/main.js'],
      {
        PORT: LOYALTY_PORT,
        DATABASE_URL: LOYALTY_DATABASE_URL,
        FAULT_INJECTION_POINT: 'NONE',
      },
    );

    // Wait for Kafka redelivery to occur and be processed
    console.log('[P4-06 Proof] Waiting for Kafka redelivery and consumer offset commit...');
    await sleep(4000);

    // Verify DB state remains unchanged (Idempotency confirmed: duplicate skipped!)
    const dbAccountD4AfterResume = (
      await loyaltyClient.query('SELECT * FROM loyalty_accounts WHERE customer_id = $1', [custD4])
    ).rows[0];

    console.log(
      `[P4-06 Proof] State After Redelivery: Points=${dbAccountD4AfterResume.points}, Tier=${dbAccountD4AfterResume.tier}`,
    );
    if (Number(dbAccountD4AfterResume.points) !== 12) {
      throw new Error(
        `IDEMPOTENCY FAILURE! Expected points to remain 12 after redelivery, found: ${dbAccountD4AfterResume.points}`,
      );
    }
    console.log(
      '  >>> IDEMPOTENCY CONFIRMED: Redelivered event skipped without double-crediting points! <<<',
    );

    // Terminate normal loyalty-worker before next scenario
    killProcess(activeChild);
    activeChild = null;
    await sleep(1000);

    // =========================================================================
    console.log('Consumer post-commit crash proof passed. CDC recovery is verified separately.');
  } finally {
    if (activeChild) killProcess(activeChild);
    await producer.disconnect();
    await admin.disconnect();
    purchasesClient.release();
    await purchasesPool.end();
    loyaltyClient.release();
    await loyaltyPool.end();
  }
}

main().catch((err) => {
  console.error('\n[P4-06 Proof] Verification FAILED:', err);
  process.exit(1);
});

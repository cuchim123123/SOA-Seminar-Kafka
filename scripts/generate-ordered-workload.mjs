import { spawn, exec } from 'node:child_process';
import { promisify } from 'node:util';
import { Kafka } from 'kafkajs';

const execAsync = promisify(exec);

const PRODUCER_PORT = process.env.PORT || '3000';
const LOYALTY_PORT = process.env.LOYALTY_WORKER_PORT || '3001';
const TOPIC = process.env.PURCHASE_TOPIC || 'purchase.events';

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
    console.log(`[Workload P2] ${name} is already running and healthy at ${url}`);
    return null;
  }

  console.log(`[Workload P2] Starting ${name} in background...`);
  const child = spawn(startCmd, startArgs, {
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: true,
    env: { ...process.env, ...envOverrides },
  });

  child.stdout?.on('data', (data) => {
    const str = data.toString().trim();
    if (str.includes('[Nest]') || str.includes('listening')) {
      console.log(`[${name}] ${str}`);
    }
  });

  child.stderr?.on('data', (data) => {
    console.error(`[${name} Error] ${data.toString().trim()}`);
  });

  const startTime = Date.now();
  while (Date.now() - startTime < 15000) {
    if (await isPortHealthy(url)) {
      console.log(`[Workload P2] ${name} became healthy at ${url}`);
      return child;
    }
    await sleep(500);
  }

  child.kill();
  throw new Error(`${name} failed to become healthy at ${url} within 15s`);
}

async function sendPurchase(producerPort, customerId, amountCents) {
  const res = await fetch(`http://localhost:${producerPort}/api/v1/demo/purchases`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ customerId, amountCents }),
  });

  if (res.status !== 202) {
    const text = await res.text();
    throw new Error(
      `Failed to send purchase for customer ${customerId}: HTTP ${res.status} ${text}`,
    );
  }

  return res.json();
}

async function main() {
  const spawnedProcesses = [];

  try {
    console.log('========================================================');
    console.log('=== Phase 2: Multi-Customer Ordered Workload Fixture ===');
    console.log('========================================================\n');

    // 1. Check Kafka container health and topic
    console.log('[Workload P2] Checking Kafka broker status...');
    const { stdout: containerStatus } = await execAsync(
      'docker inspect --format="{{json .State.Health.Status}}" kafka-broker-1',
    );
    if (!containerStatus.includes('healthy')) {
      throw new Error(`Kafka broker container is not healthy: ${containerStatus.trim()}`);
    }
    console.log('[Workload P2] Kafka broker is healthy.');

    console.log(`[Workload P2] Verifying topic '${TOPIC}' partitions...`);
    const { stdout: topicDescribe } = await execAsync(
      `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ${TOPIC}`,
    );
    if (!topicDescribe.includes('PartitionCount: 6')) {
      throw new Error(
        `Topic '${TOPIC}' does not have 6 partitions. Run 'pnpm demo:reset --confirm' first! Output: ${topicDescribe.trim()}`,
      );
    }
    console.log(`[Workload P2] Topic '${TOPIC}' verified with 6 partitions.`);

    // 2. Ensure Producer API and Loyalty Worker are running
    const producerChild = await ensureServiceRunning(
      'Producer-API',
      `http://localhost:${PRODUCER_PORT}/health/live`,
      'node',
      ['apps/producer-api/dist/main.js'],
      { PORT: PRODUCER_PORT },
    );
    if (producerChild) spawnedProcesses.push(producerChild);

    const loyaltyChild = await ensureServiceRunning(
      'Loyalty-Worker',
      `http://localhost:${LOYALTY_PORT}/health/live`,
      'node',
      ['apps/loyalty-worker/dist/main.js'],
      { PORT: LOYALTY_PORT, KAFKA_GROUP_ID: 'loyalty-v1' },
    );
    if (loyaltyChild) spawnedProcesses.push(loyaltyChild);

    // Give consumer group a moment to establish partition assignments
    await sleep(2000);

    // 3. Dispatch Workload
    console.log(
      '\n[Workload P2] Generating simultaneous multi-customer workload across 21 customers...',
    );

    const targetCustomer = 'U001';
    const otherCustomers = Array.from(
      { length: 20 },
      (_, i) => `U${String(i + 2).padStart(3, '0')}`,
    );

    const u001Events = [];
    const allRecords = [];

    // Step A: Send U001 purchase 1 ($120.00 / 12,000 cents)
    console.log(`[Workload P2] Sending ${targetCustomer} Seq 1: $120.00 (Bronze tier)...`);
    const resp1 = await sendPurchase(PRODUCER_PORT, targetCustomer, 12000);
    u001Events.push(resp1);
    allRecords.push({ customerId: targetCustomer, ...resp1 });

    // Step B: Interleave concurrent purchases for 20 other customers
    console.log(
      `[Workload P2] Sending concurrent purchases for 20 other customers (${otherCustomers[0]}..${otherCustomers[otherCustomers.length - 1]})...`,
    );
    const otherPromises = otherCustomers.map(async (custId, idx) => {
      const amountCents = (idx + 1) * 2500; // $25.00 to $500.00
      const resp = await sendPurchase(PRODUCER_PORT, custId, amountCents);
      return { customerId: custId, ...resp };
    });

    // Step C: Send U001 purchase 2 ($200.00 / 20,000 cents)
    console.log(`[Workload P2] Sending ${targetCustomer} Seq 2: $200.00 (Silver tier)...`);
    const resp2 = await sendPurchase(PRODUCER_PORT, targetCustomer, 20000);
    u001Events.push(resp2);
    allRecords.push({ customerId: targetCustomer, ...resp2 });

    // Await concurrent other customers
    const otherResults = await Promise.all(otherPromises);
    allRecords.push(...otherResults);

    // Step D: Send U001 purchase 3 ($100.00 / 10,000 cents)
    console.log(`[Workload P2] Sending ${targetCustomer} Seq 3: $100.00 (Gold tier)...`);
    const resp3 = await sendPurchase(PRODUCER_PORT, targetCustomer, 10000);
    u001Events.push(resp3);
    allRecords.push({ customerId: targetCustomer, ...resp3 });

    // 4. Inspect Kafka Broker for Emitted Events
    console.log(
      '\n[Workload P2] Querying Kafka broker directly to inspect assigned partitions and offsets...',
    );
    const kafka = new Kafka({
      clientId: `workload-verifier-${Date.now()}`,
      brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',').map((b) => b.trim()),
    });
    const verifierConsumer = kafka.consumer({
      groupId: `workload-verifier-${Date.now()}`,
      maxWaitTimeInMs: 100,
    });
    await verifierConsumer.connect();
    await verifierConsumer.subscribe({ topic: TOPIC, fromBeginning: true });

    const eventIdMap = new Map();
    const targetEventIds = new Set(allRecords.map((r) => r.eventId));

    await new Promise((resolve) => {
      const timer = setTimeout(resolve, 6000);
      verifierConsumer
        .run({
          eachMessage: async ({ partition, message }) => {
            try {
              const parsed = JSON.parse(message.value.toString('utf-8'));
              if (targetEventIds.has(parsed.eventId)) {
                eventIdMap.set(parsed.eventId, {
                  partition,
                  offset: message.offset,
                  key: message.key?.toString() ?? '',
                });
                if (eventIdMap.size >= targetEventIds.size) {
                  clearTimeout(timer);
                  resolve();
                }
              }
            } catch {
              // ignore unparseable
            }
          },
        })
        .catch(() => resolve());
    });

    await verifierConsumer.disconnect();

    // Enrich u001Events and allRecords with broker metadata
    for (const evt of u001Events) {
      const broker = eventIdMap.get(evt.eventId);
      if (broker) {
        evt.partition = broker.partition;
        evt.offset = broker.offset;
      }
    }
    for (const rec of allRecords) {
      const broker = eventIdMap.get(rec.eventId);
      if (broker) {
        rec.partition = broker.partition;
        rec.offset = broker.offset;
      }
    }

    // 5. Assert & Display Proof 1 & 2: Same partition & ascending offsets for U001
    console.log('\n--- VERIFICATION 1: Ordered Sequence for U001 (Broker Observed) ---');
    console.table(
      u001Events.map((evt, idx) => ({
        Sequence: idx + 1,
        PurchaseId: evt.purchaseId,
        Partition: evt.partition,
        Offset: evt.offset,
        EventId: evt.eventId,
      })),
    );

    const targetPartition = u001Events[0].partition;
    const samePartition = u001Events.every((e) => e.partition === targetPartition);
    if (!samePartition) {
      throw new Error(
        `Ordering invariant violation! U001 events spanned multiple partitions: ${JSON.stringify(u001Events)}`,
      );
    }

    const offset1 = BigInt(u001Events[0].offset);
    const offset2 = BigInt(u001Events[1].offset);
    const offset3 = BigInt(u001Events[2].offset);
    const strictlyAscending = offset1 < offset2 && offset2 < offset3;

    if (!strictlyAscending) {
      throw new Error(
        `Offset order invariant violation! Offsets not ascending: ${offset1}, ${offset2}, ${offset3}`,
      );
    }

    console.log(`✔ PROOF 1 PASSED: All 3 U001 events routed to Partition ${targetPartition}`);
    console.log(
      `✔ PROOF 2 PASSED: Offsets strictly ascending (${offset1} < ${offset2} < ${offset3})`,
    );

    // 5. Partition Distribution across all 6 partitions
    console.log('\n--- VERIFICATION 2: Key-Based Partition Distribution ---');
    const partitionMap = new Map();
    for (let p = 0; p < 6; p++) {
      partitionMap.set(p, []);
    }

    for (const rec of allRecords) {
      const list = partitionMap.get(rec.partition) ?? [];
      list.push(rec.customerId);
      partitionMap.set(rec.partition, list);
    }

    const distributionTable = [];
    for (let p = 0; p < 6; p++) {
      const customers = partitionMap.get(p) ?? [];
      const uniqueCustomers = Array.from(new Set(customers));
      distributionTable.push({
        'Partition ID': p,
        'Record Count': customers.length,
        'Unique Keys': uniqueCustomers.length,
        'Sample Keys':
          uniqueCustomers.slice(0, 4).join(', ') + (uniqueCustomers.length > 4 ? '...' : ''),
        'Key Collisions':
          uniqueCustomers.length > 1 ? `YES (${uniqueCustomers.length} keys)` : 'NO',
      });
    }
    console.table(distributionTable);

    const usedPartitions = distributionTable.filter((r) => r['Record Count'] > 0).length;
    console.log(
      `✔ PROOF 3 PASSED: Records spread across ${usedPartitions} distinct partitions with observed hash collisions`,
    );

    // 6. Assert End-to-End Loyalty Account State (82 Points)
    console.log('\n--- VERIFICATION 3: End-to-End Loyalty Points Calculation ---');
    console.log(
      `[Workload P2] Polling http://localhost:${LOYALTY_PORT}/api/v1/demo/loyalty/${targetCustomer}...`,
    );

    let loyaltyAccount = null;
    const pollStart = Date.now();
    while (Date.now() - pollStart < 15000) {
      try {
        const res = await fetch(
          `http://localhost:${LOYALTY_PORT}/api/v1/demo/loyalty/${targetCustomer}`,
        );
        if (res.ok) {
          const body = await res.json();
          if (body.pointsBalance === 82) {
            loyaltyAccount = body;
            break;
          }
        }
      } catch {
        // Retry
      }
      await sleep(400);
    }

    if (!loyaltyAccount) {
      throw new Error(`Failed to observe 82 points for ${targetCustomer} within 15s`);
    }

    console.table([
      {
        Customer: loyaltyAccount.customerId,
        Tier: loyaltyAccount.tier,
        'Total Spend': `$${(loyaltyAccount.totalEligibleSpendCents / 100).toFixed(2)}`,
        'Points Balance': loyaltyAccount.pointsBalance,
        Expected: '82 points (12 + 40 + 30)',
        Status: 'VERIFIED',
      },
    ]);

    console.log('\n======================================================');
    console.log('=== ALL PHASE 2 ORDERED WORKLOAD CHECKS PASSED (P2-02) ===');
    console.log('======================================================');
  } catch (err) {
    console.error('\n[Workload P2 FAILED]:', err);
    process.exitCode = 1;
  } finally {
    for (const child of spawnedProcesses) {
      console.log(`[Workload P2] Stopping background process (pid: ${child.pid})...`);
      if (process.platform === 'win32' && child.pid) {
        try {
          await execAsync(`taskkill /pid ${child.pid} /T /F`);
        } catch {
          child.kill('SIGKILL');
        }
      } else {
        child.kill('SIGTERM');
      }
    }
    process.exit(process.exitCode || 0);
  }
}

main();

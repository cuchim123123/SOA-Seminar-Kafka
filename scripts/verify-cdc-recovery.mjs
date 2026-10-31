import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
import assert from 'node:assert/strict';

const require = createRequire(new URL('../apps/producer-api/package.json', import.meta.url));
const { Kafka } = require('kafkajs');
const { Pool } = require('pg');
const { z } = require('zod');
const { createPurchaseCompletedEnvelope, deserializeEnvelope } = require('@lab/contracts');
try {
  process.loadEnvFile('.env');
} catch {
  /* injected environment */
}
const config = z
  .object({
    PURCHASES_DATABASE_URL: z.string().url(),
    KAFKA_BROKERS: z.string().default('localhost:9092'),
    PURCHASE_TOPIC: z.string().default('purchase.events'),
    KAFKA_CONNECT_URL: z.string().url().default('http://localhost:8083'),
    CDC_CONNECTOR_NAME: z
      .string()
      .regex(/^[a-zA-Z0-9_-]+$/)
      .default('purchases-outbox-connector'),
    CDC_READY_TIMEOUT_MS: z.coerce.number().positive().default(60000),
    CDC_PAUSE_ABSENCE_WINDOW_MS: z.coerce.number().positive().default(3000),
  })
  .parse(process.env);

const connectUrl = `${config.KAFKA_CONNECT_URL}/connectors/${encodeURIComponent(
  config.CDC_CONNECTOR_NAME,
)}`;

async function fetchConnectorStatus(timeoutMs = 5000) {
  const response = await fetch(connectUrl + '/status', { signal: AbortSignal.timeout(timeoutMs) });
  if (!response.ok) throw new Error(`Connector status HTTP ${response.status}`);
  return response.json();
}

async function waitUntilConnectorState(description, predicate, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let lastStates = '(no status observed)';
  while (Date.now() < deadline) {
    const status = await fetchConnectorStatus().catch(() => null);
    if (status) {
      lastStates = `connector=${status.connector.state}, tasks=[${status.tasks.map((task) => task.state).join(',') || 'none'}]`;
      if (predicate(status)) return status;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`${description} timed out: last state ${lastStates}`);
}

const CONNECTOR_RUNNING = (status) =>
  status.connector.state === 'RUNNING' &&
  status.tasks.length > 0 &&
  status.tasks.every((task) => task.state === 'RUNNING');

const CONNECTOR_PAUSED = (status) =>
  status.connector.state === 'PAUSED' &&
  status.tasks.length > 0 &&
  status.tasks.every((task) => task.state === 'PAUSED');

async function resumeIfPausedOrStopped() {
  // Self-healing: a previous crashed run may have left the connector paused.
  const status = await fetchConnectorStatus();
  if (status.connector.state !== 'RUNNING') {
    const resumeResponse = await fetch(`${connectUrl}/resume`, {
      method: 'PUT',
      signal: AbortSignal.timeout(10000),
    });
    console.log(
      `[RECOVER] Entry state ${status.connector.state}; resume HTTP ${resumeResponse.status}.`,
    );
  }
}

const id = `cdc-recovery-${randomUUID()}`;
const pausedDeliveries = [];
const postResumeDeliveries = [];

const pool = new Pool({ connectionString: config.PURCHASES_DATABASE_URL });
const consumer = new Kafka({ clientId: id, brokers: config.KAFKA_BROKERS.split(',') }).consumer({
  groupId: id,
});
const envelope = createPurchaseCompletedEnvelope({
  eventId: randomUUID(),
  customerId: id,
  purchaseId: `${id}-1`,
  amountCents: 8500,
  customerSequence: 1,
  correlationId: id,
});
let pausedFrom;
let resumedAt;
let deliveredAt;

try {
  // 0. Self-heal a connector left paused by a crashed run, then require RUNNING.
  await resumeIfPausedOrStopped();
  await waitUntilConnectorState(
    'Running precondition',
    CONNECTOR_RUNNING,
    config.CDC_READY_TIMEOUT_MS,
  );

  // 1. Subscriber joins BEFORE the outage so nothing can slip through unnoticed.
  await consumer.connect();
  await consumer.subscribe({ topic: config.PURCHASE_TOPIC, fromBeginning: false });
  const joined = new Promise((resolve) => consumer.on(consumer.events.GROUP_JOIN, resolve));
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ message, partition }) => {
      if (message.key?.toString() !== id) return;
      const value = { eventId: deserializeEnvelope(message.value.toString()).eventId, partition };
      if (pausedFrom && !resumedAt) pausedDeliveries.push(value);
      else postResumeDeliveries.push({ ...value, key: message.key.toString() });
    },
  });
  await Promise.race([
    joined,
    new Promise((_, reject) =>
      setTimeout(
        () => reject(new Error('Verifier group join timeout')),
        config.CDC_READY_TIMEOUT_MS,
      ).unref(),
    ),
  ]);

  // 2. Pause the connector via REST (bounded; slot and offsets stay intact).
  const pauseResponse = await fetch(`${connectUrl}/pause`, {
    method: 'PUT',
    signal: AbortSignal.timeout(10000),
  });
  if (!pauseResponse.ok) throw new Error(`Connector pause failed: HTTP ${pauseResponse.status}`);
  await waitUntilConnectorState(
    'Connector pause propagation',
    CONNECTOR_PAUSED,
    config.CDC_READY_TIMEOUT_MS,
  );

  // 3. Commit the source event while publication is paused.
  pausedFrom = Date.now();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      'INSERT INTO purchase_customers (customer_id, next_event_sequence) VALUES ($1,2)',
      [id],
    );
    await client.query(
      "INSERT INTO purchases (purchase_id, customer_id, amount_cents, status, completion_sequence) VALUES ($1,$2,$3,'COMPLETED',$4)",
      [envelope.payload.purchaseId, id, envelope.payload.amountCents, envelope.customerSequence],
    );
    await client.query(
      'INSERT INTO outbox_events (event_id, aggregate_id, customer_sequence, event_type, payload_json) VALUES ($1,$2,$3,$4,$5)',
      [
        envelope.eventId,
        id,
        envelope.customerSequence,
        envelope.eventType,
        JSON.stringify(envelope),
      ],
    );
    await client.query('COMMIT');
  } finally {
    client.release();
  }

  // 4. Bounded absence window: a paused connector must not deliver anything.
  await new Promise((resolve) => setTimeout(resolve, config.CDC_PAUSE_ABSENCE_WINDOW_MS));
  assert.deepEqual(
    pausedDeliveries,
    [],
    'Paused connector must not deliver source events during the outage window',
  );
  console.log(
    `[PAUSED] No delivery in ${config.CDC_PAUSE_ABSENCE_WINDOW_MS}ms while source event ${envelope.eventId} was committed.`,
  );

  // 5. Resume and verify eventual delivery with retained WAL.
  resumedAt = Date.now();
  const resumeResponse = await fetch(`${connectUrl}/resume`, {
    method: 'PUT',
    signal: AbortSignal.timeout(10000),
  });
  if (!resumeResponse.ok) throw new Error(`Connector resume failed: HTTP ${resumeResponse.status}`);
  await waitUntilConnectorState('Connector resume', CONNECTOR_RUNNING, config.CDC_READY_TIMEOUT_MS);

  const deadline = resumedAt + config.CDC_READY_TIMEOUT_MS;
  while (postResumeDeliveries.length === 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  deliveredAt = Date.now();
  assert.ok(
    postResumeDeliveries.length > 0,
    'Committed source event must be delivered after connector resume',
  );

  const delivery = postResumeDeliveries[0];
  assert.equal(delivery.eventId, envelope.eventId);
  assert.equal(delivery.key, id);
  assert.ok(Number.isInteger(delivery.partition));
  const deliveredEnvelopes = [...pausedDeliveries, ...postResumeDeliveries];
  assert.ok(
    deliveredEnvelopes.every((record) => record.eventId === envelope.eventId),
    'Every delivery attempt must carry the stable event ID',
  );

  console.log(
    JSON.stringify({
      status: 'PASSED',
      customerId: id,
      eventId: envelope.eventId,
      partition: delivery.partition,
      pauseAbsenceWindowMs: config.CDC_PAUSE_ABSENCE_WINDOW_MS,
      pausedWithoutDelivery: true,
      deliveryLatencyAfterResumeMs: deliveredAt - resumedAt,
    }),
  );
} finally {
  // Best-effort resume so a failed run cannot leave the lab without publication.
  await fetch(`${connectUrl}/resume`, { method: 'PUT', signal: AbortSignal.timeout(5000) }).catch(
    () => {},
  );
  await consumer.disconnect().catch(() => {});
  await pool.end().catch(() => {});
}

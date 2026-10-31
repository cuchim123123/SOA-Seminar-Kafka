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
    CDC_READY_TIMEOUT_MS: z.coerce.number().positive().default(60000),
  })
  .parse(process.env);
const id = `cdc-proof-${randomUUID()}`;
const pool = new Pool({ connectionString: config.PURCHASES_DATABASE_URL });
const consumer = new Kafka({ clientId: id, brokers: config.KAFKA_BROKERS.split(',') }).consumer({
  groupId: id,
});
const received = [];
const events = [1, 2, 3, 4].map((customerSequence) =>
  createPurchaseCompletedEnvelope({
    eventId: randomUUID(),
    customerId: id,
    purchaseId: `${id}-${customerSequence}`,
    amountCents: 12000,
    customerSequence,
    correlationId: id,
  }),
);
let client;
try {
  await consumer.connect();
  await consumer.subscribe({ topic: config.PURCHASE_TOPIC, fromBeginning: false });
  const joined = new Promise((resolve) => consumer.on(consumer.events.GROUP_JOIN, resolve));
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ message, partition }) => {
      if (message.key?.toString() !== id) return;
      const envelope = deserializeEnvelope(message.value.toString());
      received.push({ envelope, partition });
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
  client = await pool.connect();
  const insert = async (envelope) => {
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
  };
  // A rolled-back insert must never reach Kafka.
  await client.query('BEGIN');
  await insert(events[3]);
  await client.query('ROLLBACK');
  await client.query('BEGIN');
  await client.query(
    'INSERT INTO purchase_customers (customer_id, next_event_sequence) VALUES ($1,4)',
    [id],
  );
  for (const envelope of events.slice(0, 3)) {
    await client.query(
      "INSERT INTO purchases (purchase_id, customer_id, amount_cents, status, completion_sequence) VALUES ($1,$2,$3,'COMPLETED',$4)",
      [envelope.payload.purchaseId, id, envelope.payload.amountCents, envelope.customerSequence],
    );
    await insert(envelope);
  }
  await client.query('COMMIT');
  const deadline = Date.now() + config.CDC_READY_TIMEOUT_MS;
  while (
    new Set(received.map((record) => record.envelope.eventId)).size < 3 &&
    Date.now() < deadline
  ) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  if (new Set(received.map((record) => record.envelope.eventId)).size < 3) {
    throw new Error(
      `CDC delivery timed out: received ${new Set(received.map((record) => record.envelope.eventId)).size} of 3 unique committed events`,
    );
  }

  // Quiet window: give late/undelivered or rolled-back records a bounded chance to
  // appear before asserting exclusion. CDC is asynchronous; RUNNING alone proves nothing.
  const QUIET_WINDOW_MS = 2000;
  await new Promise((resolve) => setTimeout(resolve, QUIET_WINDOW_MS));

  const unique = [...new Map(received.map((record) => [record.envelope.eventId, record])).values()];
  const rolledBackEventId = events[3].eventId;
  assert.ok(
    !received.some((record) => record.envelope.eventId === rolledBackEventId),
    'Rolled-back outbox insert must never reach Kafka',
  );
  assert.deepEqual(
    unique.map((record) => record.envelope.eventId),
    events.slice(0, 3).map((event) => event.eventId),
    'Expected exactly the three committed envelopes in customer order',
  );
  assert.equal(new Set(unique.map((record) => record.partition)).size, 1);
  for (let i = 0; i < unique.length; i++) assert.deepEqual(unique[i].envelope, events[i]);
  console.log(
    JSON.stringify({
      status: 'PASSED',
      customerId: id,
      committedEvents: 3,
      deliveries: received.length,
      uniqueDeliveries: unique.length,
      partition: unique[0].partition,
      rollbackExcluded: true,
      quietWindowMs: QUIET_WINDOW_MS,
    }),
  );
} finally {
  if (client) {
    await client.query('ROLLBACK').catch(() => {});
    client.release();
  }
  await consumer.disconnect();
  await pool.end();
}

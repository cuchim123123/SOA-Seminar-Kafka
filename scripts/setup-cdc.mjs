import { createRequire } from 'node:module';
import { readFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';

const require = createRequire(new URL('../apps/producer-api/package.json', import.meta.url));
const { z } = require('zod');
const { Kafka } = require('kafkajs');
try {
  process.loadEnvFile('.env');
} catch {
  /* environment may be supplied externally */
}
const config = z
  .object({
    KAFKA_CONNECT_URL: z.string().url().default('http://localhost:8083'),
    CDC_CONNECTOR_NAME: z
      .string()
      .regex(/^[a-zA-Z0-9_-]+$/)
      .default('purchases-outbox-connector'),
    CDC_DATABASE_HOST: z.string().min(1).default('postgres'),
    CDC_DATABASE_PORT: z.coerce.number().int().positive().default(5432),
    PURCHASES_DATABASE_URL: z.string().url(),
    CDC_SLOT_NAME: z
      .string()
      .regex(/^[a-z0-9_]+$/)
      .default('debezium_outbox_slot'),
    CDC_PUBLICATION_NAME: z
      .string()
      .regex(/^[a-z0-9_]+$/)
      .default('debezium_outbox_pub'),
    PURCHASE_TOPIC: z.string().min(1).default('purchase.events'),
    PURCHASE_TOPIC_PARTITIONS: z.coerce.number().int().positive().default(6),
    KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
    CDC_READY_TIMEOUT_MS: z.coerce.number().int().positive().default(60000),
  })
  .parse(process.env);

// Run committed migrations before registering a source connector. Never reset data or offsets.
for (const app of ['producer-api', 'loyalty-worker', 'analytics-worker']) {
  const result = spawnSync('pnpm', ['--filter', `@lab/${app}`, 'prisma:migrate'], {
    shell: true,
    stdio: 'inherit',
  });
  if (result.status !== 0) throw new Error(`Migration failed for ${app}`);
}
const admin = new Kafka({
  clientId: 'cdc-bootstrap',
  brokers: config.KAFKA_BROKERS.split(','),
}).admin();
try {
  await admin.connect();
  if (!(await admin.listTopics()).includes(config.PURCHASE_TOPIC))
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic: config.PURCHASE_TOPIC,
          numPartitions: config.PURCHASE_TOPIC_PARTITIONS,
          replicationFactor: 1,
        },
      ],
    });
} finally {
  await admin.disconnect();
}

const template = JSON.parse(
  await readFile(new URL('../docker/debezium/outbox-connector.json', import.meta.url), 'utf8'),
);
const database = new URL(config.PURCHASES_DATABASE_URL);
const connectorConfig = {
  ...template.config,
  'database.hostname': config.CDC_DATABASE_HOST,
  'database.port': String(config.CDC_DATABASE_PORT),
  'database.user': decodeURIComponent(database.username),
  'database.password': decodeURIComponent(database.password),
  'database.dbname': database.pathname.slice(1),
  'slot.name': config.CDC_SLOT_NAME,
  'publication.name': config.CDC_PUBLICATION_NAME,
  'publication.autocreate.mode': 'all_tables',
  'transforms.outbox.route.topic.replacement': config.PURCHASE_TOPIC,
  'snapshot.mode': 'initial',
  'snapshot.select.statement.overrides': 'public.outbox_events',
  'snapshot.select.statement.overrides.public.outbox_events':
    'SELECT * FROM public.outbox_events ORDER BY aggregate_id, customer_sequence',
};
const endpoint = `${config.KAFKA_CONNECT_URL}/connectors/${encodeURIComponent(config.CDC_CONNECTOR_NAME)}`;
const deadline = Date.now() + config.CDC_READY_TIMEOUT_MS;
while (true) {
  try {
    const response = await fetch(`${config.KAFKA_CONNECT_URL}/connectors`, {
      signal: AbortSignal.timeout(5000),
    });
    if (response.ok) break;
  } catch {
    /* bounded readiness retry */
  }
  if (Date.now() >= deadline) throw new Error('Kafka Connect readiness timed out');
  await new Promise((resolve) => setTimeout(resolve, 1000));
}
const response = await fetch(`${endpoint}/config`, {
  method: 'PUT',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(connectorConfig),
  signal: AbortSignal.timeout(10000),
});
if (!response.ok) throw new Error(`Connector registration failed: HTTP ${response.status}`);

// Reconfiguration may briefly expose a previous failure; a task left FAILED with an
// unchanged configuration never revives on its own. Restart it in a bounded way
// without touching replication slots, offsets or source data.
const MAX_RESTART_ATTEMPTS = 3;
const RESTART_SPACING_MS = 5000;
let restartAttempts = 0;
let lastRestartAt = 0;
let lastStates = '(no status observed)';

while (Date.now() < deadline) {
  const status = await fetch(`${endpoint}/status`, { signal: AbortSignal.timeout(5000) })
    .then((res) => {
      if (!res.ok) return null;
      return res.json();
    })
    .catch(() => null);

  if (status) {
    const taskStates = status.tasks.map((task) => task.state);
    lastStates = `connector=${status.connector.state}, tasks=[${taskStates.join(',') || 'none'}]`;
    if (
      status.connector.state === 'RUNNING' &&
      taskStates.length > 0 &&
      taskStates.every((state) => state === 'RUNNING')
    ) {
      console.log(
        'Debezium connector and source tasks RUNNING. Validate delivery with cdc:verify.',
      );
      process.exit(0);
    }

    const hasFailedState =
      status.connector.state === 'FAILED' || taskStates.some((state) => state === 'FAILED');
    if (
      hasFailedState &&
      restartAttempts < MAX_RESTART_ATTEMPTS &&
      Date.now() - lastRestartAt >= RESTART_SPACING_MS
    ) {
      restartAttempts += 1;
      lastRestartAt = Date.now();
      console.log(
        `CDC task failed; requesting bounded restart ${restartAttempts}/${MAX_RESTART_ATTEMPTS} (${lastStates})...`,
      );
      await fetch(`${endpoint}/restart?includeTasks=true&onlyFailed=true`, {
        method: 'POST',
        signal: AbortSignal.timeout(10000),
      }).catch(() => {
        /* restart request may race a state transition; status polling decides the outcome */
      });
      await new Promise((resolve) => setTimeout(resolve, RESTART_SPACING_MS));
      continue;
    }
  }
  await new Promise((resolve) => setTimeout(resolve, 1000));
}
throw new Error(`CDC task readiness timed out: last state ${lastStates}`);

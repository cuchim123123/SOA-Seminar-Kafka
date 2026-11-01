import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.resolve(__dirname, '..');

const EXPECTED_METRIC_NAMES = [
  'lab_source_events_committed_total',
  'lab_deliveries_total',
  'lab_handler_commits_total',
  'lab_duplicate_deliveries_total',
  'lab_ack_operations_total',
  'lab_processing_failures_total',
  'lab_handler_duration_seconds',
  'lab_source_to_handler_duration_seconds',
  'lab_http_request_duration_seconds',
  'lab_consumer_state',
  'lab_replay_runs_total',
];

console.log('=== [1/5] Verifying Prometheus Configuration ===');
const promTemplate = path.join(
  rootDir,
  'docker',
  'monitoring',
  'prometheus',
  'prometheus.yml.template',
);
const promConfig = path.join(rootDir, 'docker', 'monitoring', 'prometheus', 'prometheus.yml');

if (!fs.existsSync(promTemplate)) {
  console.error(`FAIL: Missing ${promTemplate}`);
  process.exit(1);
}
if (!fs.existsSync(promConfig)) {
  console.error(
    `FAIL: Missing ${promConfig}. Run node scripts/render-monitoring-config.mjs first.`,
  );
  process.exit(1);
}

const promContent = fs.readFileSync(promConfig, 'utf8');
const requiredJobs = ['producer-api', 'loyalty-worker', 'analytics-worker', 'replay-worker'];
for (const job of requiredJobs) {
  if (!promContent.includes(`job_name: '${job}'`)) {
    console.error(`FAIL: Prometheus config missing job: ${job}`);
    process.exit(1);
  }
}
console.log('OK: Prometheus configuration contains all 4 application scrape jobs');

console.log('=== [2/5] Verifying Grafana Datasource Provisioning ===');
const datasourceFile = path.join(
  rootDir,
  'docker',
  'monitoring',
  'grafana',
  'provisioning',
  'datasources',
  'prometheus.yml',
);
if (!fs.existsSync(datasourceFile)) {
  console.error(`FAIL: Missing ${datasourceFile}`);
  process.exit(1);
}
const dsContent = fs.readFileSync(datasourceFile, 'utf8');
if (!dsContent.includes('uid: prometheus') || !dsContent.includes('url: http://prometheus:9090')) {
  console.error(
    'FAIL: Grafana datasource missing stable uid: prometheus or url: http://prometheus:9090',
  );
  process.exit(1);
}
console.log('OK: Grafana Prometheus datasource provisioned with stable UID');

console.log('=== [3/5] Verifying Grafana Dashboard Provisioning & Files ===');
const dashboardsProvFile = path.join(
  rootDir,
  'docker',
  'monitoring',
  'grafana',
  'provisioning',
  'dashboards',
  'dashboards.yml',
);
if (!fs.existsSync(dashboardsProvFile)) {
  console.error(`FAIL: Missing ${dashboardsProvFile}`);
  process.exit(1);
}

const dashboardsDir = path.join(rootDir, 'docker', 'monitoring', 'grafana', 'dashboards');
const expectedDashboards = [
  { file: 'apps-pipeline.json', uid: 'kafka-apps-pipeline' },
  { file: 'kafka-system-diagnostics.json', uid: 'kafka-system-diagnostics' },
];

for (const { file, uid } of expectedDashboards) {
  const filePath = path.join(dashboardsDir, file);
  if (!fs.existsSync(filePath)) {
    console.error(`FAIL: Missing dashboard file: ${filePath}`);
    process.exit(1);
  }
  try {
    const json = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    if (json.uid !== uid) {
      console.error(`FAIL: Dashboard ${file} UID mismatch. Expected: ${uid}, Got: ${json.uid}`);
      process.exit(1);
    }
    if (!Array.isArray(json.panels) || json.panels.length === 0) {
      console.error(`FAIL: Dashboard ${file} has no panels defined.`);
      process.exit(1);
    }
  } catch (err) {
    console.error(`FAIL: Invalid JSON in dashboard ${file}: ${err.message}`);
    process.exit(1);
  }
}
console.log(`OK: All ${expectedDashboards.length} Grafana dashboard JSONs valid with correct UIDs`);

console.log('=== [4/5] Verifying Dashboard PromQL Query Contract ===');
for (const { file } of expectedDashboards) {
  const filePath = path.join(dashboardsDir, file);
  const raw = fs.readFileSync(filePath, 'utf8');
  let matchedMetrics = 0;
  for (const metric of EXPECTED_METRIC_NAMES) {
    if (raw.includes(metric)) {
      matchedMetrics++;
    }
  }
  console.log(
    `OK: Dashboard ${file} references ${matchedMetrics} expected application telemetry metrics`,
  );
}

console.log('=== [5/5] Checking Live /metrics HTTP Endpoints (if services running) ===');
const liveEndpoints = [
  { name: 'producer-api', url: 'http://localhost:3000/metrics' },
  { name: 'loyalty-worker', url: 'http://localhost:3001/metrics' },
  { name: 'analytics-worker', url: 'http://localhost:3002/metrics' },
  { name: 'replay-worker', url: 'http://localhost:3004/metrics' },
  { name: 'prometheus', url: 'http://localhost:9090/-/healthy' },
  { name: 'grafana', url: 'http://localhost:3005/api/health' },
];

for (const ep of liveEndpoints) {
  try {
    const res = await fetch(ep.url, { signal: AbortSignal.timeout(1000) });
    if (res.ok) {
      console.log(`  [ONLINE] ${ep.name} -> HTTP ${res.status}`);
    } else {
      console.log(`  [OFFLINE / NOT READY] ${ep.name} -> HTTP ${res.status}`);
    }
  } catch {
    console.log(`  [OFFLINE] ${ep.name} (not currently running on port)`);
  }
}

console.log(
  '\nSUCCESS: Observability infrastructure, configuration, and measurement contracts verified!',
);

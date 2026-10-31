import { spawn, exec, execSync } from 'node:child_process';
import { promisify } from 'node:util';
import readline from 'node:readline';

const execAsync = promisify(exec);

const ANSI = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  dim: '\x1b[2m',
  green: '\x1b[32m',
  cyan: '\x1b[36m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  red: '\x1b[31m',
};

// Containers required by any application path: broker, source database and the
// Debezium Connect worker. All three use compose healthchecks.
const REQUIRED_CONTAINERS = ['kafka-broker-1', 'postgres-db', 'debezium-connect'];

const SERVICES = [
  {
    name: 'PRODUCER ',
    color: ANSI.magenta,
    cmd: 'pnpm',
    args: ['--filter', '@lab/producer-api', 'start'],
  },
  {
    name: 'LOYALTY  ',
    color: ANSI.green,
    cmd: 'pnpm',
    args: ['--filter', '@lab/loyalty-worker', 'start'],
  },
  {
    name: 'ANALYTICS',
    color: ANSI.yellow,
    cmd: 'pnpm',
    args: ['--filter', '@lab/analytics-worker', 'start'],
  },
  {
    name: 'REPLAY   ',
    color: ANSI.red,
    cmd: 'pnpm',
    args: ['--filter', '@lab/replay-worker', 'start'],
  },
  {
    name: 'DEMO-UI  ',
    color: ANSI.blue,
    cmd: 'pnpm',
    args: ['--filter', '@lab/demo-ui', 'dev'],
  },
];

const children = [];

function killChildProcess(child) {
  if (!child || child.killed) return;
  try {
    if (process.platform === 'win32') {
      execSync(`taskkill /pid ${child.pid} /T /F`, { stdio: 'ignore' });
    } else {
      process.kill(-child.pid, 'SIGINT');
    }
  } catch {
    // Process may have already terminated
  }
}

async function cleanupAndExit() {
  console.log(`\n${ANSI.yellow}${ANSI.bold}[Shutdown] Stopping all services...${ANSI.reset}`);
  for (const child of children) {
    killChildProcess(child);
  }
  console.log(`${ANSI.green}[Shutdown] All microservice processes stopped.${ANSI.reset}`);
  console.log(
    `${ANSI.dim}To stop Docker containers as well, run: pnpm lab:down or docker compose stop${ANSI.reset}\n`,
  );
  process.exit(0);
}

process.on('SIGINT', cleanupAndExit);
process.on('SIGTERM', cleanupAndExit);

async function waitForHealthyContainers() {
  const deadline = Date.now() + 90_000;
  console.log(
    `${ANSI.dim}Waiting for healthy containers: ${REQUIRED_CONTAINERS.join(', ')}...${ANSI.reset}`,
  );
  while (Date.now() < deadline) {
    let healthyCount = 0;
    for (const container of REQUIRED_CONTAINERS) {
      try {
        const { stdout } = await execAsync(
          `docker inspect --format="{{json .State.Health.Status}}" ${container}`,
        );
        let status = '';
        try {
          status = JSON.parse(stdout.trim());
        } catch {
          status = stdout.trim().replace(/^"|"$/g, '');
        }
        if (status === 'healthy') healthyCount += 1;
      } catch {
        // Container may not exist yet or health status not available yet
      }
    }
    if (healthyCount === REQUIRED_CONTAINERS.length) return;
    await new Promise((r) => setTimeout(r, 1500));
  }
  throw new Error(
    `Docker containers did not become healthy in time: ${REQUIRED_CONTAINERS.join(', ')}`,
  );
}

async function failFast(label, command) {
  try {
    await execAsync(command);
  } catch (err) {
    console.error(
      `${ANSI.red}${ANSI.bold}✕ ${label} failed:${ANSI.reset}\n${err.stdout ?? ''}\n${err.stderr ?? err.message}\n`,
    );
    for (const child of children) killChildProcess(child);
    process.exit(1);
  }
}

async function startInfrastructure() {
  console.log(`${ANSI.cyan}${ANSI.bold}=== STARTING KAFKA INTERACTIVE LAB ===${ANSI.reset}\n`);
  console.log(
    `${ANSI.dim}[1/4] Ensuring Docker infrastructure is running (Kafka, Postgres, Kafbat UI, Debezium)...${ANSI.reset}`,
  );

  try {
    await execAsync('docker compose up -d');
    console.log(`${ANSI.green}✓ Docker containers running.${ANSI.reset}`);
  } catch (err) {
    console.error(
      `${ANSI.red}✕ Failed to start Docker Compose. Ensure Docker Desktop is active:${ANSI.reset}`,
      err.message,
    );
    process.exit(1);
  }

  await waitForHealthyContainers();
  console.log(
    `${ANSI.green}✓ Kafka broker, PostgreSQL and Debezium Connect are healthy.${ANSI.reset}`,
  );
}

function pipeOutput(stream, prefix, color) {
  const rl = readline.createInterface({ input: stream });
  rl.on('line', (line) => {
    if (!line.trim()) return;
    console.log(`${color}[${prefix}]${ANSI.reset} ${line}`);
  });
}

function startServices() {
  console.log(
    `${ANSI.dim}[4/4] Spawning backend services and Vite interactive UI...${ANSI.reset}\n`,
  );

  for (const s of SERVICES) {
    const child = spawn(s.cmd, s.args, {
      shell: true,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, FORCE_COLOR: '1' },
    });

    children.push(child);

    pipeOutput(child.stdout, s.name, s.color);
    pipeOutput(child.stderr, s.name, s.color);

    child.on('error', (err) => {
      console.error(`${s.color}[${s.name}] ERROR:${ANSI.reset} ${err.message}`);
    });
  }

  // Print helpful navigation guide
  setTimeout(() => {
    console.log('\n' + '='.repeat(68));
    console.log(`${ANSI.green}${ANSI.bold}  🚀 KAFKA INTERACTIVE LAB IS RUNNING!${ANSI.reset}`);
    console.log('='.repeat(68));
    console.log(`  ${ANSI.bold}📱 Interactive UI:${ANSI.reset}      http://localhost:5173`);
    console.log(`  ${ANSI.bold}🌐 Kafbat UI Admin:${ANSI.reset}      http://localhost:8080`);
    console.log(`  ${ANSI.bold}⚡ Producer API:${ANSI.reset}        http://localhost:3000`);
    console.log(`  ${ANSI.bold}⚙️ Loyalty Worker:${ANSI.reset}      http://localhost:3001`);
    console.log(`  ${ANSI.bold}📊 Analytics Worker:${ANSI.reset}    http://localhost:3002`);
    console.log(`  ${ANSI.bold}↺ Replay Worker:${ANSI.reset}        http://localhost:3004`);
    console.log(`  ${ANSI.bold}🔁 Outbox CDC (Debezium):${ANSI.reset} http://localhost:8083`);
    console.log('='.repeat(68));
    console.log(
      `  ${ANSI.dim}Press Ctrl+C at any time to shut down all processes cleanly.${ANSI.reset}`,
    );
    console.log('='.repeat(68) + '\n');
  }, 3500);
}

async function main() {
  await startInfrastructure();
  await failFast('Build (pnpm build)', 'pnpm build');
  console.log(`${ANSI.green}✓ All workspace packages built.${ANSI.reset}\n`);
  await failFast('CDC provisioning (pnpm cdc:setup)', 'pnpm cdc:setup');
  startServices();
}

main().catch((err) => {
  console.error(`${ANSI.red}✕ Lab startup failed:${ANSI.reset}`, err.message);
  process.exit(1);
});

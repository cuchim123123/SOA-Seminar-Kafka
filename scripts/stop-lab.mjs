import { exec, execSync } from 'node:child_process';
import { promisify } from 'node:util';

const execAsync = promisify(exec);

const ANSI = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  green: '\x1b[32m',
  cyan: '\x1b[36m',
  yellow: '\x1b[33m',
  dim: '\x1b[2m',
};

// Application and Debezium Connect ports; 3003 (publisher process) is retired under CDC.
const PORTS = [3000, 3001, 3002, 3004, 5173, 8083];

async function stopPorts() {
  console.log(
    `${ANSI.cyan}[1/2] Terminating any active processes on lab ports (${PORTS.join(', ')})...${ANSI.reset}`,
  );
  if (process.platform === 'win32') {
    try {
      execSync(
        `powershell -Command "Get-NetTCPConnection -LocalPort ${PORTS.join(',')} -State Listen -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }"`,
        { stdio: 'ignore' },
      );
    } catch {
      // Ignore errors
    }
  }
  console.log(`${ANSI.green}✓ Application ports cleared.${ANSI.reset}`);
}

async function stopContainers() {
  console.log(
    `${ANSI.cyan}[2/2] Stopping Docker containers (Kafka, Postgres, Kafbat UI, Debezium)...${ANSI.reset}`,
  );
  try {
    await execAsync('docker compose stop');
    console.log(`${ANSI.green}✓ Docker containers stopped.${ANSI.reset}`);
  } catch (err) {
    console.log(`${ANSI.yellow}! Docker stop warning:${ANSI.reset}`, err.message);
  }
}

async function main() {
  console.log(`\n${ANSI.bold}=== STOPPING KAFKA INTERACTIVE LAB ===${ANSI.reset}\n`);
  await stopPorts();
  await stopContainers();
  console.log(
    `\n${ANSI.green}${ANSI.bold}✓ All lab processes and containers stopped successfully.${ANSI.reset}\n`,
  );
}

main();

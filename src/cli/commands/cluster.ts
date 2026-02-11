import { Command } from 'commander';
import * as clack from '@clack/prompts';
import pc from 'picocolors';
import { execSync, fork } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync, unlinkSync, readdirSync, copyFileSync, mkdirSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

// ── Resource path resolution ─────────────────────────────────────────
// When bundled by tsup → dist/index.js, __dirname = <root>/dist
// When running from source, __dirname = <root>/src/cli/commands
// In both cases we need to find <root>/src/cli/cluster/

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

function clusterDir(): string {
  // Bundled: dist/ → ../src/cli/cluster
  const fromDist = join(__dirname, '..', 'src', 'cli', 'cluster');
  if (existsSync(join(fromDist, 'docker-compose.yml'))) {
    return fromDist;
  }
  // Source: src/cli/commands/ → ../cluster
  return join(__dirname, '..', 'cluster');
}

function composePath(): string {
  return join(clusterDir(), 'docker-compose.yml');
}

function pipelinesDir(): string {
  return join(clusterDir(), 'pipelines');
}

function pidFilePath(): string {
  return join(tmpdir(), 'flink-reactor-cdc-publisher.pid');
}

// ── Command registration ─────────────────────────────────────────────

export function registerClusterCommand(program: Command): void {
  const cluster = program
    .command('cluster')
    .description('Manage local Flink dev cluster for dashboard development');

  cluster
    .command('up')
    .description('Start local Flink cluster (JM + 2×TM + SQL Gateway + Kafka)')
    .option('--port <port>', 'Flink REST port', '8081')
    .option('--seed', 'Submit example pipelines after startup')
    .action(async (opts: { port: string; seed?: boolean }) => {
      await runClusterUp({ port: opts.port, seed: opts.seed ?? false });
    });

  cluster
    .command('down')
    .description('Stop local Flink cluster')
    .option('--volumes', 'Remove Docker volumes (checkpoint and state data)')
    .action(async (opts: { volumes?: boolean }) => {
      await runClusterDown({ volumes: opts.volumes ?? false });
    });

  cluster
    .command('seed')
    .description('Submit example SQL pipelines and publish CDC data')
    .option('--only <category>', 'Filter by category: streaming, batch, or cdc')
    .action(async (opts: { only?: string }) => {
      await runClusterSeed({ only: opts.only as 'streaming' | 'batch' | 'cdc' | undefined });
    });

  cluster
    .command('status')
    .description('Show cluster health, running jobs, and resource utilization')
    .option('--port <port>', 'Flink REST port', '8081')
    .action(async (opts: { port: string }) => {
      await runClusterStatus(parseInt(opts.port, 10));
    });

  cluster
    .command('submit <sql-file>')
    .description('Submit a single SQL file via SQL Gateway')
    .option('--port <port>', 'SQL Gateway port', '8083')
    .action(async (sqlFile: string, opts: { port: string }) => {
      await runClusterSubmit(sqlFile, parseInt(opts.port, 10));
    });
}

// ── cluster up ───────────────────────────────────────────────────────

export async function runClusterUp(opts: {
  port: string;
  seed: boolean;
}): Promise<void> {
  clack.intro(pc.bgCyan(pc.black(' flink-reactor cluster up ')));

  // Pre-flight: Docker available?
  if (!dockerAvailable()) {
    console.error(pc.red('Docker is not installed or not running.'));
    console.error(pc.dim('Install Docker: https://docs.docker.com/get-docker/'));
    process.exitCode = 1;
    return;
  }

  const compose = composePath();
  const dataDir = join(clusterDir(), 'data');

  // Copy sample data to a Docker-accessible volume path
  const spinner = clack.spinner();
  spinner.start('Building Flink image and starting services...');

  try {
    execSync(
      `docker compose -f "${compose}" up --build -d`,
      {
        cwd: clusterDir(),
        stdio: 'pipe',
        env: { ...process.env, FLINK_PORT: opts.port },
      },
    );

    // Copy sample CSV into the flink-data volume via the jobmanager container
    try {
      execSync(
        `docker compose -f "${compose}" exec -T jobmanager mkdir -p /opt/flink/data`,
        { cwd: clusterDir(), stdio: 'pipe' },
      );
      execSync(
        `docker compose -f "${compose}" cp "${join(dataDir, 'sample-transactions.csv')}" jobmanager:/opt/flink/data/sample-transactions.csv`,
        { cwd: clusterDir(), stdio: 'pipe' },
      );
    } catch {
      // Non-critical: filesystem batch job will fail but others work fine
    }

    spinner.stop(pc.green('Docker services started.'));
  } catch (err) {
    spinner.stop(pc.red('Failed to start Docker services.'));
    if (err instanceof Error) {
      console.error(pc.dim(err.message));
    }
    process.exitCode = 1;
    return;
  }

  // Wait for services to be healthy
  const { waitForServices } = await import('../cluster/health-check.js');
  try {
    await waitForServices({
      flinkPort: parseInt(opts.port, 10),
      sqlGatewayPort: 8083,
      kafkaPort: 9094,
    });
  } catch {
    console.error(pc.red('Services did not become ready in time.'));
    console.error(pc.dim('Check docker logs: docker compose -f ... logs'));
    process.exitCode = 1;
    return;
  }

  console.log('');
  console.log(`  ${pc.green('✓')} Flink UI:     ${pc.dim(`http://localhost:${opts.port}`)}`);
  console.log(`  ${pc.green('✓')} SQL Gateway:  ${pc.dim('http://localhost:8083')}`);
  console.log(`  ${pc.green('✓')} Kafka:        ${pc.dim('localhost:9094')}`);
  console.log('');

  if (opts.seed) {
    await seedPipelines({});
  }

  clack.outro(pc.green('Cluster is ready!'));
}

// ── cluster down ─────────────────────────────────────────────────────

export async function runClusterDown(opts: {
  volumes: boolean;
}): Promise<void> {
  clack.intro(pc.bgCyan(pc.black(' flink-reactor cluster down ')));

  // Kill background CDC publisher if running
  killCdcPublisher();

  const compose = composePath();
  const args = ['compose', '-f', compose, 'down'];
  if (opts.volumes) {
    args.push('-v');
  }

  const spinner = clack.spinner();
  spinner.start('Stopping cluster...');

  try {
    execSync(`docker ${args.join(' ')}`, {
      cwd: clusterDir(),
      stdio: 'pipe',
    });
    spinner.stop(pc.green(opts.volumes ? 'Cluster stopped and volumes removed.' : 'Cluster stopped.'));
  } catch {
    spinner.stop(pc.yellow('Cluster may already be stopped.'));
  }

  clack.outro('Done.');
}

// ── cluster seed ─────────────────────────────────────────────────────

export async function runClusterSeed(opts: {
  only?: 'streaming' | 'batch' | 'cdc';
}): Promise<void> {
  clack.intro(pc.bgCyan(pc.black(' flink-reactor cluster seed ')));

  const { isClusterRunning } = await import('../cluster/health-check.js');
  if (!(await isClusterRunning(8081))) {
    console.error(pc.red('Cluster is not running.'));
    console.error(pc.dim('Start it first: flink-reactor cluster up'));
    process.exitCode = 1;
    return;
  }

  await seedPipelines(opts);
  clack.outro(pc.green('Seeding complete!'));
}

async function seedPipelines(opts: { only?: 'streaming' | 'batch' | 'cdc' }): Promise<void> {
  const { SqlGatewayClient } = await import('../cluster/sql-gateway-client.js');
  const { publishCdcMessages } = await import('../cluster/cdc-publisher.js');

  const client = new SqlGatewayClient('http://localhost:8083');
  const category = opts.only;

  // Step 1: Publish CDC batch data (needed before CDC pipelines)
  if (!category || category === 'cdc' || category === 'streaming') {
    const spinner = clack.spinner();
    spinner.start('Publishing CDC seed data to Kafka...');
    try {
      await publishCdcMessages({ mode: 'batch' });
      spinner.stop(pc.green('CDC seed data published.'));
    } catch (err) {
      spinner.stop(pc.yellow('Failed to publish CDC data (Kafka may not be ready).'));
      if (err instanceof Error) {
        console.log(pc.dim(`  ${err.message}`));
      }
    }
  }

  // Step 2: Submit SQL pipelines
  const categories = category
    ? [category === 'cdc' ? 'streaming' : category]
    : ['streaming', 'batch'];

  let submitted = 0;
  let failed = 0;

  for (const cat of categories) {
    const dir = join(pipelinesDir(), cat);
    if (!existsSync(dir)) continue;

    const files = readdirSync(dir)
      .filter((f) => f.endsWith('.sql'))
      .sort();

    // When --only cdc, only submit Kafka-based pipelines
    const filtered = category === 'cdc'
      ? files.filter((f) => f.includes('kafka'))
      : files;

    for (const file of filtered) {
      const filePath = join(dir, file);
      const name = file.replace('.sql', '');
      const spinner = clack.spinner();
      spinner.start(`Submitting ${pc.dim(`${cat}/${name}`)}...`);

      try {
        const result = await client.submitSqlFile(filePath);
        spinner.stop(`  ${pc.green('✓')} ${cat}/${name} → ${pc.dim(result.status)}`);
        submitted++;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        spinner.stop(`  ${pc.red('✗')} ${cat}/${name} → ${pc.dim(msg)}`);
        failed++;
      }
    }
  }

  console.log('');
  console.log(`  Submitted: ${pc.green(String(submitted))}  Failed: ${failed > 0 ? pc.red(String(failed)) : pc.dim('0')}`);

  // Step 3: Start continuous CDC publisher in background
  if (!category || category === 'cdc' || category === 'streaming') {
    startBackgroundCdcPublisher();
  }
}

// ── cluster status ───────────────────────────────────────────────────

export async function runClusterStatus(flinkPort: number = 8081): Promise<void> {
  const { isClusterRunning } = await import('../cluster/health-check.js');

  if (!(await isClusterRunning(flinkPort))) {
    console.log(pc.yellow('Cluster is not reachable.'));
    console.log(pc.dim(`Tried http://localhost:${flinkPort}/overview`));
    return;
  }

  try {
    const overviewRes = await fetch(`http://localhost:${flinkPort}/overview`);
    const overview = (await overviewRes.json()) as {
      taskmanagers: number;
      'slots-total': number;
      'slots-available': number;
      'jobs-running': number;
      'jobs-finished': number;
      'jobs-cancelled': number;
      'jobs-failed': number;
    };

    console.log(pc.bold('\n  Cluster Overview\n'));
    console.log(`  TaskManagers:    ${pc.green(String(overview.taskmanagers))}`);
    console.log(`  Slots (total):   ${pc.green(String(overview['slots-total']))}`);
    console.log(`  Slots (free):    ${pc.green(String(overview['slots-available']))}`);
    console.log('');
    console.log(`  Jobs running:    ${pc.green(String(overview['jobs-running']))}`);
    console.log(`  Jobs finished:   ${pc.dim(String(overview['jobs-finished']))}`);
    console.log(`  Jobs cancelled:  ${pc.dim(String(overview['jobs-cancelled']))}`);
    console.log(`  Jobs failed:     ${overview['jobs-failed'] > 0 ? pc.red(String(overview['jobs-failed'])) : pc.dim('0')}`);

    // Fetch job list
    const jobsRes = await fetch(`http://localhost:${flinkPort}/jobs/overview`);
    const jobsData = (await jobsRes.json()) as {
      jobs: Array<{
        jid: string;
        name: string;
        state: string;
        'start-time': number;
        duration: number;
      }>;
    };

    if (jobsData.jobs.length > 0) {
      console.log(pc.bold('\n  Jobs\n'));
      console.log(
        `  ${pc.dim(padRight('Name', 40))} ${pc.dim(padRight('State', 12))} ${pc.dim('Duration')}`,
      );
      console.log(`  ${pc.dim('─'.repeat(65))}`);

      for (const job of jobsData.jobs) {
        const stateColor =
          job.state === 'RUNNING' ? pc.green :
          job.state === 'FINISHED' ? pc.dim :
          job.state === 'FAILED' ? pc.red :
          pc.yellow;

        console.log(
          `  ${padRight(job.name, 40)} ${stateColor(padRight(job.state, 12))} ${pc.dim(formatDuration(job.duration))}`,
        );
      }
    }

    console.log('');
  } catch (err) {
    console.error(pc.red('Failed to fetch cluster status.'));
    if (err instanceof Error) {
      console.error(pc.dim(err.message));
    }
  }
}

// ── cluster submit ───────────────────────────────────────────────────

export async function runClusterSubmit(sqlFile: string, sqlGatewayPort: number = 8083): Promise<void> {
  const filePath = resolve(sqlFile);

  if (!existsSync(filePath)) {
    console.error(pc.red(`File not found: ${sqlFile}`));
    process.exitCode = 1;
    return;
  }

  const { isClusterRunning } = await import('../cluster/health-check.js');
  if (!(await isClusterRunning(8081))) {
    console.error(pc.red('Cluster is not running.'));
    console.error(pc.dim('Start it first: flink-reactor cluster up'));
    process.exitCode = 1;
    return;
  }

  const { SqlGatewayClient } = await import('../cluster/sql-gateway-client.js');
  const client = new SqlGatewayClient(`http://localhost:${sqlGatewayPort}`);

  const spinner = clack.spinner();
  spinner.start(`Submitting ${pc.dim(sqlFile)}...`);

  try {
    const result = await client.submitSqlFile(filePath);
    spinner.stop(pc.green('Statement submitted.'));
    console.log('');
    console.log(`  Status:    ${pc.green(result.status)}`);
    console.log(`  Session:   ${pc.dim(result.sessionHandle)}`);
    console.log(`  Operation: ${pc.dim(result.operationHandle)}`);
    console.log('');
  } catch (err) {
    spinner.stop(pc.red('Submission failed.'));
    if (err instanceof Error) {
      console.error(pc.dim(err.message));
    }
    process.exitCode = 1;
  }
}

// ── Background CDC publisher ─────────────────────────────────────────

function startBackgroundCdcPublisher(): void {
  // Write a small inline script that imports and runs the continuous publisher
  // Fork it as a detached child process
  const scriptPath = join(tmpdir(), 'flink-reactor-cdc-continuous.mjs');
  const cdcModulePath = join(clusterDir(), 'cdc-publisher.js');

  writeFileSync(
    scriptPath,
    `
import { publishCdcMessages } from '${cdcModulePath}';
const ac = new AbortController();
process.on('SIGTERM', () => ac.abort());
process.on('SIGINT', () => ac.abort());
publishCdcMessages({ mode: 'continuous', signal: ac.signal }).catch(() => {});
`,
    'utf-8',
  );

  try {
    const child = fork(scriptPath, [], {
      detached: true,
      stdio: 'ignore',
    });
    child.unref();

    if (child.pid) {
      writeFileSync(pidFilePath(), String(child.pid), 'utf-8');
      console.log(`  ${pc.green('✓')} CDC publisher running ${pc.dim(`(PID ${child.pid})`)}`);
    }
  } catch {
    console.log(pc.dim('  Could not start background CDC publisher.'));
  }
}

function killCdcPublisher(): void {
  const pidFile = pidFilePath();
  if (!existsSync(pidFile)) return;

  try {
    const pid = parseInt(readFileSync(pidFile, 'utf-8').trim(), 10);
    process.kill(pid, 'SIGTERM');
    console.log(pc.dim(`  Stopped CDC publisher (PID ${pid})`));
  } catch {
    // Process may already be dead
  }

  try {
    unlinkSync(pidFile);
  } catch {
    // Best-effort cleanup
  }
}

// ── Utilities ────────────────────────────────────────────────────────

function dockerAvailable(): boolean {
  try {
    execSync('docker info', { stdio: 'pipe' });
    return true;
  } catch {
    return false;
  }
}

function padRight(str: string, len: number): string {
  return str.length >= len ? str.substring(0, len) : str + ' '.repeat(len - str.length);
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const seconds = Math.floor(ms / 1000) % 60;
  const minutes = Math.floor(ms / 60_000) % 60;
  const hours = Math.floor(ms / 3_600_000);
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

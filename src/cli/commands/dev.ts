import { Command } from 'commander';
import pc from 'picocolors';
import { existsSync, watch, type FSWatcher } from 'node:fs';
import { join } from 'node:path';
import { execSync, type ChildProcess, spawn } from 'node:child_process';
import {
  resolveProjectContext,
  discoverPipelines,
} from '../discovery.js';
import { runSynth } from './synth.js';
import { runValidate } from './validate.js';
import { runGraph } from './graph.js';

// ── Command registration ────────────────────────────────────────────

export function registerDevCommand(program: Command): void {
  program
    .command('dev')
    .description('Start development mode with file watching')
    .option('-p, --pipeline <name>', 'Watch a specific pipeline')
    .option('--no-cluster', 'Skip starting local Flink cluster')
    .option('--port <port>', 'Flink dashboard port', '8081')
    .action(async (opts: { pipeline?: string; cluster: boolean; port: string }) => {
      await runDev(opts);
    });
}

// ── Dev server state ────────────────────────────────────────────────

interface DevState {
  projectDir: string;
  pipeline?: string;
  cluster: boolean;
  port: string;
  watchers: FSWatcher[];
  clusterProcess: ChildProcess | null;
  shuttingDown: boolean;
}

// ── Dev logic ───────────────────────────────────────────────────────

export async function runDev(opts: {
  pipeline?: string;
  cluster: boolean;
  port: string;
  projectDir?: string;
}): Promise<void> {
  const projectDir = opts.projectDir ?? process.cwd();

  const state: DevState = {
    projectDir,
    pipeline: opts.pipeline,
    cluster: opts.cluster,
    port: opts.port,
    watchers: [],
    clusterProcess: null,
    shuttingDown: false,
  };

  console.log(pc.bold('\n  flink-reactor dev\n'));

  // Start Flink cluster if requested
  if (state.cluster) {
    state.clusterProcess = await startCluster(state);
  }

  // Run initial synthesis
  console.log(pc.dim('Running initial synthesis...\n'));
  await runSynth({
    pipeline: state.pipeline,
    outdir: 'dist',
    projectDir,
  });

  // Set up file watchers
  setupWatchers(state);

  // Set up keyboard shortcuts
  setupKeyboard(state);

  printHelp();

  // Keep process alive
  await new Promise<void>((resolve) => {
    const check = (): void => {
      if (state.shuttingDown) {
        resolve();
      } else {
        setTimeout(check, 500);
      }
    };
    check();
  });
}

// ── Docker cluster ──────────────────────────────────────────────────

async function startCluster(state: DevState): Promise<ChildProcess | null> {
  const composePath = join(state.projectDir, 'docker-compose.flink.yml');

  if (!existsSync(composePath)) {
    console.log(pc.yellow('No docker-compose.flink.yml found. Skipping cluster start.'));
    console.log(pc.dim('Create one or use --no-cluster to skip.\n'));
    return null;
  }

  console.log(pc.dim(`Starting Flink cluster on port ${state.port}...`));

  try {
    const child = spawn('docker', ['compose', '-f', composePath, 'up', '-d'], {
      cwd: state.projectDir,
      stdio: 'pipe',
      env: { ...process.env, FLINK_PORT: state.port },
    });

    child.on('error', () => {
      console.log(pc.yellow('Docker not available. Skipping cluster start.'));
    });

    // Wait briefly for startup
    await new Promise((resolve) => setTimeout(resolve, 2000));
    console.log(pc.green(`Flink dashboard: http://localhost:${state.port}\n`));
    return child;
  } catch {
    console.log(pc.yellow('Failed to start Flink cluster. Continuing without it.'));
    return null;
  }
}

// ── File watchers ───────────────────────────────────────────────────

function setupWatchers(state: DevState): void {
  const dirs = ['pipelines', 'schemas', 'patterns'];

  for (const dir of dirs) {
    const watchDir = join(state.projectDir, dir);
    if (!existsSync(watchDir)) continue;

    try {
      const watcher = watch(watchDir, { recursive: true }, (event, filename) => {
        if (state.shuttingDown) return;
        if (!filename) return;

        console.log(pc.dim(`\n[${new Date().toLocaleTimeString()}] Change detected: ${dir}/${filename}`));
        onFileChange(state);
      });

      state.watchers.push(watcher);
      console.log(pc.dim(`  Watching ${dir}/`));
    } catch {
      // Directory might not exist or watch not supported
    }
  }

  console.log('');
}

let debounceTimer: ReturnType<typeof setTimeout> | null = null;

function onFileChange(state: DevState): void {
  if (debounceTimer) clearTimeout(debounceTimer);

  debounceTimer = setTimeout(async () => {
    console.log(pc.dim('Re-synthesizing...\n'));
    await runSynth({
      pipeline: state.pipeline,
      outdir: 'dist',
      projectDir: state.projectDir,
    });
  }, 300);
}

// ── Keyboard shortcuts ──────────────────────────────────────────────

function setupKeyboard(state: DevState): void {
  if (!process.stdin.isTTY) return;

  process.stdin.setRawMode(true);
  process.stdin.resume();
  process.stdin.setEncoding('utf-8');

  process.stdin.on('data', async (key: string) => {
    if (state.shuttingDown) return;

    switch (key) {
      case 'r':
        console.log(pc.dim('\n[manual] Re-synthesizing...\n'));
        await runSynth({
          pipeline: state.pipeline,
          outdir: 'dist',
          projectDir: state.projectDir,
        });
        break;

      case 'v':
        console.log(pc.dim('\n[manual] Validating...\n'));
        await runValidate({
          pipeline: state.pipeline,
          projectDir: state.projectDir,
        });
        break;

      case 'g':
        console.log(pc.dim('\n[manual] Generating graph...\n'));
        await runGraph({
          pipeline: state.pipeline,
          format: 'ascii',
          projectDir: state.projectDir,
        });
        break;

      case 'o':
        openDashboard(state.port);
        break;

      case 's':
        await showSqlPreview(state);
        break;

      case 'q':
      case '\u0003': // Ctrl+C
        await shutdown(state);
        break;

      case 'h':
      case '?':
        printHelp();
        break;
    }
  });
}

function printHelp(): void {
  console.log(pc.dim('  Keyboard shortcuts:'));
  console.log(pc.dim('    r  re-synthesize'));
  console.log(pc.dim('    v  validate'));
  console.log(pc.dim('    g  show graph'));
  console.log(pc.dim('    o  open Flink dashboard'));
  console.log(pc.dim('    s  SQL preview'));
  console.log(pc.dim('    q  quit'));
  console.log('');
}

function openDashboard(port: string): void {
  const url = `http://localhost:${port}`;
  console.log(pc.dim(`\nOpening ${url}...`));

  try {
    const platform = process.platform;
    const cmd = platform === 'darwin' ? 'open' :
      platform === 'win32' ? 'start' : 'xdg-open';
    execSync(`${cmd} ${url}`, { stdio: 'ignore' });
  } catch {
    console.log(pc.dim(`Open manually: ${url}`));
  }
}

async function showSqlPreview(state: DevState): Promise<void> {
  const { readFileSync } = await import('node:fs');
  const pipelines = discoverPipelines(state.projectDir, state.pipeline);

  for (const p of pipelines) {
    const sqlPath = join(state.projectDir, 'dist', p.name, 'pipeline.sql');
    if (!existsSync(sqlPath)) {
      console.log(pc.yellow(`\nNo synthesized SQL for ${p.name}. Run synth first.`));
      continue;
    }

    const sql = readFileSync(sqlPath, 'utf-8');
    console.log(`\n${pc.bold(p.name)} ${pc.dim('─'.repeat(30))}`);
    console.log(sql);
  }
}

// ── Shutdown ────────────────────────────────────────────────────────

async function shutdown(state: DevState): Promise<void> {
  state.shuttingDown = true;
  console.log(pc.dim('\nShutting down...'));

  // Close file watchers
  for (const watcher of state.watchers) {
    watcher.close();
  }

  // Stop cluster
  if (state.clusterProcess) {
    const composePath = join(state.projectDir, 'docker-compose.flink.yml');
    try {
      execSync(`docker compose -f "${composePath}" down`, {
        cwd: state.projectDir,
        stdio: 'pipe',
      });
    } catch {
      // Best-effort cleanup
    }
  }

  // Restore terminal
  if (process.stdin.isTTY) {
    process.stdin.setRawMode(false);
  }

  process.exit(0);
}

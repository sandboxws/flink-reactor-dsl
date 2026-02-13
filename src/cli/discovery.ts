import { readdirSync, existsSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { createJiti } from 'jiti';
import type { FlinkReactorConfig } from '../core/config.js';
import type { EnvironmentConfig } from '../core/environment.js';
import type { ConstructNode } from '../core/types.js';

// jiti handles .ts/.tsx imports at runtime — automatic JSX transform
// injects `import { jsx } from 'flink-reactor/jsx-runtime'` automatically
const jiti = createJiti(import.meta.url, {
  jsx: { runtime: 'automatic', importSource: 'flink-reactor' },
});

// ── Types ───────────────────────────────────────────────────────────

export interface DiscoveredPipeline {
  readonly name: string;
  readonly entryPoint: string;
}

export interface ProjectContext {
  readonly projectDir: string;
  readonly config: FlinkReactorConfig | null;
  readonly env: EnvironmentConfig | null;
  readonly pipelines: readonly DiscoveredPipeline[];
}

// ── Pipeline discovery ──────────────────────────────────────────────

/**
 * Discover pipelines by walking the pipelines/ directory.
 * Each subdirectory containing an index.tsx is a pipeline.
 */
export function discoverPipelines(
  projectDir: string,
  targetPipeline?: string,
): DiscoveredPipeline[] {
  const pipelinesDir = join(projectDir, 'pipelines');

  if (!existsSync(pipelinesDir)) {
    return [];
  }

  const entries = readdirSync(pipelinesDir);
  const pipelines: DiscoveredPipeline[] = [];

  for (const entry of entries) {
    const entryPath = join(pipelinesDir, entry);
    if (!statSync(entryPath).isDirectory()) continue;

    const indexPath = join(entryPath, 'index.tsx');
    if (!existsSync(indexPath)) continue;

    if (targetPipeline && entry !== targetPipeline) continue;

    pipelines.push({
      name: entry,
      entryPoint: indexPath,
    });
  }

  return pipelines.sort((a, b) => a.name.localeCompare(b.name));
}

// ── Config loading ──────────────────────────────────────────────────

/**
 * Load the project config from flink-reactor.config.ts.
 * Returns null if no config file exists.
 */
export async function loadConfig(
  projectDir: string,
): Promise<FlinkReactorConfig | null> {
  const configPath = join(projectDir, 'flink-reactor.config.ts');

  if (!existsSync(configPath)) {
    return null;
  }

  const mod = await jiti.import(resolve(configPath)) as Record<string, unknown>;
  return (mod.default ?? mod) as FlinkReactorConfig;
}

// ── Environment loading ─────────────────────────────────────────────

/**
 * Load an environment config from env/<name>.ts.
 * Returns null if no env file exists or no env name given.
 */
export async function loadEnvironment(
  projectDir: string,
  envName?: string,
): Promise<EnvironmentConfig | null> {
  if (!envName) return null;

  const envPath = join(projectDir, 'env', `${envName}.ts`);

  if (!existsSync(envPath)) {
    throw new Error(`Environment file not found: env/${envName}.ts`);
  }

  const mod = await jiti.import(resolve(envPath)) as Record<string, unknown>;
  return (mod.default ?? mod) as EnvironmentConfig;
}

// ── Pipeline loading ────────────────────────────────────────────────

/**
 * Dynamically import a pipeline entry point and return its construct tree.
 * The pipeline's index.tsx should export a default ConstructNode.
 */
export async function loadPipeline(
  entryPoint: string,
): Promise<ConstructNode> {
  const mod = await jiti.import(resolve(entryPoint)) as Record<string, unknown>;
  return mod.default as ConstructNode;
}

// ── Full project context ────────────────────────────────────────────

/**
 * Build the full project context: config, environment, and pipeline list.
 */
export async function resolveProjectContext(
  projectDir: string,
  options?: {
    readonly pipeline?: string;
    readonly env?: string;
  },
): Promise<ProjectContext> {
  const config = await loadConfig(projectDir);
  const env = await loadEnvironment(projectDir, options?.env);
  const pipelines = discoverPipelines(projectDir, options?.pipeline);

  return {
    projectDir,
    config,
    env,
    pipelines,
  };
}

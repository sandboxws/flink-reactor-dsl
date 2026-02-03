import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, rmSync, mkdirSync, writeFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { discoverPipelines, loadConfig } from '../../discovery.js';

describe('pipeline discovery', () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'flink-reactor-discovery-'));
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it('finds pipelines in pipelines/ directory', () => {
    const dir = join(tempDir, 'pipelines');
    mkdirSync(join(dir, 'orders'), { recursive: true });
    mkdirSync(join(dir, 'analytics'), { recursive: true });
    mkdirSync(join(dir, 'not-a-pipeline'), { recursive: true });

    writeFileSync(join(dir, 'orders', 'index.tsx'), 'export default null;');
    writeFileSync(join(dir, 'analytics', 'index.tsx'), 'export default null;');
    // not-a-pipeline has no index.tsx

    const pipelines = discoverPipelines(tempDir);

    expect(pipelines).toHaveLength(2);
    expect(pipelines[0].name).toBe('analytics');
    expect(pipelines[1].name).toBe('orders');
    expect(pipelines[0].entryPoint).toContain('analytics/index.tsx');
    expect(pipelines[1].entryPoint).toContain('orders/index.tsx');
  });

  it('returns empty array when pipelines/ does not exist', () => {
    const pipelines = discoverPipelines(tempDir);
    expect(pipelines).toEqual([]);
  });

  it('filters by pipeline name when --pipeline flag is set', () => {
    const dir = join(tempDir, 'pipelines');
    mkdirSync(join(dir, 'orders'), { recursive: true });
    mkdirSync(join(dir, 'analytics'), { recursive: true });

    writeFileSync(join(dir, 'orders', 'index.tsx'), 'export default null;');
    writeFileSync(join(dir, 'analytics', 'index.tsx'), 'export default null;');

    const pipelines = discoverPipelines(tempDir, 'orders');

    expect(pipelines).toHaveLength(1);
    expect(pipelines[0].name).toBe('orders');
  });

  it('skips files that are not directories', () => {
    const dir = join(tempDir, 'pipelines');
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, 'README.md'), '# Pipelines');

    const pipelines = discoverPipelines(tempDir);
    expect(pipelines).toEqual([]);
  });
});

describe('config loading', () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'flink-reactor-config-'));
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it('returns null when no config file exists', async () => {
    const config = await loadConfig(tempDir);
    expect(config).toBeNull();
  });

  it('loads config from flink-reactor.config.ts', async () => {
    writeFileSync(
      join(tempDir, 'flink-reactor.config.ts'),
      `export default {
        flink: { version: '2.0' },
        kafka: { bootstrapServers: 'kafka:9092' },
      };`,
      'utf-8',
    );

    const config = await loadConfig(tempDir);
    expect(config).not.toBeNull();
    expect(config?.flink?.version).toBe('2.0');
    expect(config?.kafka?.bootstrapServers).toBe('kafka:9092');
  });
});

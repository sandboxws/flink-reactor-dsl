import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, rmSync, existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { scaffoldProject } from '../new.js';
import type { ScaffoldOptions } from '../new.js';

describe('integration: new --template starter --yes', () => {
  let tempDir: string;
  let projectDir: string;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'flink-reactor-integration-'));
    projectDir = join(tempDir, 'my-app');
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it('produces a valid project structure', () => {
    const options: ScaffoldOptions = {
      projectName: 'my-app',
      template: 'starter',
      pm: 'pnpm',
      flinkVersion: '1.20',
      gitInit: false,
      installDeps: false,
    };

    scaffoldProject(projectDir, options);

    // Core config files
    expect(existsSync(join(projectDir, 'package.json'))).toBe(true);
    expect(existsSync(join(projectDir, 'tsconfig.json'))).toBe(true);
    expect(existsSync(join(projectDir, 'flink-reactor.config.ts'))).toBe(true);
    expect(existsSync(join(projectDir, '.gitignore'))).toBe(true);

    // Environment
    expect(existsSync(join(projectDir, 'env', 'dev.ts'))).toBe(true);

    // Schema
    expect(existsSync(join(projectDir, 'schemas', 'events.ts'))).toBe(true);

    // Pipeline
    expect(existsSync(join(projectDir, 'pipelines', 'hello-world', 'index.tsx'))).toBe(true);

    // Test
    expect(existsSync(join(projectDir, 'tests', 'pipelines', 'hello-world.test.ts'))).toBe(true);

    // Validate package.json
    const pkg = JSON.parse(readFileSync(join(projectDir, 'package.json'), 'utf-8'));
    expect(pkg.name).toBe('my-app');
    expect(pkg.type).toBe('module');
    expect(pkg.dependencies).toHaveProperty('flink-reactor');

    // Validate tsconfig.json
    const tsconfig = JSON.parse(readFileSync(join(projectDir, 'tsconfig.json'), 'utf-8'));
    expect(tsconfig.compilerOptions.strict).toBe(true);
    expect(tsconfig.compilerOptions.jsx).toBe('react-jsx');
    expect(tsconfig.compilerOptions.jsxImportSource).toBe('flink-reactor');

    // Validate config references correct Flink version
    const config = readFileSync(join(projectDir, 'flink-reactor.config.ts'), 'utf-8');
    expect(config).toContain("'1.20'");

    // Validate pipeline content — automatic JSX runtime, no createElement import needed
    const pipeline = readFileSync(
      join(projectDir, 'pipelines', 'hello-world', 'index.tsx'),
      'utf-8',
    );
    expect(pipeline).not.toContain('createElement');
    expect(pipeline).toContain('Pipeline');
    expect(pipeline).toContain('KafkaSource');
    expect(pipeline).toContain('KafkaSink');
    expect(pipeline).toContain('EventSchema');
  });
});

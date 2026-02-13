import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mkdtempSync, rmSync, existsSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import {
  generatePipeline,
  generateSchema,
  generateEnv,
  generatePattern,
  generateApp,
  generatePackage,
} from '../generate.js';

describe('generate commands', () => {
  let tempDir: string;
  let originalCwd: string;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'flink-reactor-gen-'));
    originalCwd = process.cwd();
    process.chdir(tempDir);
  });

  afterEach(() => {
    process.chdir(originalCwd);
    rmSync(tempDir, { recursive: true, force: true });
  });

  describe('generatePipeline', () => {
    it('creates blank pipeline by default', () => {
      generatePipeline('order-processing', 'blank');

      const filePath = join(tempDir, 'pipelines', 'order-processing', 'index.tsx');
      expect(existsSync(filePath)).toBe(true);

      const content = readFileSync(filePath, 'utf-8');
      expect(content).toContain('<Pipeline name="order-processing">');
    });

    it('creates kafka pipeline template', () => {
      generatePipeline('events', 'kafka');

      const content = readFileSync(
        join(tempDir, 'pipelines', 'events', 'index.tsx'),
        'utf-8',
      );
      expect(content).toContain('<KafkaSource');
      expect(content).toContain('<KafkaSink');
    });

    it('creates jdbc pipeline template', () => {
      generatePipeline('sink-to-db', 'jdbc');

      const content = readFileSync(
        join(tempDir, 'pipelines', 'sink-to-db', 'index.tsx'),
        'utf-8',
      );
      expect(content).toContain('<JdbcSink');
    });

    it('creates matching test file', () => {
      generatePipeline('my-pipeline', 'blank');

      const testPath = join(tempDir, 'tests', 'pipelines', 'my-pipeline.test.ts');
      expect(existsSync(testPath)).toBe(true);

      const content = readFileSync(testPath, 'utf-8');
      expect(content).toContain("describe('my-pipeline pipeline'");
    });
  });

  describe('generateSchema', () => {
    it('creates schema file with PascalCase name', () => {
      generateSchema('user-events');

      const filePath = join(tempDir, 'schemas', 'user-events.ts');
      expect(existsSync(filePath)).toBe(true);

      const content = readFileSync(filePath, 'utf-8');
      expect(content).toContain('UserEventsSchema');
      expect(content).toContain('Schema({');
    });
  });

  describe('generateEnv', () => {
    it('creates environment config file', () => {
      generateEnv('staging');

      const filePath = join(tempDir, 'env', 'staging.ts');
      expect(existsSync(filePath)).toBe(true);

      const content = readFileSync(filePath, 'utf-8');
      expect(content).toContain("name: 'staging'");
    });
  });

  describe('generatePattern', () => {
    it('creates pattern file', () => {
      generatePattern('dead-letter');

      const filePath = join(tempDir, 'patterns', 'dead-letter.ts');
      expect(existsSync(filePath)).toBe(true);

      const content = readFileSync(filePath, 'utf-8');
      expect(content).toContain('DeadLetter');
    });
  });

  describe('generateApp', () => {
    it('fails in non-monorepo project', () => {
      const spy = vi.spyOn(console, 'error').mockImplementation(() => {});
      generateApp('new-app');

      expect(spy).toHaveBeenCalledWith(expect.stringContaining('monorepo'));
      spy.mockRestore();
    });

    it('creates app structure in monorepo', () => {
      // Set up monorepo structure
      writeFileSync(join(tempDir, 'pnpm-workspace.yaml'), 'packages:\n  - "apps/*"\n');
      writeFileSync(
        join(tempDir, 'package.json'),
        JSON.stringify({ name: 'my-workspace' }),
      );

      generateApp('analytics');

      expect(existsSync(join(tempDir, 'apps', 'analytics', 'package.json'))).toBe(true);
      expect(existsSync(join(tempDir, 'apps', 'analytics', 'flink-reactor.config.ts'))).toBe(true);
      expect(existsSync(join(tempDir, 'apps', 'analytics', 'env', 'dev.ts'))).toBe(true);
    });
  });

  describe('generatePackage', () => {
    it('fails in non-monorepo project', () => {
      const spy = vi.spyOn(console, 'error').mockImplementation(() => {});
      generatePackage('utils');

      expect(spy).toHaveBeenCalledWith(expect.stringContaining('monorepo'));
      spy.mockRestore();
    });

    it('creates package structure in monorepo', () => {
      writeFileSync(join(tempDir, 'pnpm-workspace.yaml'), 'packages:\n  - "packages/*"\n');
      writeFileSync(
        join(tempDir, 'package.json'),
        JSON.stringify({ name: 'my-workspace' }),
      );

      generatePackage('utils');

      expect(existsSync(join(tempDir, 'packages', 'utils', 'package.json'))).toBe(true);
      expect(existsSync(join(tempDir, 'packages', 'utils', 'index.ts'))).toBe(true);
    });
  });
});

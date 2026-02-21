import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { env, isEnvVarRef, resolveEnvVars } from '../env-var.js';

describe('env()', () => {
  it('creates a frozen EnvVarRef marker', () => {
    const ref = env('MY_VAR');
    expect(ref.varName).toBe('MY_VAR');
    expect(ref.fallback).toBeUndefined();
    expect(Object.isFrozen(ref)).toBe(true);
  });

  it('supports an optional fallback', () => {
    const ref = env('MY_VAR', 'default-value');
    expect(ref.varName).toBe('MY_VAR');
    expect(ref.fallback).toBe('default-value');
  });

  it('throws on empty or non-string name', () => {
    expect(() => env('')).toThrow('non-empty variable name');
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(() => env(null as any)).toThrow('non-empty variable name');
  });
});

describe('isEnvVarRef()', () => {
  it('returns true for env() markers', () => {
    expect(isEnvVarRef(env('VAR'))).toBe(true);
  });

  it('returns false for plain objects', () => {
    expect(isEnvVarRef({ varName: 'VAR' })).toBe(false);
  });

  it('returns false for primitives', () => {
    expect(isEnvVarRef('string')).toBe(false);
    expect(isEnvVarRef(42)).toBe(false);
    expect(isEnvVarRef(null)).toBe(false);
    expect(isEnvVarRef(undefined)).toBe(false);
  });
});

describe('resolveEnvVars()', () => {
  const saved: Record<string, string | undefined> = {};

  beforeEach(() => {
    saved['TEST_URL'] = process.env['TEST_URL'];
    saved['TEST_PASS'] = process.env['TEST_PASS'];
    saved['TEST_MISSING'] = process.env['TEST_MISSING'];
  });

  afterEach(() => {
    for (const [key, val] of Object.entries(saved)) {
      if (val === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = val;
      }
    }
  });

  it('replaces EnvVarRef with process.env value', () => {
    process.env['TEST_URL'] = 'http://flink:8081';
    const obj = { cluster: { url: env('TEST_URL') } };
    const resolved = resolveEnvVars(obj);
    expect(resolved.cluster.url).toBe('http://flink:8081');
  });

  it('uses fallback when env var is undefined', () => {
    delete process.env['TEST_MISSING'];
    const obj = { host: env('TEST_MISSING', 'localhost') };
    const resolved = resolveEnvVars(obj);
    expect(resolved.host).toBe('localhost');
  });

  it('throws with config path on missing required var', () => {
    delete process.env['TEST_MISSING'];
    const obj = { dashboard: { auth: { password: env('TEST_MISSING') } } };
    expect(() => resolveEnvVars(obj)).toThrow(
      "Missing required environment variable 'TEST_MISSING' at config path 'dashboard.auth.password'",
    );
  });

  it('preserves non-EnvVarRef values', () => {
    const obj = {
      port: 8081,
      name: 'test',
      enabled: true,
      tags: ['a', 'b'],
      nested: { deep: 42 },
    };
    const resolved = resolveEnvVars(obj);
    expect(resolved).toEqual(obj);
  });

  it('handles mixed EnvVarRef and literal values', () => {
    process.env['TEST_PASS'] = 's3cret';
    const obj = {
      auth: {
        type: 'basic' as const,
        username: 'admin',
        password: env('TEST_PASS'),
      },
    };
    const resolved = resolveEnvVars(obj);
    expect(resolved.auth.type).toBe('basic');
    expect(resolved.auth.username).toBe('admin');
    expect(resolved.auth.password).toBe('s3cret');
  });

  it('handles arrays containing EnvVarRef', () => {
    process.env['TEST_URL'] = 'http://flink:8081';
    const obj = { urls: [env('TEST_URL'), 'http://static:9090'] };
    const resolved = resolveEnvVars(obj);
    expect(resolved.urls).toEqual(['http://flink:8081', 'http://static:9090']);
  });

  it('handles deeply nested structures', () => {
    process.env['TEST_URL'] = 'https://prod.example.com';
    process.env['TEST_PASS'] = 'p@ss';
    const obj = {
      environments: {
        production: {
          cluster: { url: env('TEST_URL') },
          dashboard: {
            auth: { password: env('TEST_PASS') },
            ssl: { enabled: true },
          },
        },
      },
    };
    const resolved = resolveEnvVars(obj);
    expect(resolved.environments.production.cluster.url).toBe('https://prod.example.com');
    expect(resolved.environments.production.dashboard.auth.password).toBe('p@ss');
    expect(resolved.environments.production.dashboard.ssl.enabled).toBe(true);
  });
});

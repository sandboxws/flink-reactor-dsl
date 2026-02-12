import { describe, it, expect } from 'vitest';
import { resolvePlugins, EMPTY_PLUGIN_CHAIN } from '../plugin-registry.js';
import type { FlinkReactorPlugin } from '../plugin.js';
import type { ConstructNode } from '../types.js';

function makePlugin(overrides: Partial<FlinkReactorPlugin> & { name: string }): FlinkReactorPlugin {
  return overrides;
}

function makeTree(): ConstructNode {
  return {
    id: 'Pipeline_0',
    kind: 'Pipeline',
    component: 'Pipeline',
    props: { name: 'test' },
    children: [{
      id: 'KafkaSource_0',
      kind: 'Source',
      component: 'KafkaSource',
      props: { topic: 'input' },
      children: [],
    }],
  };
}

describe('resolvePlugins', () => {
  it('returns EMPTY_PLUGIN_CHAIN for empty array', () => {
    const chain = resolvePlugins([]);
    expect(chain).toBe(EMPTY_PLUGIN_CHAIN);
    expect(chain.order).toEqual([]);
    expect(chain.components.size).toBe(0);
  });

  it('orders plugins alphabetically when no constraints', () => {
    const chain = resolvePlugins([
      makePlugin({ name: 'charlie' }),
      makePlugin({ name: 'alpha' }),
      makePlugin({ name: 'bravo' }),
    ]);

    expect(chain.order).toEqual(['alpha', 'bravo', 'charlie']);
  });

  it('respects "before" ordering constraint', () => {
    const chain = resolvePlugins([
      makePlugin({ name: 'logging', ordering: { before: ['metrics'] } }),
      makePlugin({ name: 'metrics' }),
    ]);

    expect(chain.order).toEqual(['logging', 'metrics']);
  });

  it('respects "after" ordering constraint', () => {
    const chain = resolvePlugins([
      makePlugin({ name: 'metrics', ordering: { after: ['logging'] } }),
      makePlugin({ name: 'logging' }),
    ]);

    expect(chain.order).toEqual(['logging', 'metrics']);
  });

  it('handles complex ordering with multiple constraints', () => {
    const chain = resolvePlugins([
      makePlugin({ name: 'error-handling', ordering: { after: ['logging', 'metrics'] } }),
      makePlugin({ name: 'metrics', ordering: { after: ['logging'] } }),
      makePlugin({ name: 'logging' }),
    ]);

    expect(chain.order).toEqual(['logging', 'metrics', 'error-handling']);
  });

  it('uses alphabetical tiebreaker for unrelated plugins', () => {
    const chain = resolvePlugins([
      makePlugin({ name: 'zebra' }),
      makePlugin({ name: 'alpha' }),
      makePlugin({ name: 'middle', ordering: { after: ['alpha'] } }),
    ]);

    // alpha comes first (no deps), then middle (after alpha), then zebra (alphabetical with no deps relative to middle)
    expect(chain.order.indexOf('alpha')).toBeLessThan(chain.order.indexOf('middle'));
  });

  it('ignores ordering references to non-existent plugins', () => {
    const chain = resolvePlugins([
      makePlugin({ name: 'a', ordering: { after: ['nonexistent'] } }),
      makePlugin({ name: 'b' }),
    ]);

    // Should not throw, constraint on nonexistent plugin is ignored
    expect(chain.order).toEqual(['a', 'b']);
  });

  // ── Error cases ─────────────────────────────────────────────────────

  it('throws on duplicate plugin names', () => {
    expect(() => resolvePlugins([
      makePlugin({ name: 'duplicate' }),
      makePlugin({ name: 'duplicate' }),
    ])).toThrow("Duplicate plugin name 'duplicate'");
  });

  it('throws on circular ordering constraints', () => {
    expect(() => resolvePlugins([
      makePlugin({ name: 'a', ordering: { before: ['b'] } }),
      makePlugin({ name: 'b', ordering: { before: ['a'] } }),
    ])).toThrow('Circular ordering constraint');
  });

  it('throws on duplicate component registrations', () => {
    expect(() => resolvePlugins([
      makePlugin({
        name: 'plugin-a',
        components: new Map([['CustomSource', 'Source']]),
      }),
      makePlugin({
        name: 'plugin-b',
        components: new Map([['CustomSource', 'Source']]),
      }),
    ])).toThrow("Component 'CustomSource' registered by both 'plugin-a' and 'plugin-b'");
  });

  it('throws on duplicate SQL generator registrations', () => {
    const gen = () => 'SELECT 1';
    expect(() => resolvePlugins([
      makePlugin({
        name: 'plugin-a',
        sqlGenerators: new Map([['CustomSource', gen]]),
      }),
      makePlugin({
        name: 'plugin-b',
        sqlGenerators: new Map([['CustomSource', gen]]),
      }),
    ])).toThrow("SQL generator for 'CustomSource' registered by both 'plugin-a' and 'plugin-b'");
  });

  it('throws on duplicate DDL generator registrations', () => {
    const gen = () => 'CREATE TABLE t();';
    expect(() => resolvePlugins([
      makePlugin({
        name: 'plugin-a',
        ddlGenerators: new Map([['CustomSource', gen]]),
      }),
      makePlugin({
        name: 'plugin-b',
        ddlGenerators: new Map([['CustomSource', gen]]),
      }),
    ])).toThrow("DDL generator for 'CustomSource' registered by both 'plugin-a' and 'plugin-b'");
  });

  // ── Component merging ───────────────────────────────────────────────

  it('merges component registrations from multiple plugins', () => {
    const chain = resolvePlugins([
      makePlugin({
        name: 'plugin-a',
        components: new Map([['SourceA', 'Source']]),
      }),
      makePlugin({
        name: 'plugin-b',
        components: new Map([['SinkB', 'Sink']]),
      }),
    ]);

    expect(chain.components.get('SourceA')).toBe('Source');
    expect(chain.components.get('SinkB')).toBe('Sink');
  });

  // ── Tree transformer composition ────────────────────────────────────

  it('composes tree transformers left-to-right', () => {
    const log: string[] = [];

    const chain = resolvePlugins([
      makePlugin({
        name: 'first',
        transformTree: (tree) => {
          log.push('first');
          return { ...tree, props: { ...tree.props, first: true } };
        },
      }),
      makePlugin({
        name: 'second',
        transformTree: (tree) => {
          log.push('second');
          return { ...tree, props: { ...tree.props, second: true } };
        },
      }),
    ]);

    const tree = makeTree();
    const result = chain.transformTree(tree);

    expect(log).toEqual(['first', 'second']);
    expect(result.props.first).toBe(true);
    expect(result.props.second).toBe(true);
  });

  it('returns identity transform when no plugins have transformTree', () => {
    const chain = resolvePlugins([
      makePlugin({ name: 'no-transform' }),
    ]);

    const tree = makeTree();
    const result = chain.transformTree(tree);
    expect(result).toBe(tree); // Same reference — identity function
  });

  // ── CRD transformer composition ─────────────────────────────────────

  it('composes CRD transformers left-to-right', () => {
    const chain = resolvePlugins([
      makePlugin({
        name: 'labels',
        transformCrd: (crd) => ({
          ...crd,
          metadata: { ...crd.metadata, labels: { plugin: 'labels' } },
        }),
      }),
      makePlugin({
        name: 'annotations',
        transformCrd: (crd) => ({
          ...crd,
          metadata: { ...crd.metadata, annotations: { plugin: 'annotations' } },
        }),
      }),
    ]);

    const baseCrd = {
      apiVersion: 'flink.apache.org/v1beta1' as const,
      kind: 'FlinkDeployment' as const,
      metadata: { name: 'test' },
      spec: {
        image: 'flink:2.0',
        flinkVersion: 'v2_0',
        flinkConfiguration: {},
        jobManager: { resource: { cpu: '1', memory: '1024m' }, replicas: 1 },
        taskManager: { resource: { cpu: '1', memory: '1024m' } },
        job: { jarURI: 'local:///opt/flink/usrlib/sql-runner.jar', parallelism: 1 },
      },
    };

    const result = chain.transformCrd(baseCrd, makeTree());
    expect(result.metadata.labels).toEqual({ plugin: 'labels' });
    expect(result.metadata.annotations).toEqual({ plugin: 'annotations' });
  });

  // ── Validators ──────────────────────────────────────────────────────

  it('collects validators in plugin order', () => {
    const chain = resolvePlugins([
      makePlugin({
        name: 'validator-a',
        validate: () => [{ severity: 'warning', message: 'from-a' }],
      }),
      makePlugin({
        name: 'validator-b',
        validate: () => [{ severity: 'error', message: 'from-b' }],
      }),
    ]);

    expect(chain.validators).toHaveLength(2);
  });

  // ── Lifecycle hooks ─────────────────────────────────────────────────

  it('collects beforeSynth and afterSynth hooks in order', () => {
    const chain = resolvePlugins([
      makePlugin({
        name: 'plugin-a',
        beforeSynth: () => {},
        afterSynth: () => {},
      }),
      makePlugin({
        name: 'plugin-b',
        beforeSynth: () => {},
      }),
    ]);

    expect(chain.beforeSynth).toHaveLength(2);
    expect(chain.afterSynth).toHaveLength(1);
  });

  // ── Determinism ─────────────────────────────────────────────────────

  it('produces identical ordering regardless of input order', () => {
    const pluginsA = [
      makePlugin({ name: 'delta' }),
      makePlugin({ name: 'alpha' }),
      makePlugin({ name: 'gamma' }),
      makePlugin({ name: 'beta' }),
    ];

    const pluginsB = [
      makePlugin({ name: 'gamma' }),
      makePlugin({ name: 'beta' }),
      makePlugin({ name: 'delta' }),
      makePlugin({ name: 'alpha' }),
    ];

    const chainA = resolvePlugins(pluginsA);
    const chainB = resolvePlugins(pluginsB);

    expect(chainA.order).toEqual(chainB.order);
    expect(chainA.order).toEqual(['alpha', 'beta', 'delta', 'gamma']);
  });
});

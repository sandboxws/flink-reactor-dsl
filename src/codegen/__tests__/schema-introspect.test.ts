import { describe, it, expect, beforeEach } from 'vitest';
import { resetNodeIdCounter } from '../../core/jsx-runtime.js';
import { Schema, Field } from '../../core/schema.js';
import { KafkaSource, JdbcSource } from '../../components/sources.js';
import { KafkaSink, JdbcSink } from '../../components/sinks.js';
import { Filter, Map, Aggregate } from '../../components/transforms.js';
import { Pipeline } from '../../components/pipeline.js';
import { Route } from '../../components/route.js';
import { introspectPipelineSchemas } from '../schema-introspect.js';

beforeEach(() => {
  resetNodeIdCounter();
});

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    user_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    event_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: 'event_time',
    expression: "`event_time` - INTERVAL '5' SECOND",
  },
  primaryKey: { columns: ['order_id'] },
});

const UserSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    name: Field.STRING(),
    email: Field.STRING(),
  },
});

// ── Simple source → sink ────────────────────────────────────────────

describe('simple source → sink', () => {
  it('introspects source schema with PK and watermark annotations', () => {
    const pipeline = Pipeline({
      name: 'simple',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            KafkaSource({
              topic: 'orders',
              schema: OrderSchema,
            }),
          ],
        }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);

    // One source, one sink
    const sources = schemas.filter((s) => s.kind === 'source');
    const sinks = schemas.filter((s) => s.kind === 'sink');
    expect(sources).toHaveLength(1);
    expect(sinks).toHaveLength(1);

    // Source has PK and WM annotations
    const src = sources[0];
    expect(src.component).toBe('KafkaSource');
    expect(src.columns).toHaveLength(4);

    const orderId = src.columns.find((c) => c.name === 'order_id')!;
    expect(orderId.constraints).toContain('PK');

    const eventTime = src.columns.find((c) => c.name === 'event_time')!;
    expect(eventTime.constraints).toContain('WM');

    const userId = src.columns.find((c) => c.name === 'user_id')!;
    expect(userId.constraints).toHaveLength(0);

    // Sink has resolved schema (same fields, no constraints)
    const sink = sinks[0];
    expect(sink.component).toBe('KafkaSink');
    expect(sink.columns).toHaveLength(4);
    expect(sink.columns.every((c) => c.constraints.length === 0)).toBe(true);
  });
});

// ── Multi-source pipeline ───────────────────────────────────────────

describe('multi-source pipeline', () => {
  it('introspects multiple sources and their downstream sinks', () => {
    const pipeline = Pipeline({
      name: 'multi-source',
      children: [
        KafkaSink({
          topic: 'orders-out',
          children: [
            KafkaSource({
              topic: 'orders',
              schema: OrderSchema,
            }),
          ],
        }),
        KafkaSink({
          topic: 'users-out',
          children: [
            KafkaSource({
              topic: 'users',
              schema: UserSchema,
            }),
          ],
        }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sources = schemas.filter((s) => s.kind === 'source');
    const sinks = schemas.filter((s) => s.kind === 'sink');

    expect(sources).toHaveLength(2);
    expect(sinks).toHaveLength(2);

    // First source has 4 fields, second has 3
    const ordersSrc = sources.find((s) => s.nameHint === 'orders')!;
    expect(ordersSrc.columns).toHaveLength(4);

    const usersSrc = sources.find((s) => s.nameHint === 'users')!;
    expect(usersSrc.columns).toHaveLength(3);
  });
});

// ── Transform chain reshapes sink schema ────────────────────────────

describe('transform chain', () => {
  it('Map reshapes the sink schema', () => {
    const pipeline = Pipeline({
      name: 'map-pipeline',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Map({
              select: {
                order_id: 'order_id',
                total: 'amount',
              },
              children: [
                KafkaSource({
                  topic: 'orders',
                  schema: OrderSchema,
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sources = schemas.filter((s) => s.kind === 'source');
    const sinks = schemas.filter((s) => s.kind === 'sink');

    // Source still has 4 fields
    expect(sources[0].columns).toHaveLength(4);

    // Sink has 2 fields (Map projected)
    expect(sinks[0].columns).toHaveLength(2);
    expect(sinks[0].columns[0].name).toBe('order_id');
    expect(sinks[0].columns[0].type).toBe('BIGINT');
    expect(sinks[0].columns[1].name).toBe('total');
    expect(sinks[0].columns[1].type).toBe('DECIMAL(10, 2)');
  });

  it('Aggregate reshapes the sink schema', () => {
    const pipeline = Pipeline({
      name: 'agg-pipeline',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Aggregate({
              groupBy: ['user_id'],
              select: {
                user_id: 'user_id',
                total_amount: 'SUM(amount)',
                order_count: 'COUNT(*)',
              },
              children: [
                KafkaSource({
                  topic: 'orders',
                  schema: OrderSchema,
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sinks = schemas.filter((s) => s.kind === 'sink');

    expect(sinks[0].columns).toHaveLength(3);
    expect(sinks[0].columns[0]).toEqual({ name: 'user_id', type: 'STRING', constraints: [] });
    expect(sinks[0].columns[1]).toEqual({ name: 'total_amount', type: 'DECIMAL(10, 2)', constraints: [] });
    expect(sinks[0].columns[2]).toEqual({ name: 'order_count', type: 'BIGINT', constraints: [] });
  });

  it('Filter preserves the sink schema', () => {
    const pipeline = Pipeline({
      name: 'filter-pipeline',
      children: [
        KafkaSink({
          topic: 'output',
          children: [
            Filter({
              condition: '`amount` > 100',
              children: [
                KafkaSource({
                  topic: 'orders',
                  schema: OrderSchema,
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sinks = schemas.filter((s) => s.kind === 'sink');

    // Filter is passthrough — sink has same 4 fields as source
    expect(sinks[0].columns).toHaveLength(4);
  });
});

// ── Forward-reading JSX pattern ─────────────────────────────────────

describe('forward-reading JSX pattern', () => {
  it('resolves sink schema from preceding source sibling', () => {
    const pipeline = Pipeline({
      name: 'forward',
      children: [
        KafkaSource({
          topic: 'orders',
          schema: OrderSchema,
        }),
        KafkaSink({ topic: 'output' }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sinks = schemas.filter((s) => s.kind === 'sink');

    expect(sinks).toHaveLength(1);
    expect(sinks[0].columns).toHaveLength(4);
  });

  it('resolves sink schema through intermediate transforms', () => {
    const pipeline = Pipeline({
      name: 'forward-transform',
      children: [
        KafkaSource({
          topic: 'orders',
          schema: OrderSchema,
        }),
        Map({
          select: {
            order_id: 'order_id',
            doubled: 'amount * 2',
          },
        }),
        KafkaSink({ topic: 'output' }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sinks = schemas.filter((s) => s.kind === 'sink');

    expect(sinks).toHaveLength(1);
    expect(sinks[0].columns).toHaveLength(2);
    expect(sinks[0].columns[0].name).toBe('order_id');
    expect(sinks[0].columns[1].name).toBe('doubled');
  });
});

// ── Route with branching ────────────────────────────────────────────

describe('route branching', () => {
  it('resolves different schemas per branch', () => {
    const pipeline = Pipeline({
      name: 'routing',
      children: [
        KafkaSource({
          topic: 'orders',
          schema: OrderSchema,
        }),
        Route({
          children: [
            Route.Branch({
              condition: '`amount` > 1000',
              children: [
                KafkaSink({ topic: 'high-value' }),
              ],
            }),
            Route.Default({
              children: [
                Aggregate({
                  groupBy: ['user_id'],
                  select: {
                    user_id: 'user_id',
                    total: 'SUM(amount)',
                  },
                }),
                JdbcSink({
                  url: 'jdbc:postgresql://db:5432/analytics',
                  table: 'user_totals',
                }),
              ],
            }),
          ],
        }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sinks = schemas.filter((s) => s.kind === 'sink');

    expect(sinks).toHaveLength(2);

    // First branch: passthrough (4 fields)
    const highValue = sinks.find((s) => s.nameHint === 'high_value')!;
    expect(highValue.columns).toHaveLength(4);

    // Default branch: aggregated (2 fields)
    const userTotals = sinks.find((s) => s.nameHint === 'user_totals')!;
    expect(userTotals.columns).toHaveLength(2);
    expect(userTotals.columns[0]).toEqual({ name: 'user_id', type: 'STRING', constraints: [] });
    expect(userTotals.columns[1]).toEqual({ name: 'total', type: 'DECIMAL(10, 2)', constraints: [] });
  });
});

// ── Unresolvable sink schema ────────────────────────────────────────

describe('unresolvable sink schema', () => {
  it('returns empty columns for sinks with no upstream schema', () => {
    const pipeline = Pipeline({
      name: 'unknown',
      children: [
        KafkaSink({ topic: 'output' }),
      ],
    });

    const schemas = introspectPipelineSchemas(pipeline);
    const sinks = schemas.filter((s) => s.kind === 'sink');

    expect(sinks).toHaveLength(1);
    expect(sinks[0].columns).toHaveLength(0);
  });
});

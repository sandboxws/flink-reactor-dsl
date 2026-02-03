import { describe, it, expect, beforeEach } from 'vitest';
import { resetNodeIdCounter } from '../../core/jsx-runtime.js';
import { SynthContext } from '../../core/synth-context.js';
import { Schema, Field } from '../../core/schema.js';
import { KafkaSource } from '../sources.js';
import { KafkaSink } from '../sinks.js';
import { Filter, Map, FlatMap, Aggregate, Union, Deduplicate, TopN } from '../transforms.js';

beforeEach(() => {
  resetNodeIdCounter();
});

const EventSchema = Schema({
  fields: {
    id: Field.BIGINT(),
    userId: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    eventTime: Field.TIMESTAMP(3),
  },
});

const DifferentSchema = Schema({
  fields: {
    name: Field.STRING(),
    score: Field.INT(),
  },
});

// ── 4.1 Source → Filter → Map → Sink linear chain ──────────────────

describe('linear chain: Source → Filter → Map → Sink', () => {
  it('builds a valid four-node construct tree', () => {
    const sink = KafkaSink({ topic: 'out' });
    const map = Map({
      select: { total: 'amount * 2', user: 'userId' },
      children: sink,
    });
    const filter = Filter({ condition: 'amount > 100', children: map });
    const source = KafkaSource({
      topic: 'in',
      schema: EventSchema,
      children: filter,
    });

    const ctx = new SynthContext();
    ctx.buildFromTree(source);

    expect(ctx.getAllNodes()).toHaveLength(4);
    expect(ctx.getAllEdges()).toHaveLength(3);

    const sorted = ctx.topologicalSort();
    expect(sorted.map((n) => n.component)).toEqual([
      'KafkaSource', 'Filter', 'Map', 'KafkaSink',
    ]);
    expect(ctx.validate()).toHaveLength(0);
  });
});

// ── 4.2 Union of two same-schema streams ────────────────────────────

describe('Union', () => {
  it('merges two same-schema streams', () => {
    const sourceA = KafkaSource({ topic: 'a', schema: EventSchema });
    const sourceB = KafkaSource({ topic: 'b', schema: EventSchema });
    const union = Union({
      inputs: [EventSchema, EventSchema],
      children: [sourceA, sourceB],
    });

    expect(union.kind).toBe('Transform');
    expect(union.component).toBe('Union');
    expect(union.children).toHaveLength(2);
  });

  // ── 4.3 Union rejects mismatched schemas ──────────────────────────

  it('throws on mismatched schemas', () => {
    expect(() =>
      Union({
        inputs: [EventSchema, DifferentSchema],
      }),
    ).toThrow('Union schema mismatch');
  });

  it('accepts union without inputs prop (validation deferred to synthesis)', () => {
    const sourceA = KafkaSource({ topic: 'a', schema: EventSchema });
    const sourceB = KafkaSource({ topic: 'b', schema: EventSchema });
    const union = Union({ children: [sourceA, sourceB] });

    expect(union.component).toBe('Union');
    expect(union.children).toHaveLength(2);
  });
});

// ── 4.4 Deduplicate stores key/order/keep params ───────────────────

describe('Deduplicate', () => {
  it('stores deduplication parameters on the construct node', () => {
    const node = Deduplicate({
      key: ['event_id'],
      order: 'event_time',
      keep: 'first',
    });

    expect(node.kind).toBe('Transform');
    expect(node.component).toBe('Deduplicate');
    expect(node.props.key).toEqual(['event_id']);
    expect(node.props.order).toBe('event_time');
    expect(node.props.keep).toBe('first');
  });

  it('supports last-row deduplication', () => {
    const node = Deduplicate({
      key: ['user_id', 'session_id'],
      order: 'updated_at',
      keep: 'last',
    });

    expect(node.props.keep).toBe('last');
    expect(node.props.key).toEqual(['user_id', 'session_id']);
  });
});

// ── 4.5 TopN stores partitionBy/orderBy/n params ───────────────────

describe('TopN', () => {
  it('stores ranking parameters on the construct node', () => {
    const node = TopN({
      partitionBy: ['region'],
      orderBy: { revenue: 'DESC' },
      n: 5,
    });

    expect(node.kind).toBe('Transform');
    expect(node.component).toBe('TopN');
    expect(node.props.partitionBy).toEqual(['region']);
    expect(node.props.orderBy).toEqual({ revenue: 'DESC' });
    expect(node.props.n).toBe(5);
  });

  it('supports multi-column ordering', () => {
    const node = TopN({
      partitionBy: ['category'],
      orderBy: { score: 'DESC', name: 'ASC' },
      n: 10,
    });

    expect(node.props.orderBy).toEqual({ score: 'DESC', name: 'ASC' });
  });
});

// ── 4.7 FlatMap with unnest configuration ───────────────────────────

describe('FlatMap', () => {
  it('stores unnest configuration on the construct node', () => {
    const node = FlatMap({
      unnest: 'tags',
      as: { tag: 'STRING' },
    });

    expect(node.kind).toBe('Transform');
    expect(node.component).toBe('FlatMap');
    expect(node.props.unnest).toBe('tags');
    expect(node.props.as).toEqual({ tag: 'STRING' });
  });

  it('supports map unnesting with key-value output', () => {
    const node = FlatMap({
      unnest: 'attributes',
      as: { attr_key: 'STRING', attr_value: 'STRING' },
    });

    expect(node.props.unnest).toBe('attributes');
    expect(node.props.as).toEqual({
      attr_key: 'STRING',
      attr_value: 'STRING',
    });
  });
});

// ── Filter and Aggregate basic tests ────────────────────────────────

describe('Filter', () => {
  it('stores condition on the construct node', () => {
    const node = Filter({ condition: "status = 'active' AND amount > 0" });

    expect(node.kind).toBe('Transform');
    expect(node.component).toBe('Filter');
    expect(node.props.condition).toBe("status = 'active' AND amount > 0");
  });
});

describe('Aggregate', () => {
  it('stores groupBy and select on the construct node', () => {
    const node = Aggregate({
      groupBy: ['user_id'],
      select: {
        user_id: 'user_id',
        total: 'SUM(amount)',
        count: 'COUNT(*)',
      },
    });

    expect(node.kind).toBe('Transform');
    expect(node.component).toBe('Aggregate');
    expect(node.props.groupBy).toEqual(['user_id']);
    expect(node.props.select).toEqual({
      user_id: 'user_id',
      total: 'SUM(amount)',
      count: 'COUNT(*)',
    });
  });
});

describe('Map', () => {
  it('stores select projection on the construct node', () => {
    const node = Map({
      select: {
        total: 'amount * quantity',
        order_date: 'CAST(ts AS DATE)',
      },
    });

    expect(node.kind).toBe('Transform');
    expect(node.component).toBe('Map');
    expect(node.props.select).toEqual({
      total: 'amount * quantity',
      order_date: 'CAST(ts AS DATE)',
    });
  });
});

// ── Per-operator parallelism on transforms ──────────────────────────

describe('per-operator parallelism', () => {
  it('stores parallelism on transform nodes', () => {
    const filter = Filter({ condition: 'x > 0', parallelism: 16 });
    const agg = Aggregate({
      groupBy: ['key'],
      select: { total: 'SUM(val)' },
      parallelism: 4,
    });

    expect(filter.props.parallelism).toBe(16);
    expect(agg.props.parallelism).toBe(4);
  });
});

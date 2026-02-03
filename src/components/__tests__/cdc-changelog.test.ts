import { describe, it, expect, beforeEach } from 'vitest';
import { resetNodeIdCounter } from '../../core/jsx-runtime.js';
import { Schema, Field } from '../../core/schema.js';
import { KafkaSource, inferChangelogMode } from '../sources.js';
import { KafkaSink } from '../sinks.js';
import { JdbcSink } from '../sinks.js';
import { SynthContext } from '../../core/synth-context.js';

beforeEach(() => {
  resetNodeIdCounter();
});

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    customer_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    order_time: Field.TIMESTAMP(3),
  },
});

describe('inferChangelogMode', () => {
  it('returns retract for debezium-json', () => {
    expect(inferChangelogMode('debezium-json')).toBe('retract');
  });

  it('returns retract for canal-json', () => {
    expect(inferChangelogMode('canal-json')).toBe('retract');
  });

  it('returns retract for maxwell-json', () => {
    expect(inferChangelogMode('maxwell-json')).toBe('retract');
  });

  it('returns append-only for json', () => {
    expect(inferChangelogMode('json')).toBe('append-only');
  });

  it('returns append-only for avro', () => {
    expect(inferChangelogMode('avro')).toBe('append-only');
  });

  it('returns append-only for csv', () => {
    expect(inferChangelogMode('csv')).toBe('append-only');
  });

  it('returns append-only for undefined (defaults to json)', () => {
    expect(inferChangelogMode(undefined)).toBe('append-only');
  });
});

describe('KafkaSource with CDC format', () => {
  it('stores changelogMode retract for debezium-json', () => {
    const node = KafkaSource({
      topic: 'db.inventory.orders',
      schema: OrderSchema,
      format: 'debezium-json',
      primaryKey: ['order_id'],
    });

    expect(node.props.changelogMode).toBe('retract');
    expect(node.props.primaryKey).toEqual(['order_id']);
    expect(node.props.format).toBe('debezium-json');
  });

  it('stores changelogMode retract for canal-json', () => {
    const node = KafkaSource({
      topic: 'canal-binlog',
      schema: OrderSchema,
      format: 'canal-json',
    });

    expect(node.props.changelogMode).toBe('retract');
  });

  it('stores changelogMode append-only for plain json', () => {
    const node = KafkaSource({
      topic: 'events',
      schema: OrderSchema,
      format: 'json',
    });

    expect(node.props.changelogMode).toBe('append-only');
  });

  it('stores changelogMode append-only when format is omitted', () => {
    const node = KafkaSource({
      topic: 'events',
      schema: OrderSchema,
    });

    expect(node.props.changelogMode).toBe('append-only');
  });
});

describe('Changelog validation', () => {
  it('rejects retract stream connected to append-only KafkaSink', () => {
    const ctx = new SynthContext();

    const source = KafkaSource({
      topic: 'db.orders',
      schema: OrderSchema,
      format: 'debezium-json',
      primaryKey: ['order_id'],
    });

    const sink = KafkaSink({ topic: 'output' });

    ctx.addNode(source);
    ctx.addNode(sink);
    ctx.addEdge(source.id, sink.id);

    const diagnostics = ctx.detectChangelogMismatch();
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0].severity).toBe('error');
    expect(diagnostics[0].message).toContain('does not support');
    expect(diagnostics[0].message).toContain('retract');
    expect(diagnostics[0].nodeId).toBe(sink.id);
  });

  it('accepts retract stream connected to JdbcSink with upsertMode=true', () => {
    const ctx = new SynthContext();

    const source = KafkaSource({
      topic: 'db.orders',
      schema: OrderSchema,
      format: 'debezium-json',
      primaryKey: ['order_id'],
    });

    const sink = JdbcSink({
      url: 'jdbc:postgresql://localhost:5432/db',
      table: 'orders',
      upsertMode: true,
      keyFields: ['order_id'],
    });

    ctx.addNode(source);
    ctx.addNode(sink);
    ctx.addEdge(source.id, sink.id);

    const diagnostics = ctx.detectChangelogMismatch();
    expect(diagnostics).toHaveLength(0);
  });

  it('rejects retract stream connected to JdbcSink without upsertMode', () => {
    const ctx = new SynthContext();

    const source = KafkaSource({
      topic: 'db.orders',
      schema: OrderSchema,
      format: 'debezium-json',
      primaryKey: ['order_id'],
    });

    const sink = JdbcSink({
      url: 'jdbc:postgresql://localhost:5432/db',
      table: 'orders',
    });

    ctx.addNode(source);
    ctx.addNode(sink);
    ctx.addEdge(source.id, sink.id);

    const diagnostics = ctx.detectChangelogMismatch();
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0].message).toContain('retract');
  });

  it('allows append-only stream to any sink', () => {
    const ctx = new SynthContext();

    const source = KafkaSource({
      topic: 'events',
      schema: OrderSchema,
      format: 'json',
    });

    const sink = KafkaSink({ topic: 'output' });

    ctx.addNode(source);
    ctx.addNode(sink);
    ctx.addEdge(source.id, sink.id);

    const diagnostics = ctx.detectChangelogMismatch();
    expect(diagnostics).toHaveLength(0);
  });

  it('propagates changelog mode through transform nodes', () => {
    const ctx = new SynthContext();

    const source = KafkaSource({
      topic: 'db.orders',
      schema: OrderSchema,
      format: 'debezium-json',
    });

    // Simulate a transform node in between (no changelogMode prop)
    const transform = {
      id: 'filter_0',
      kind: 'Transform' as const,
      component: 'Filter',
      props: { predicate: 'amount > 100' },
      children: [],
    };

    const sink = KafkaSink({ topic: 'output' });

    ctx.addNode(source);
    ctx.addNode(transform);
    ctx.addNode(sink);
    ctx.addEdge(source.id, transform.id);
    ctx.addEdge(transform.id, sink.id);

    const diagnostics = ctx.detectChangelogMismatch();
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0].message).toContain('retract');
  });
});

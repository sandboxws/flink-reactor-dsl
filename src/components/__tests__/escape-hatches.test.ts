import { describe, it, expect, beforeEach } from 'vitest';
import { createElement, resetNodeIdCounter } from '../../core/jsx-runtime.js';
import { Schema, Field } from '../../core/schema.js';
import { SynthContext } from '../../core/synth-context.js';
import { RawSQL, UDF } from '../escape-hatches.js';

beforeEach(() => {
  resetNodeIdCounter();
});

function makeSource(name: string) {
  return createElement('KafkaSource', { topic: name });
}

// ── RawSQL ──────────────────────────────────────────────────────────

describe('RawSQL', () => {
  it('creates a RawSQL node with input streams as children', () => {
    const orders = makeSource('orders');
    const users = makeSource('users');

    const raw = RawSQL({
      sql: 'SELECT o.*, u.name FROM orders o JOIN users u ON o.user_id = u.id',
      inputs: [orders, users],
      outputSchema: Schema({
        fields: {
          order_id: Field.BIGINT(),
          user_name: Field.STRING(),
        },
      }),
    });

    expect(raw.kind).toBe('RawSQL');
    expect(raw.component).toBe('RawSQL');
    expect(raw.props.sql).toBe(
      'SELECT o.*, u.name FROM orders o JOIN users u ON o.user_id = u.id',
    );
    expect(raw.props.inputIds).toEqual([orders.id, users.id]);
    // Input streams are wired as children for DAG edges
    expect(raw.children).toHaveLength(2);
  });

  it('output schema matches the declared schema', () => {
    const source = makeSource('events');
    const outputSchema = Schema({
      fields: {
        event_id: Field.BIGINT(),
        computed_field: Field.STRING(),
      },
    });

    const raw = RawSQL({
      sql: 'SELECT event_id, UPPER(payload) AS computed_field FROM events',
      inputs: [source],
      outputSchema,
    });

    expect(raw.props.outputSchema).toEqual(outputSchema);
    expect((raw.props.outputSchema as typeof outputSchema).fields.event_id).toBe('BIGINT');
    expect((raw.props.outputSchema as typeof outputSchema).fields.computed_field).toBe('STRING');
  });

  it('wires correctly into the DAG', () => {
    const source = makeSource('orders');

    const raw = RawSQL({
      sql: 'SELECT * FROM orders WHERE amount > 100',
      inputs: [source],
      outputSchema: Schema({
        fields: { order_id: Field.BIGINT(), amount: Field.DECIMAL(10, 2) },
      }),
    });

    const ctx = new SynthContext();
    ctx.buildFromTree(raw);
    expect(ctx.getAllNodes()).toHaveLength(2);
    expect(ctx.getAllEdges()).toHaveLength(1);
  });

  it('throws when no inputs are provided', () => {
    expect(() =>
      RawSQL({
        sql: 'SELECT 1',
        inputs: [],
        outputSchema: Schema({ fields: { x: Field.INT() } }),
      }),
    ).toThrow('RawSQL requires at least one input stream');
  });
});

// ── UDF ─────────────────────────────────────────────────────────────

describe('UDF', () => {
  it('creates a UDF node with function registration metadata', () => {
    const udf = UDF({
      name: 'my_hash',
      className: 'com.mycompany.HashFunction',
      jarPath: '/path/to/udf.jar',
    });

    expect(udf.kind).toBe('UDF');
    expect(udf.component).toBe('UDF');
    expect(udf.props.name).toBe('my_hash');
    expect(udf.props.className).toBe('com.mycompany.HashFunction');
    expect(udf.props.jarPath).toBe('/path/to/udf.jar');
  });
});

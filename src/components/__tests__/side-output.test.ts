import { describe, it, expect, beforeEach } from 'vitest';
import { resetNodeIdCounter } from '../../core/jsx-runtime.js';
import { SynthContext } from '../../core/synth-context.js';
import { Schema, Field } from '../../core/schema.js';
import { KafkaSource } from '../sources.js';
import { KafkaSink } from '../sinks.js';
import { SideOutput } from '../side-output.js';

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
});

describe('SideOutput', () => {
  it('creates a SideOutput node with condition and children', () => {
    const source = KafkaSource({ topic: 'orders', schema: OrderSchema });
    const sideSink = KafkaSink({ topic: 'order-errors' });
    const sideSinkWrapper = SideOutput.Sink({ children: sideSink });

    const sideOutput = SideOutput({
      condition: 'amount < 0 OR user_id IS NULL',
      tag: 'invalid-order',
      children: [sideSinkWrapper, source],
    });

    expect(sideOutput.kind).toBe('Transform');
    expect(sideOutput.component).toBe('SideOutput');
    expect(sideOutput.props.condition).toBe('amount < 0 OR user_id IS NULL');
    expect(sideOutput.props.tag).toBe('invalid-order');
    expect(sideOutput.children).toHaveLength(2);
  });

  it('builds a valid DAG with source → side-output → sinks', () => {
    const source = KafkaSource({ topic: 'orders', schema: OrderSchema });
    const sideSink = KafkaSink({ topic: 'order-errors' });
    const sideSinkWrapper = SideOutput.Sink({ children: sideSink });

    const sideOutput = SideOutput({
      condition: 'amount < 0',
      children: [sideSinkWrapper, source],
    });

    const ctx = new SynthContext();
    ctx.buildFromTree(sideOutput);

    // SideOutput → SideOutput.Sink → KafkaSink, SideOutput → KafkaSource
    expect(ctx.getAllNodes()).toHaveLength(4);
    expect(ctx.getAllEdges()).toHaveLength(3);
  });

  it('throws when condition is missing', () => {
    const source = KafkaSource({ topic: 'orders', schema: OrderSchema });
    const sideSink = KafkaSink({ topic: 'errors' });
    const wrapper = SideOutput.Sink({ children: sideSink });

    expect(() =>
      SideOutput({
        condition: '',
        children: [wrapper, source],
      }),
    ).toThrow('SideOutput requires a condition');
  });

  it('throws when SideOutput.Sink is missing', () => {
    const source = KafkaSource({ topic: 'orders', schema: OrderSchema });

    expect(() =>
      SideOutput({
        condition: 'amount < 0',
        children: [source],
      }),
    ).toThrow('SideOutput requires a SideOutput.Sink child');
  });

  it('supports optional tag and outputSchema', () => {
    const source = KafkaSource({ topic: 'orders', schema: OrderSchema });
    const sideSink = KafkaSink({ topic: 'errors' });
    const wrapper = SideOutput.Sink({ children: sideSink });

    const sideOutput = SideOutput({
      condition: 'amount < 0',
      children: [wrapper, source],
    });

    expect(sideOutput.props.tag).toBeUndefined();
    expect(sideOutput.props.outputSchema).toBeUndefined();
  });
});

describe('SideOutput.Sink', () => {
  it('wraps a downstream sink', () => {
    const sink = KafkaSink({ topic: 'errors' });
    const wrapper = SideOutput.Sink({ children: sink });

    expect(wrapper.component).toBe('SideOutput.Sink');
    expect(wrapper.children).toHaveLength(1);
    expect(wrapper.children[0].component).toBe('KafkaSink');
  });
});

import { describe, it, expect, beforeEach } from 'vitest';
import { createElement, Fragment, resetNodeIdCounter } from '../jsx-runtime.js';

beforeEach(() => {
  resetNodeIdCounter();
});

describe('createElement', () => {
  it('creates a construct node with the given component name', () => {
    const node = createElement('KafkaSource', { topic: 'orders' });

    expect(node.id).toBe('KafkaSource_0');
    expect(node.kind).toBe('Source');
    expect(node.component).toBe('KafkaSource');
    expect(node.props).toEqual({ topic: 'orders' });
    expect(node.children).toEqual([]);
  });

  it('assigns sequential IDs', () => {
    const a = createElement('KafkaSource', null);
    const b = createElement('Filter', null);
    const c = createElement('KafkaSink', null);

    expect(a.id).toBe('KafkaSource_0');
    expect(b.id).toBe('Filter_1');
    expect(c.id).toBe('KafkaSink_2');
  });

  it('correctly maps component names to node kinds', () => {
    expect(createElement('KafkaSource', null).kind).toBe('Source');
    expect(createElement('JdbcSource', null).kind).toBe('Source');
    expect(createElement('KafkaSink', null).kind).toBe('Sink');
    expect(createElement('JdbcSink', null).kind).toBe('Sink');
    expect(createElement('Filter', null).kind).toBe('Transform');
    expect(createElement('Map', null).kind).toBe('Transform');
    expect(createElement('Join', null).kind).toBe('Join');
    expect(createElement('TumbleWindow', null).kind).toBe('Window');
    expect(createElement('Pipeline', null).kind).toBe('Pipeline');
    expect(createElement('RawSQL', null).kind).toBe('RawSQL');
    expect(createElement('UDF', null).kind).toBe('UDF');
    expect(createElement('MatchRecognize', null).kind).toBe('CEP');
  });

  it('defaults unknown components to Transform kind', () => {
    expect(createElement('CustomThing', null).kind).toBe('Transform');
  });

  it('attaches children as nested nodes (linear pipeline)', () => {
    const sink = createElement('KafkaSink', { topic: 'output' });
    const filter = createElement('Filter', { predicate: 'x > 0' }, sink);
    const source = createElement('KafkaSource', { topic: 'input' }, filter);

    expect(source.children).toHaveLength(1);
    expect(source.children[0].component).toBe('Filter');
    expect(source.children[0].children).toHaveLength(1);
    expect(source.children[0].children[0].component).toBe('KafkaSink');
  });

  it('flattens array children', () => {
    const child1 = createElement('KafkaSink', { topic: 'a' });
    const child2 = createElement('KafkaSink', { topic: 'b' });
    const parent = createElement('KafkaSource', null, [child1, child2]);

    expect(parent.children).toHaveLength(2);
  });

  it('handles null props', () => {
    const node = createElement('KafkaSource', null);
    expect(node.props).toEqual({});
  });

  it('strips children key from props', () => {
    const node = createElement('KafkaSource', { topic: 'x', children: [] });
    expect(node.props).toEqual({ topic: 'x' });
    expect(node.props).not.toHaveProperty('children');
  });
});

describe('Fragment', () => {
  it('returns children as-is', () => {
    const a = createElement('Filter', null);
    const b = createElement('Map', null);
    const result = Fragment({ children: [a, b] });

    expect(result).toHaveLength(2);
    expect(result[0].component).toBe('Filter');
    expect(result[1].component).toBe('Map');
  });

  it('returns empty array for no children', () => {
    expect(Fragment({})).toEqual([]);
  });
});

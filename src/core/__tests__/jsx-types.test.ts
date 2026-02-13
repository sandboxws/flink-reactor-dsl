import { describe, it, expectTypeOf } from 'vitest';
import { createElement } from '../jsx-runtime.js';
import type { ConstructNode, TypedConstructNode } from '../types.js';
import { Pipeline } from '../../components/pipeline.js';
import { Filter } from '../../components/transforms.js';
import { KafkaSource } from '../../components/sources.js';

describe('JSX type safety — positive assertions', () => {
  it('createElement returns ConstructNode', () => {
    const node = createElement('TestComponent', { foo: 'bar' });
    expectTypeOf(node).toEqualTypeOf<ConstructNode>();
  });

  it('component factory functions return ConstructNode', () => {
    expectTypeOf(Pipeline).returns.toMatchTypeOf<ConstructNode>();
    expectTypeOf(Filter).returns.toMatchTypeOf<ConstructNode>();
    expectTypeOf(KafkaSource).returns.toMatchTypeOf<ConstructNode>();
  });

  it('TypedConstructNode<C> is assignable to ConstructNode', () => {
    expectTypeOf<TypedConstructNode<'Route.Branch'>>().toMatchTypeOf<ConstructNode>();
    expectTypeOf<TypedConstructNode<'Query.Select'>>().toMatchTypeOf<ConstructNode>();
  });

  it('ConstructNode is NOT assignable to TypedConstructNode<C>', () => {
    expectTypeOf<ConstructNode>().not.toMatchTypeOf<TypedConstructNode<'Route.Branch'>>();
    expectTypeOf<ConstructNode>().not.toMatchTypeOf<TypedConstructNode<'Query.Select'>>();
  });
});

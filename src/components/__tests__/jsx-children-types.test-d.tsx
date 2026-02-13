import { test, expectTypeOf } from 'vitest';
import { createElement } from '../../core/jsx-runtime.js';
import type { ConstructNode, TypedConstructNode } from '../../core/types.js';
import { Route } from '../route.js';
import { KafkaSink } from '../sinks.js';
import { Query } from '../query.js';
import { SideOutput } from '../side-output.js';
import { Validate } from '../validate.js';

// ── Route JSX compiles correctly ─────────────────────────────────────

test('Route accepts Route.Branch and Route.Default in JSX', () => {
  <Route>
    <Route.Branch condition="x > 0">
      <KafkaSink topic="a" />
    </Route.Branch>
    <Route.Default>
      <KafkaSink topic="b" />
    </Route.Default>
  </Route>;
});

// ── Branded type assignability ───────────────────────────────────────

test('TypedConstructNode<C> is assignable to ConstructNode', () => {
  const branch = Route.Branch({ condition: 'x > 0' });
  const node: ConstructNode = branch;
  void node;
});

test('ConstructNode is NOT assignable to TypedConstructNode<C>', () => {
  expectTypeOf<ConstructNode>().not.toMatchTypeOf<TypedConstructNode<'Route.Branch'>>();
});

// ── Sub-component branded return types ───────────────────────────────

test('Route.Branch returns TypedConstructNode<"Route.Branch">', () => {
  expectTypeOf(Route.Branch).returns.toEqualTypeOf<TypedConstructNode<'Route.Branch'>>();
});

test('Route.Default returns TypedConstructNode<"Route.Default">', () => {
  expectTypeOf(Route.Default).returns.toEqualTypeOf<TypedConstructNode<'Route.Default'>>();
});

test('Query sub-components return branded types', () => {
  expectTypeOf(Query.Select).returns.toEqualTypeOf<TypedConstructNode<'Query.Select'>>();
  expectTypeOf(Query.Where).returns.toEqualTypeOf<TypedConstructNode<'Query.Where'>>();
  expectTypeOf(Query.GroupBy).returns.toEqualTypeOf<TypedConstructNode<'Query.GroupBy'>>();
  expectTypeOf(Query.Having).returns.toEqualTypeOf<TypedConstructNode<'Query.Having'>>();
  expectTypeOf(Query.OrderBy).returns.toEqualTypeOf<TypedConstructNode<'Query.OrderBy'>>();
});

test('SideOutput.Sink returns TypedConstructNode<"SideOutput.Sink">', () => {
  expectTypeOf(SideOutput.Sink).returns.toEqualTypeOf<TypedConstructNode<'SideOutput.Sink'>>();
});

test('Validate.Reject returns TypedConstructNode<"Validate.Reject">', () => {
  expectTypeOf(Validate.Reject).returns.toEqualTypeOf<TypedConstructNode<'Validate.Reject'>>();
});

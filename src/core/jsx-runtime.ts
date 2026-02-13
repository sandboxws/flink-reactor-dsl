import type { ConstructNode, NodeKind } from './types.js';

// ── JSX type declarations ────────────────────────────────────────────

/**
 * Global JSX namespace for the flink-reactor custom JSX runtime.
 *
 * - `Element = ConstructNode` — all JSX expressions have type `ConstructNode`
 * - `IntrinsicElements = {}` — empty interface rejects `<div>`, `<span>`, etc. at compile time
 * - `ElementChildrenAttribute` — maps JSX children to the `children` prop
 *
 * Note: In classic JSX mode ("jsx": "react"), `Element` determines the type of ALL
 * JSX expressions. This means branded sub-component types (TypedConstructNode<C>)
 * collapse to ConstructNode in JSX context, preventing compile-time children constraints
 * on components like Route. Branded types still provide value for programmatic API usage
 * and explicit type annotations. Full JSX children constraints would require migrating
 * to "jsx": "react-jsx" automatic mode (a future change).
 */
declare global {
  namespace JSX {
    type Element = ConstructNode;
    interface IntrinsicElements {}
    interface ElementChildrenAttribute {
      children: {};
    }
  }
}

// ── ID generation ────────────────────────────────────────────────────

let nextNodeId = 0;

export function resetNodeIdCounter(): void {
  nextNodeId = 0;
}

function generateNodeId(component: string): string {
  return `${component}_${nextNodeId++}`;
}

// ── Component type → NodeKind mapping ────────────────────────────────

const BUILTIN_KINDS: ReadonlyMap<string, NodeKind> = new Map([
  ['Pipeline', 'Pipeline'],
  ['KafkaSource', 'Source'],
  ['JdbcSource', 'Source'],
  ['GenericSource', 'Source'],
  ['CatalogSource', 'Source'],
  ['KafkaSink', 'Sink'],
  ['JdbcSink', 'Sink'],
  ['FileSystemSink', 'Sink'],
  ['PaimonSink', 'Sink'],
  ['IcebergSink', 'Sink'],
  ['GenericSink', 'Sink'],
  ['Filter', 'Transform'],
  ['Map', 'Transform'],
  ['FlatMap', 'Transform'],
  ['Aggregate', 'Transform'],
  ['Union', 'Transform'],
  ['Deduplicate', 'Transform'],
  ['TopN', 'Transform'],
  ['Route', 'Transform'],
  ['Join', 'Join'],
  ['TemporalJoin', 'Join'],
  ['LookupJoin', 'Join'],
  ['IntervalJoin', 'Join'],
  ['TumbleWindow', 'Window'],
  ['SlideWindow', 'Window'],
  ['SessionWindow', 'Window'],
  ['PaimonCatalog', 'Catalog'],
  ['IcebergCatalog', 'Catalog'],
  ['HiveCatalog', 'Catalog'],
  ['JdbcCatalog', 'Catalog'],
  ['GenericCatalog', 'Catalog'],
  ['Query', 'Transform'],
  ['RawSQL', 'RawSQL'],
  ['UDF', 'UDF'],
  ['MatchRecognize', 'CEP'],
  ['SideOutput', 'Transform'],
  ['SideOutput.Sink', 'Transform'],
  ['Validate', 'Transform'],
  ['Validate.Reject', 'Transform'],
  ['View', 'View'],
  ['LateralJoin', 'Join'],
]);

/** Mutable map combining built-in + plugin-registered component kinds */
let kindMap: Map<string, NodeKind> = new Map(BUILTIN_KINDS);

/**
 * Register additional component kinds from plugins.
 * Merges plugin-provided component→kind mappings into the active kind map.
 */
export function registerComponentKinds(components: ReadonlyMap<string, NodeKind>): void {
  for (const [name, kind] of components) {
    kindMap.set(name, kind);
  }
}

/**
 * Reset the kind map to only built-in components.
 * Used in tests to clean up after plugin registration.
 */
export function resetComponentKinds(): void {
  kindMap = new Map(BUILTIN_KINDS);
}

function resolveKind(component: string): NodeKind {
  return kindMap.get(component) ?? 'Transform';
}

// ── createElement ────────────────────────────────────────────────────

/**
 * Custom JSX factory that builds a construct tree node.
 *
 * - `component`: the string tag name or function component
 * - `props`: the component props (excluding children)
 * - `...children`: nested JSX children (linear pipeline sugar)
 *
 * Children are flattened and attached to the node. This creates
 * implicit edges: parent → child in reading order.
 */
export function createElement(
  component: string | Function,
  props: Record<string, unknown> | null,
  ...children: (ConstructNode | ConstructNode[] | null | undefined)[]
): ConstructNode {
  const resolvedComponent =
    typeof component === 'function' ? component.name : component;
  const id = generateNodeId(resolvedComponent);
  const kind = resolveKind(resolvedComponent);

  const flatChildren = children
    .flat(Infinity)
    .filter((c): c is ConstructNode => c != null && typeof c === 'object' && '_tag' in c === false && 'id' in c);

  const cleanProps = { ...(props ?? {}) };
  delete cleanProps.children;

  const node: ConstructNode = {
    id,
    kind,
    component: resolvedComponent,
    props: cleanProps,
    children: flatChildren,
  };

  return node;
}

// ── Fragment ─────────────────────────────────────────────────────────

/**
 * Fragment groups multiple elements without adding a wrapper node.
 * Returns the children as-is for flattening by the parent createElement.
 */
export function Fragment(props: { children?: ConstructNode[] }): ConstructNode[] {
  return props.children ?? [];
}

// ── JSX automatic runtime exports ────────────────────────────────────

/**
 * jsx() for the automatic JSX transform (single child).
 */
export function jsx(
  type: string | Function,
  props: Record<string, unknown>,
  key?: string,
): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  if (key !== undefined) {
    rest.key = key;
  }

  return createElement(type, rest, ...childArray);
}

/**
 * jsxs() for the automatic JSX transform (multiple children).
 */
export const jsxs = jsx;

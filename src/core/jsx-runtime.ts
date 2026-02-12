import type { ConstructNode, NodeKind } from './types.js';

// ── ID generation ────────────────────────────────────────────────────

let nextNodeId = 0;

export function resetNodeIdCounter(): void {
  nextNodeId = 0;
}

function generateNodeId(component: string): string {
  return `${component}_${nextNodeId++}`;
}

// ── Component type → NodeKind mapping ────────────────────────────────

const KIND_MAP: ReadonlyMap<string, NodeKind> = new Map([
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
]);

function resolveKind(component: string): NodeKind {
  return KIND_MAP.get(component) ?? 'Transform';
}

// ── createElement ────────────────────────────────────────────────────

/**
 * Custom JSX factory that builds a construct tree node.
 *
 * - `component`: the string tag name (e.g., 'KafkaSource', 'Filter')
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

import type { ConstructNode, NodeKind } from "./types.js"

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
    type Element = ConstructNode
    interface IntrinsicElements {}
    interface ElementChildrenAttribute {
      // biome-ignore lint/complexity/noBannedTypes: {} is intentional for JSX ElementChildrenAttribute mapping
      children: {}
    }
  }
}

// ── ID generation ────────────────────────────────────────────────────

let nextNodeId = 0
const usedNodeIds: Set<string> = new Set()

export function resetNodeIdCounter(): void {
  nextNodeId = 0
  usedNodeIds.clear()
}

/**
 * Validate that required props are present in a component factory.
 * Throws a clear error message identifying the component and missing props.
 *
 * Call this at the top of factory functions BEFORE accessing props
 * that would otherwise crash with an unhelpful error (e.g. toSqlIdentifier
 * on an undefined topic).
 */
export function requireProps<T extends object>(
  component: string,
  props: T,
  required: readonly (keyof T & string)[],
): void {
  const missing = required.filter(
    (p) => props[p] === undefined || props[p] === null,
  )
  if (missing.length > 0) {
    const list = missing.map((p) => `\`${p}\``).join(", ")
    throw new Error(
      `<${component}> is missing required prop${missing.length > 1 ? "s" : ""}: ${list}`,
    )
  }
}

/**
 * Convert a string to a valid SQL identifier.
 * Replaces hyphens, dots, and slashes with underscores.
 */
export function toSqlIdentifier(value: string): string {
  return value
    .replace(/[.\-/]/g, "_")
    .replace(/[^a-zA-Z0-9_]/g, "")
    .replace(/^_+|_+$/g, "")
    .replace(/_+/g, "_")
}

function generateNodeId(component: string, nameHint?: string): string {
  const base = nameHint
    ? toSqlIdentifier(nameHint)
    : `${component}_${nextNodeId++}`

  let id = base
  let suffix = 2
  while (usedNodeIds.has(id)) {
    id = `${base}_${suffix}`
    suffix++
  }

  usedNodeIds.add(id)
  if (!nameHint) {
    // Counter already incremented above for auto-generated IDs
  } else {
    nextNodeId++ // keep counter moving for unnamed nodes
  }

  return id
}

// ── Component type → NodeKind mapping ────────────────────────────────

const BUILTIN_KINDS: ReadonlyMap<string, NodeKind> = new Map([
  ["Pipeline", "Pipeline"],
  ["KafkaSource", "Source"],
  ["JdbcSource", "Source"],
  ["GenericSource", "Source"],
  ["DataGenSource", "Source"],
  ["CatalogSource", "Source"],
  ["PostgresCdcPipelineSource", "Source"],
  ["KafkaSink", "Sink"],
  ["JdbcSink", "Sink"],
  ["FileSystemSink", "Sink"],
  ["PaimonSink", "Sink"],
  ["IcebergSink", "Sink"],
  ["GenericSink", "Sink"],
  ["Filter", "Transform"],
  ["Map", "Transform"],
  ["FlatMap", "Transform"],
  ["Aggregate", "Transform"],
  ["Union", "Transform"],
  ["Deduplicate", "Transform"],
  ["TopN", "Transform"],
  ["Route", "Transform"],
  ["StatementSet", "Transform"],
  ["Join", "Join"],
  ["TemporalJoin", "Join"],
  ["LookupJoin", "Join"],
  ["IntervalJoin", "Join"],
  ["TumbleWindow", "Window"],
  ["SlideWindow", "Window"],
  ["SessionWindow", "Window"],
  ["PaimonCatalog", "Catalog"],
  ["IcebergCatalog", "Catalog"],
  ["HiveCatalog", "Catalog"],
  ["JdbcCatalog", "Catalog"],
  ["GenericCatalog", "Catalog"],
  ["Rename", "Transform"],
  ["Drop", "Transform"],
  ["Cast", "Transform"],
  ["Coalesce", "Transform"],
  ["AddField", "Transform"],
  ["Query", "Transform"],
  ["RawSQL", "RawSQL"],
  ["UDF", "UDF"],
  ["MatchRecognize", "CEP"],
  ["SideOutput", "Transform"],
  ["SideOutput.Sink", "Transform"],
  ["Validate", "Transform"],
  ["Validate.Reject", "Transform"],
  ["View", "View"],
  ["LateralJoin", "Join"],
  ["MaterializedTable", "MaterializedTable"],
  ["Qualify", "Qualify"],
])

/** Mutable map combining built-in + plugin-registered component kinds */
let kindMap: Map<string, NodeKind> = new Map(BUILTIN_KINDS)

/**
 * Register additional component kinds from plugins.
 * Merges plugin-provided component→kind mappings into the active kind map.
 */
export function registerComponentKinds(
  components: ReadonlyMap<string, NodeKind>,
): void {
  for (const [name, kind] of components) {
    kindMap.set(name, kind)
  }
}

/**
 * Reset the kind map to only built-in components.
 * Used in tests to clean up after plugin registration.
 */
export function resetComponentKinds(): void {
  kindMap = new Map(BUILTIN_KINDS)
}

function resolveKind(component: string): NodeKind {
  return kindMap.get(component) ?? "Transform"
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
  // biome-ignore lint/complexity/noBannedTypes: Function type is intentional for JSX component factories
  component: string | Function,
  props: Record<string, unknown> | null,
  ...children: (ConstructNode | ConstructNode[] | null | undefined)[]
): ConstructNode {
  // Function components: delegate to the factory so it can set _nameHint,
  // changelogMode, and other derived props before creating the node.
  if (typeof component === "function") {
    const flatChildren = children
      .flat(Infinity)
      .filter(
        (c): c is ConstructNode =>
          c != null &&
          typeof c === "object" &&
          "_tag" in c === false &&
          "id" in c,
      )

    const mergedProps: Record<string, unknown> = { ...(props ?? {}) }
    if (flatChildren.length > 0) {
      mergedProps.children = flatChildren
    }
    return component(mergedProps) as ConstructNode
  }

  // String components: create the node directly
  const nameHint = props?._nameHint as string | undefined
  const id = generateNodeId(component, nameHint)
  const kind = resolveKind(component)

  const flatChildren = children
    .flat(Infinity)
    .filter(
      (c): c is ConstructNode =>
        c != null &&
        typeof c === "object" &&
        "_tag" in c === false &&
        "id" in c,
    )

  const cleanProps = { ...(props ?? {}) }
  delete cleanProps.children
  delete cleanProps._nameHint

  const node: ConstructNode = {
    id,
    kind,
    component,
    props: cleanProps,
    children: flatChildren,
  }

  return node
}

// ── Fragment ─────────────────────────────────────────────────────────

/**
 * Fragment groups multiple elements without adding a wrapper node.
 * Returns the children as-is for flattening by the parent createElement.
 */
export function Fragment(props: {
  children?: ConstructNode[]
}): ConstructNode[] {
  return props.children ?? []
}

// ── JSX automatic runtime exports ────────────────────────────────────

/**
 * jsx() for the automatic JSX transform (single child).
 */
export function jsx(
  // biome-ignore lint/complexity/noBannedTypes: Function type is intentional for JSX component factories
  type: string | Function,
  props: Record<string, unknown>,
  key?: string,
): ConstructNode {
  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  if (key !== undefined) {
    rest.key = key
  }

  return createElement(type, rest, ...childArray)
}

/**
 * jsxs() for the automatic JSX transform (multiple children).
 */
export const jsxs = jsx

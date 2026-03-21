import { createElement } from "@/core/jsx-runtime.js"
import type { SchemaDefinition } from "@/core/schema.js"
import type {
  BaseComponentProps,
  ConstructNode,
  FlinkType,
  TapConfig,
} from "@/core/types.js"

// ── Shared base props for all transforms ────────────────────────────

export interface BaseTransformProps extends BaseComponentProps {
  /** Enable operator tailing for this transform */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Shared component constructor: destructures children from props
 * and delegates to createElement.
 */
function createComponent<P extends BaseTransformProps>(
  name: string,
  props: P,
): ConstructNode {
  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]
  return createElement(name, { ...rest }, ...childArray)
}

// ── Filter ──────────────────────────────────────────────────────────

export interface FilterProps extends BaseTransformProps {
  /** SQL WHERE expression (e.g. "amount > 100 AND status = 'active'") */
  readonly condition: string
}

/**
 * Filter: passes through only rows matching the SQL condition.
 * Preserves the input schema unchanged.
 */
export function Filter(props: FilterProps): ConstructNode {
  return createComponent("Filter", props)
}

// ── Map ─────────────────────────────────────────────────────────────

export interface MapProps extends BaseTransformProps {
  /** Record mapping output field names to SQL expressions */
  readonly select: Record<string, string>
}

/**
 * Map: projects and transforms fields via SQL SELECT expressions.
 * Produces a stream with the projected schema.
 */
export function Map(props: MapProps): ConstructNode {
  return createComponent("Map", props)
}

// ── FlatMap ─────────────────────────────────────────────────────────

export interface FlatMapProps extends BaseTransformProps {
  /** Field name to expand (array or map column) */
  readonly unnest: string
  /** Output field schema for the unnested elements */
  readonly as: Record<string, FlinkType>
}

/**
 * FlatMap: expands array/map columns via CROSS JOIN UNNEST.
 * Each element of the collection becomes a separate row.
 */
export function FlatMap(props: FlatMapProps): ConstructNode {
  return createComponent("FlatMap", props)
}

// ── Aggregate ───────────────────────────────────────────────────────

export interface AggregateProps extends BaseTransformProps {
  /** Fields to group by */
  readonly groupBy: readonly string[]
  /** Record mapping output fields to aggregate expressions (e.g. 'COUNT(*)', 'SUM(amount)') */
  readonly select: Record<string, string>
}

/**
 * Aggregate: groups rows and computes aggregate expressions.
 * Produces a stream with the output schema defined by select.
 */
export function Aggregate(props: AggregateProps): ConstructNode {
  return createComponent("Aggregate", props)
}

// ── Union ───────────────────────────────────────────────────────────

export interface UnionProps extends BaseTransformProps {
  /** Input schemas to validate compatibility (set by the framework during synthesis) */
  readonly inputs?: readonly SchemaDefinition[]
}

/**
 * Union: merges multiple same-schema streams via UNION ALL.
 *
 * When `inputs` schemas are provided, validates that all schemas
 * have matching field names and types. The actual stream inputs
 * are connected via the DAG edges at synthesis time.
 */
export function Union(props: UnionProps): ConstructNode {
  if (props.inputs && props.inputs.length >= 2) {
    validateUnionSchemas(props.inputs)
  }

  return createComponent("Union", props)
}

function validateUnionSchemas(schemas: readonly SchemaDefinition[]): void {
  const reference = schemas[0]
  const refKeys = Object.keys(reference.fields).sort()
  const refSignature = refKeys
    .map((k) => `${k}:${reference.fields[k]}`)
    .join(",")

  for (let i = 1; i < schemas.length; i++) {
    const current = schemas[i]
    const curKeys = Object.keys(current.fields).sort()
    const curSignature = curKeys
      .map((k) => `${k}:${current.fields[k]}`)
      .join(",")

    if (refSignature !== curSignature) {
      throw new Error(
        `Union schema mismatch: input ${i} has fields [${curKeys.join(", ")}] ` +
          `which do not match input 0 fields [${refKeys.join(", ")}]`,
      )
    }
  }
}

// ── Deduplicate ─────────────────────────────────────────────────────

export interface DeduplicateProps extends BaseTransformProps {
  /** Fields forming the deduplication key */
  readonly key: readonly string[]
  /** Field to order by for selecting which row to keep */
  readonly order: string
  /** Keep the first or last row per key */
  readonly keep: "first" | "last"
}

/**
 * Deduplicate: first-row or last-row deduplication using the
 * ROW_NUMBER() window function pattern.
 *
 * Generates: ROW_NUMBER() OVER (PARTITION BY key ORDER BY order [ASC|DESC]) WHERE rownum = 1
 */
export function Deduplicate(props: DeduplicateProps): ConstructNode {
  return createComponent("Deduplicate", props)
}

// ── TopN ────────────────────────────────────────────────────────────

export interface TopNProps extends BaseTransformProps {
  /** Fields to partition the ranking by */
  readonly partitionBy: readonly string[]
  /** Ordering specification: field name → ASC or DESC */
  readonly orderBy: Record<string, "ASC" | "DESC">
  /** Number of top rows to keep per partition */
  readonly n: number
}

/**
 * TopN: ranking within partitions using the ROW_NUMBER() pattern.
 *
 * Generates: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) WHERE rownum <= n
 */
export function TopN(props: TopNProps): ConstructNode {
  return createComponent("TopN", props)
}

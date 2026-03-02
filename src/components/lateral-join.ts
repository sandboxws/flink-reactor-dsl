import { createElement } from "../core/jsx-runtime.js"
import type { SchemaDefinition } from "../core/schema.js"
import type {
  BaseComponentProps,
  ConstructNode,
  FlinkType,
} from "../core/types.js"

// ── LateralJoin ─────────────────────────────────────────────────────

export interface LateralJoinProps extends BaseComponentProps {
  /** Upstream stream to join against */
  readonly input: ConstructNode
  /** TVF name (registered via UDF or built-in like VECTOR_SEARCH) */
  readonly function: string
  /** Arguments passed to the TVF (column refs or literals) */
  readonly args: readonly (string | number)[]
  /** Output column names and types from the TVF */
  readonly as: Record<string, FlinkType>
  /** Join type: 'cross' (default) or 'left' */
  readonly type?: "cross" | "left"
  /** Schema of the combined output */
  readonly outputSchema?: SchemaDefinition
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * LateralJoin: table-valued function join.
 *
 * Joins a stream against a table-valued function (TVF) using the
 * `LATERAL TABLE(...)` Flink SQL pattern. This enables:
 *
 * - Custom UDFs that return multiple rows per input row
 * - Built-in TVFs like `VECTOR_SEARCH` (Flink 2.2+)
 * - Any function registered via `CREATE FUNCTION`
 *
 * Usage:
 * ```tsx
 * <LateralJoin
 *   input={source}
 *   function="parse_address"
 *   args={['shipping_address']}
 *   as={{ street: 'STRING', city: 'STRING', zip: 'STRING' }}
 *   type="left"
 * />
 * ```
 */
export function LateralJoin(props: LateralJoinProps): ConstructNode {
  if (!props.input) {
    throw new Error("LateralJoin requires an input")
  }
  if (!props.function) {
    throw new Error("LateralJoin requires a function name")
  }
  if (!props.args || props.args.length === 0) {
    throw new Error("LateralJoin requires at least one argument")
  }
  if (!props.as || Object.keys(props.as).length === 0) {
    throw new Error("LateralJoin requires output column definitions (as)")
  }

  const { children, input, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  return createElement(
    "LateralJoin",
    { ...rest, input: input.id },
    input,
    ...childArray,
  )
}

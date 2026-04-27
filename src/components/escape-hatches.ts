import { createElement } from "@/core/jsx-runtime.js"
import type { SchemaDefinition } from "@/core/schema.js"
import type { BaseComponentProps, ConstructNode } from "@/core/types.js"

// ── RawSQL ──────────────────────────────────────────────────────────

export interface RawSQLProps extends BaseComponentProps {
  /** Arbitrary SQL string to inline as a subquery */
  readonly sql: string
  /**
   * Input streams referenced by name in the SQL body. Optional: omit when
   * the SQL is self-contained (e.g. a `VALUES` literal). When `<RawSQL>`
   * sits in a sibling chain, the preceding sibling becomes the implicit
   * upstream — so `inputs` is only needed for fan-in (RawSQL referencing
   * two or more named sources at once that aren't already in the chain).
   */
  readonly inputs?: readonly ConstructNode[]
  /** Schema describing the output of the raw SQL */
  readonly outputSchema: SchemaDefinition
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * RawSQL: escape hatch that inlines arbitrary SQL into the pipeline.
 *
 * The optional `inputs` array declares upstream streams referenced by name
 * in the SQL body (for fan-in patterns). The `outputSchema` declares the
 * shape of the result, allowing downstream components to consume it as a
 * typed Stream.
 */
export function RawSQL(props: RawSQLProps): ConstructNode {
  const { inputs, children, ...rest } = props
  const inputArray = inputs ?? []

  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  // Input streams become children so the DAG wires correctly
  return createElement(
    "RawSQL",
    { ...rest, inputIds: inputArray.map((i) => i.id) },
    ...inputArray,
    ...childArray,
  )
}

// ── UDF ─────────────────────────────────────────────────────────────

export interface UDFProps extends BaseComponentProps {
  /** Function name to register in Flink */
  readonly name: string
  /** Fully-qualified Java/Scala class implementing the function */
  readonly className: string
  /** Path to the JAR containing the UDF class */
  readonly jarPath: string
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * UDF: registers a user-defined function via CREATE FUNCTION DDL.
 *
 * The jarPath is collected by the connector resolver so the JAR
 * is included in the deployment classpath.
 */
export function UDF(props: UDFProps): ConstructNode {
  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  return createElement("UDF", { ...rest }, ...childArray)
}

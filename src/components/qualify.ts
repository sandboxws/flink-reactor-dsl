import { createElement } from "@/core/jsx-runtime.js"
import type { BaseComponentProps, ConstructNode } from "@/core/types.js"

// ── Qualify ─────────────────────────────────────────────────────────

export interface QualifyProps extends BaseComponentProps {
  /** SQL expression filtering on a window function alias, e.g. "rn = 1" */
  readonly condition: string
  /** Optional window function expression to add to the SELECT list */
  readonly window?: string
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Qualify: escape hatch that generates a QUALIFY clause for filtering
 * on window function results. Requires Flink >= 2.0.
 *
 * Usage:
 *   <Qualify condition="rn = 1" window="ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `ts`) AS rn" />
 */
export function Qualify(props: QualifyProps): ConstructNode {
  const { children, ...rest } = props

  if (!rest.condition) {
    throw new Error("Qualify requires a 'condition' prop")
  }

  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  return createElement("Qualify", { ...rest }, ...childArray)
}

import { createElement } from "@/core/jsx-runtime.js"
import type { BaseComponentProps, ConstructNode } from "@/core/types.js"

// ── StatementSet ───────────────────────────────────────────────────

export interface StatementSetProps extends BaseComponentProps {
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * StatementSet: transparent grouping node for multiple sink branches.
 *
 * Groups child sinks so they execute within a single
 * `EXECUTE STATEMENT SET BEGIN ... END;` block. This is a passthrough
 * node — it doesn't generate SQL itself. Multiple sinks under a
 * StatementSet produce the same SQL as multiple sinks under Route.
 *
 * The SQL generator already wraps multiple INSERT INTOs in
 * EXECUTE STATEMENT SET automatically. StatementSet makes the
 * intent explicit in the pipeline definition.
 */
export function StatementSet(props: StatementSetProps): ConstructNode {
  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  return createElement("StatementSet", { ...rest }, ...childArray)
}

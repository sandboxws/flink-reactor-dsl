import type { ConstructNode } from "@/core/types.js"
import type { BuildContext } from "./sql-build-context.js"
import { quoteIdentifier as q } from "./sql-identifiers.js"

/**
 * Reference resolution helpers — turn a node id into the SQL identifier
 * string the planner will see.
 *
 * - `resolveRef` is the simple case: most nodes resolve to their backtick-
 *   quoted id, but `CatalogSource` resolves to a fully qualified
 *   `catalog.database.table` reference (the catalog managed the registration,
 *   no temporary view shadowing it).
 * - `resolveSinkRef` is identical-shaped but specialized to sinks: any sink
 *   with `catalogName` resolves to the qualified path, otherwise to the id.
 *   Kept separate from `resolveRef` because callers branch on
 *   `node.kind === "Sink"` and the prop set is sink-specific.
 * - `resolveJoinOperand` returns either `(table-ref, null)` for a Source
 *   operand or `(cte-name, "<cte-name> AS (...)")` for a non-Source operand
 *   (e.g. a Join feeding another Join). The caller appends the CTE to
 *   `WITH (…)` so the join clause can reference the CTE by name.
 *
 * `resolveJoinOperand` takes `buildQuery` as a callback to break the
 * dependency cycle: `sql-refs.ts` doesn't know how to build query SQL, but
 * the orchestrator and (post-C.15) the dispatcher do. Passing the function
 * keeps `sql-refs.ts` decoupled from the dispatcher's shape.
 */

export function resolveRef(ctx: BuildContext, nodeId: string): string {
  const node = ctx.nodeIndex.get(nodeId)
  if (!node) return q(nodeId)

  if (node.component === "CatalogSource") {
    return `${q(String(node.props.catalogName))}.${q(String(node.props.database))}.${q(String(node.props.table))}`
  }

  return q(nodeId)
}

export function resolveSinkRef(sink: ConstructNode): string {
  if (sink.props.catalogName) {
    return `${q(String(sink.props.catalogName))}.${q(String(sink.props.database))}.${q(String(sink.props.table))}`
  }
  return q(sink.id)
}

/**
 * Resolve a join operand (left/right input).
 * If the operand is a Source, returns a simple table reference.
 * If the operand is a non-Source (e.g. another Join), inlines its SQL
 * as a CTE so the join can reference it by name.
 */
export function resolveJoinOperand(
  ctx: BuildContext,
  nodeId: string,
  buildQuery: (ctx: BuildContext, node: ConstructNode) => string,
): { ref: string; cte: string | null } {
  const node = ctx.nodeIndex.get(nodeId)
  if (!node || node.kind === "Source") {
    return { ref: resolveRef(ctx, nodeId), cte: null }
  }
  // Non-source operand — build its query and wrap as a CTE
  const sql = buildQuery(ctx, node)
  const cteName = q(nodeId)
  return { ref: cteName, cte: `${cteName} AS (\n${sql}\n)` }
}

import type { ConstructNode } from "@/core/types.js"
import { quoteIdentifier as q } from "./sql/sql-identifiers.js"
import type { BuildContext } from "./sql-build-context.js"

/**
 * Shared infrastructure for the per-component query builders.
 *
 * `getUpstream` is the single helper here today: every builder that needs
 * a "FROM <upstream>" clause asks `getUpstream` to produce both the SQL
 * and a usable table reference, branching on whether the upstream is a
 * simple Source (use the table name directly) or a built subquery (wrap
 * in parentheses). Centralising this here keeps each builder file small
 * and focused on its component's clause-shape.
 *
 * The dispatcher recursion (`ctx.buildQuery`) is invoked here — see the
 * `BuildQueryFn` doc on `BuildContext` for the rationale.
 */

export interface UpstreamSql {
  sql: string
  sourceRef: string
  isSimple: boolean
}

/**
 * Get the upstream SQL and source table reference from a node's children.
 */
export function getUpstream(
  ctx: BuildContext,
  node: ConstructNode,
): UpstreamSql {
  if (node.children.length === 0) {
    return {
      sql: "SELECT * FROM unknown",
      sourceRef: "unknown",
      isSimple: false,
    }
  }

  const child = node.children[0]

  // VirtualRef: either a simple table reference or an embedded subquery
  if (child.component === "VirtualRef") {
    if (child.props._sql) {
      return {
        sql: child.props._sql as string,
        sourceRef: child.id,
        isSimple: false,
      }
    }
    return {
      sql: `SELECT * FROM ${q(child.id)}`,
      sourceRef: q(child.id),
      isSimple: true,
    }
  }

  // If the child is a source and no plugin overrides it, return a simple reference
  if (
    child.kind === "Source" &&
    !ctx.pluginSqlGenerators?.has(child.component)
  ) {
    const ref =
      child.component === "CatalogSource"
        ? `${q(String(child.props.catalogName))}.${q(String(child.props.database))}.${q(String(child.props.table))}`
        : q(child.id)
    return { sql: `SELECT * FROM ${ref}`, sourceRef: ref, isSimple: true }
  }

  // Otherwise build the child query
  const childSql = ctx.buildQuery(ctx, child)
  return { sql: childSql, sourceRef: child.id, isSimple: false }
}

import { FlinkVersionCompat } from "@/core/flink-compat.js"
import type { ConstructNode } from "@/core/types.js"
import {
  type BuildContext,
  pushFragment,
  shiftFragmentsSince,
} from "./sql-build-context.js"
import { quoteIdentifier as q } from "./sql-identifiers.js"
import { getUpstream } from "./sql-query-helpers.js"

/**
 * Simple per-component query builders — each one takes a node and returns
 * a SELECT expression that the dispatcher will emit verbatim or wrap in a
 * CTE for downstream consumers.
 *
 * "Simple" here means: shape is `[SELECT projection] FROM <upstream> [WHERE
 * | GROUP BY | QUALIFY | ROW_NUMBER OVER]`, no schema introspection
 * required, no cross-references to other nodes. Aggregate sits in its own
 * file because it must coordinate with Window for the pseudo-window
 * forwarding pattern. Field-shape transforms (Rename, Drop, Cast,
 * Coalesce, AddField) sit in `sql-query-field-ops.ts` because they all
 * need `resolveNodeSchema` to inspect the upstream column list.
 */

// ── Filter ──────────────────────────────────────────────────────────

export function buildFilterQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const condition = node.props.condition as string
  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)

  const wherePart = ` WHERE ${condition}`

  if (upstream.isSimple) {
    const result = `SELECT * FROM ${upstream.sourceRef}${wherePart}`
    pushFragment(ctx, result.length - wherePart.length, wherePart.length, node)
    return result
  }
  const prefix = `SELECT * FROM (\n`
  const result = `${prefix}${upstream.sql}\n)${wherePart}`
  shiftFragmentsSince(ctx, fragStart, prefix.length)
  pushFragment(ctx, result.length - wherePart.length, wherePart.length, node)
  return result
}

// ── Map ─────────────────────────────────────────────────────────────

export function buildMapQuery(ctx: BuildContext, node: ConstructNode): string {
  const select = node.props.select as Record<string, string>
  const projections = Object.entries(select)
    .map(([alias, expr]) => `${expr} AS ${q(alias)}`)
    .join(", ")

  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)

  const selectPart = `SELECT ${projections}`

  if (upstream.isSimple) {
    const result = `${selectPart} FROM ${upstream.sourceRef}`
    pushFragment(ctx, 0, selectPart.length, node)
    return result
  }
  const prefix = `${selectPart} FROM (\n`
  const result = `${prefix}${upstream.sql}\n)`
  shiftFragmentsSince(ctx, fragStart, prefix.length)
  pushFragment(ctx, 0, selectPart.length, node)
  return result
}

// ── FlatMap ─────────────────────────────────────────────────────────

export function buildFlatMapQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const unnestField = node.props.unnest as string
  const asFields = node.props.as as Record<string, string>
  const aliases = Object.keys(asFields).map(q).join(", ")

  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)
  const ref = upstream.sourceRef

  // sourceRef is already backtick-quoted for simple refs — use directly
  const selectPart = `SELECT ${ref}.*, ${aliases}`
  const fromPart = upstream.isSimple
    ? ref
    : `(\n${upstream.sql}\n) AS ${q(ref)}`
  const crossJoinPart = `CROSS JOIN UNNEST(${ref}.${q(unnestField)}) AS T(${aliases})`
  const result = `${selectPart} FROM ${fromPart} ${crossJoinPart}`

  if (!upstream.isSimple) {
    const prefix = `${selectPart} FROM (\n`
    shiftFragmentsSince(ctx, fragStart, prefix.length)
  }

  // Track: the added aliases in SELECT and the CROSS JOIN UNNEST are FlatMap's contribution
  pushFragment(ctx, 0, selectPart.length, node)
  pushFragment(
    ctx,
    result.length - crossJoinPart.length,
    crossJoinPart.length,
    node,
  )

  return result
}

// ── Union ───────────────────────────────────────────────────────────

export function buildUnionQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const parts: string[] = []

  for (const child of node.children) {
    parts.push(ctx.buildQuery(ctx, child))
  }

  if (parts.length === 0) return "SELECT * FROM unknown"
  const result = parts.join("\nUNION ALL\n")

  // Track each "UNION ALL" keyword as a Union fragment
  if (parts.length > 1) {
    let offset = parts[0].length + 1 // +1 for the \n before "UNION ALL"
    for (let i = 1; i < parts.length; i++) {
      pushFragment(ctx, offset, "UNION ALL".length, node)
      offset += "UNION ALL".length + 1 + parts[i].length + 1 // +1 for \n after, +1 for \n before next
    }
  }

  return result
}

// ── Deduplicate ─────────────────────────────────────────────────────

export function buildDeduplicateQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const key = node.props.key as readonly string[]
  const order = node.props.order as string
  const keep = node.props.keep as "first" | "last"

  const partitionBy = key.map(q).join(", ")
  const orderDir = keep === "first" ? "ASC" : "DESC"

  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)
  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  if (FlinkVersionCompat.isVersionAtLeast(ctx.version, "2.0")) {
    const selectPart = `SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${q(order)} ${orderDir}) AS rownum`
    const qualifyPart = "QUALIFY rownum = 1"
    const result = `${selectPart}\nFROM ${fromClause}\n${qualifyPart}`
    if (!upstream.isSimple)
      shiftFragmentsSince(
        ctx,
        fragStart,
        selectPart.length + 1 + "FROM (\n".length,
      )
    pushFragment(ctx, 0, selectPart.length, node)
    pushFragment(
      ctx,
      result.length - qualifyPart.length,
      qualifyPart.length,
      node,
    )
    return result
  }

  const innerSelect = `  SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${q(order)} ${orderDir}) AS rownum`
  const wherePart = ") WHERE rownum = 1"
  const result = `SELECT * FROM (\n${innerSelect}\n  FROM ${fromClause}\n${wherePart}`
  if (!upstream.isSimple)
    shiftFragmentsSince(
      ctx,
      fragStart,
      "SELECT * FROM (\n".length + innerSelect.length + 1 + "  FROM (\n".length,
    )
  pushFragment(ctx, 0, "SELECT * FROM (".length, node)
  pushFragment(ctx, "SELECT * FROM (\n".length, innerSelect.length, node)
  pushFragment(ctx, result.length - wherePart.length, wherePart.length, node)
  return result
}

// ── TopN ────────────────────────────────────────────────────────────

export function buildTopNQuery(ctx: BuildContext, node: ConstructNode): string {
  const partitionBy = (node.props.partitionBy as readonly string[])
    .map(q)
    .join(", ")
  const orderBy = node.props.orderBy as Record<string, "ASC" | "DESC">
  const n = node.props.n as number

  const orderClause = Object.entries(orderBy)
    .map(([field, dir]) => `${q(field)} ${dir}`)
    .join(", ")

  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)
  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  if (FlinkVersionCompat.isVersionAtLeast(ctx.version, "2.0")) {
    const selectPart = `SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${orderClause}) AS rownum`
    const qualifyPart = `QUALIFY rownum <= ${n}`
    const result = `${selectPart}\nFROM ${fromClause}\n${qualifyPart}`
    if (!upstream.isSimple)
      shiftFragmentsSince(
        ctx,
        fragStart,
        selectPart.length + 1 + "FROM (\n".length,
      )
    pushFragment(ctx, 0, selectPart.length, node)
    pushFragment(
      ctx,
      result.length - qualifyPart.length,
      qualifyPart.length,
      node,
    )
    return result
  }

  const innerSelect = `  SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${orderClause}) AS rownum`
  const wherePart = `) WHERE rownum <= ${n}`
  const result = `SELECT * FROM (\n${innerSelect}\n  FROM ${fromClause}\n${wherePart}`
  if (!upstream.isSimple)
    shiftFragmentsSince(
      ctx,
      fragStart,
      "SELECT * FROM (\n".length + innerSelect.length + 1 + "  FROM (\n".length,
    )
  pushFragment(ctx, 0, "SELECT * FROM (".length, node)
  pushFragment(ctx, "SELECT * FROM (\n".length, innerSelect.length, node)
  pushFragment(ctx, result.length - wherePart.length, wherePart.length, node)
  return result
}

// ── Qualify (escape hatch) ───────────────────────────────────────────

export function buildQualifyQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const condition = node.props.condition as string
  const windowExpr = node.props.window as string | undefined

  const upstream = getUpstream(ctx, node)

  const selectList = windowExpr ? `*, ${windowExpr}` : "*"
  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  return [
    `SELECT ${selectList}`,
    `FROM ${fromClause}`,
    `QUALIFY ${condition}`,
  ].join("\n")
}

import type { ConstructNode } from "@/core/types.js"
import { toInterval } from "./sql/sql-duration.js"
import { quoteIdentifier as q } from "./sql/sql-identifiers.js"
import {
  type BuildContext,
  pushFragment,
  shiftFragmentsSince,
} from "./sql-build-context.js"
import { getUpstream } from "./sql-query-helpers.js"

/**
 * Aggregate and Window builders. They live in the same file because of a
 * tight coupling: a forward-nested `<Aggregate>{ child: <TumbleWindow> }`
 * tree must emit a single `SELECT … FROM TABLE(TVF(...)) GROUP BY …`
 * statement, not a `GROUP BY` wrapped around a `SELECT * FROM (TVF)`.
 *
 * Why: the Flink planner's `FOR SYSTEM_TIME AS OF` validation (used by
 * downstream TemporalJoins) checks the *immediate* time-attribute on the
 * upstream relation. Wrapping the TVF in a subquery strips the
 * time-attribute metadata from `window_start`/`window_end`, so the
 * temporal join fails at planner time (BUG-030 in the issue tracker).
 *
 * The fix in `buildAggregateQuery` is the "pseudo-window forwarding"
 * pattern: when the first child is a Window, re-shape it to carry this
 * Aggregate as its child and delegate to `buildWindowQuery`. That builder
 * sees both Aggregate and the source in one frame and can emit the
 * single-statement form. Splitting these into separate files would force
 * either a back-import (Aggregate → Window) or a registry indirection in
 * the middle of synthesis — neither is worth it, so they cohabit.
 */

// ── Aggregate ───────────────────────────────────────────────────────

export function buildAggregateQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  // Forward-nesting (Aggregate { children: Window }): emit the single
  // `SELECT … FROM TABLE(TVF(...)) GROUP BY …` pattern by delegating to
  // buildWindowQuery with a pseudo-Window that carries this Aggregate as
  // a child. The default path below would wrap the TVF in a
  // `SELECT * FROM (TABLE(TVF))` subquery, which strips the time-attribute
  // metadata on window_start/window_end and breaks downstream TemporalJoin
  // `FOR SYSTEM_TIME AS OF` validation (BUG-030).
  const firstChild = node.children[0]
  if (
    firstChild &&
    (firstChild.component === "TumbleWindow" ||
      firstChild.component === "SlideWindow" ||
      firstChild.component === "SessionWindow")
  ) {
    const pseudoWindow: ConstructNode = {
      ...firstChild,
      children: [...firstChild.children, node],
    }
    return buildWindowQuery(ctx, pseudoWindow)
  }

  const groupBy = node.props.groupBy as readonly string[]
  const select = node.props.select as Record<string, string>
  const groupBySet = new Set(groupBy)

  // Detect window TVF metadata columns referenced in select expressions.
  // When the upstream is a windowed TVF (TUMBLE/HOP/SESSION), Flink requires
  // window_start and window_end to appear in GROUP BY if used in SELECT.
  const windowMetaCols = ["window_start", "window_end"] as const
  const referencedWindowCols = windowMetaCols.filter((wc) =>
    Object.values(select).some((expr) => expr === wc),
  )

  const groupColParts = groupBy.map(q)
  for (const wc of referencedWindowCols) {
    groupColParts.push(wc)
  }
  const groupCols = groupColParts.join(", ")

  const projections = [
    ...groupBy.map(q),
    // Skip select entries that duplicate a groupBy column
    ...Object.entries(select)
      .filter(([alias]) => !groupBySet.has(alias))
      .map(([alias, expr]) => `${expr} AS ${q(alias)}`),
  ].join(", ")

  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)

  const selectPart = `SELECT ${projections}`
  const groupByPart = ` GROUP BY ${groupCols}`

  if (upstream.isSimple) {
    const result = `${selectPart} FROM ${upstream.sourceRef}${groupByPart}`
    pushFragment(ctx, 0, selectPart.length, node)
    pushFragment(
      ctx,
      result.length - groupByPart.length,
      groupByPart.length,
      node,
    )
    return result
  }
  const prefix = `${selectPart} FROM (\n`
  const result = `${prefix}${upstream.sql}\n)${groupByPart}`
  shiftFragmentsSince(ctx, fragStart, prefix.length)
  pushFragment(ctx, 0, selectPart.length, node)
  pushFragment(
    ctx,
    result.length - groupByPart.length,
    groupByPart.length,
    node,
  )
  return result
}

// ── Window ──────────────────────────────────────────────────────────

export function buildWindowQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const windowCol = node.props.on as string

  // Find the Aggregate child (if windowed aggregation)
  const aggChild = node.children.find((c) => c.component === "Aggregate")

  // Find the source feeding the window (non-Aggregate child)
  const sourceChild = node.children.find((c) => c.component !== "Aggregate")
  const upstream = sourceChild
    ? getUpstream(ctx, { children: [sourceChild] } as ConstructNode)
    : getUpstream(ctx, node)
  // Flink TVF syntax requires TABLE <identifier>, not TABLE (<subquery>).
  // When the upstream is a subquery, lift it into a CTE so the TVF gets
  // a named table reference.
  let ctePrefix = ""
  let sourceRef: string
  if (upstream.isSimple) {
    sourceRef = upstream.sourceRef
  } else {
    const cteName = `_windowed_input`
    ctePrefix = `WITH ${q(cteName)} AS (\n${upstream.sql}\n)\n`
    sourceRef = q(cteName)
  }

  const tvf = buildWindowTvf(node, sourceRef, windowCol)

  if (!aggChild) {
    return `${ctePrefix}SELECT * FROM TABLE(\n  ${tvf}\n)`
  }

  const groupBy = aggChild.props.groupBy as readonly string[]
  const select = aggChild.props.select as Record<string, string>

  const groupCols = [...groupBy.map(q), "window_start", "window_end"].join(", ")

  const groupBySet = new Set(groupBy)
  const projections = [
    ...groupBy.map(q),
    // Skip select entries that duplicate a groupBy column
    ...Object.entries(select)
      .filter(([alias]) => !groupBySet.has(alias))
      .map(([alias, expr]) => `${expr} AS ${q(alias)}`),
    "window_start",
    "window_end",
  ].join(", ")

  return `${ctePrefix}SELECT ${projections} FROM TABLE(\n  ${tvf}\n) GROUP BY ${groupCols}`
}

function buildWindowTvf(
  node: ConstructNode,
  sourceRef: string,
  windowCol: string,
): string {
  switch (node.component) {
    case "TumbleWindow": {
      const size = toInterval(node.props.size as string)
      return `TUMBLE(TABLE ${sourceRef}, DESCRIPTOR(${q(windowCol)}), ${size})`
    }
    case "SlideWindow": {
      const size = toInterval(node.props.size as string)
      const slide = toInterval(node.props.slide as string)
      return `HOP(TABLE ${sourceRef}, DESCRIPTOR(${q(windowCol)}), ${slide}, ${size})`
    }
    case "SessionWindow": {
      const gap = toInterval(node.props.gap as string)
      return `SESSION(TABLE ${sourceRef}, DESCRIPTOR(${q(windowCol)}), ${gap})`
    }
    default:
      return `UNKNOWN_WINDOW(TABLE ${sourceRef})`
  }
}

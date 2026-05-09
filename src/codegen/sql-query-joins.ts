import type { SchemaDefinition } from "@/core/schema.js"
import type { ConstructNode } from "@/core/types.js"
import type { BuildContext } from "./sql-build-context.js"
import { quoteIdentifier as q } from "./sql-identifiers.js"
import { sameNameJoinKeys } from "./sql-join-helpers.js"
import { resolveJoinOperand, resolveRef } from "./sql-refs.js"

/**
 * The 5 join builders — Join, TemporalJoin, LookupJoin, IntervalJoin,
 * LateralJoin — clustered together because they share the BUG-017
 * "same-name join key" pruning pattern. Every variant calls
 * `sameNameJoinKeys` to detect a `SELECT * FROM left JOIN right ON
 * a.col = b.col`-style join, then qualifies the ON clause with side refs
 * and uses `buildJoinProjectionSkippingRightCols` to drop the duplicate
 * right-side column from the projection. Without that pruning, the
 * downstream sink sees two columns named `col` and breaks.
 *
 * The single shared helper, `buildJoinProjectionSkippingRightCols`, is
 * file-private — it doesn't need to leave this module.
 */

// ── Join ────────────────────────────────────────────────────────────

export function buildJoinQuery(ctx: BuildContext, node: ConstructNode): string {
  const joinType = (node.props.type as string) ?? "inner"
  const onCondition = node.props.on as string
  const hints = node.props.hints as { broadcast?: "left" | "right" } | undefined
  const leftId = node.props.left as string
  const rightId = node.props.right as string

  const left = resolveJoinOperand(ctx, leftId, ctx.buildQuery)
  const right = resolveJoinOperand(ctx, rightId, ctx.buildQuery)
  const ctes = [left.cte, right.cte].filter(Boolean)
  const ctePrefix = ctes.length > 0 ? `WITH ${ctes.join(",\n")}\n` : ""

  if (joinType === "anti") {
    return `${ctePrefix}SELECT ${left.ref}.* FROM ${left.ref} WHERE NOT EXISTS (\n  SELECT 1 FROM ${right.ref} WHERE ${onCondition}\n)`
  }

  if (joinType === "semi") {
    return `${ctePrefix}SELECT ${left.ref}.* FROM ${left.ref} WHERE EXISTS (\n  SELECT 1 FROM ${right.ref} WHERE ${onCondition}\n)`
  }

  const sqlJoinType =
    joinType === "full" ? "FULL OUTER" : joinType.toUpperCase()

  const hintClause = hints?.broadcast
    ? `/*+ BROADCAST(${resolveRef(ctx, hints.broadcast === "left" ? leftId : rightId)}) */ `
    : ""

  // Same shared-key disambiguation as temporal / interval / lookup joins:
  // if both sides reference the same column in the ON clause, qualify
  // with side refs and drop the duplicate right-side column from SELECT *.
  const sharedKeys = sameNameJoinKeys(onCondition)
  const qualifiedOn = sharedKeys
    ? sharedKeys
        .map((k) => `${left.ref}.${q(k)} = ${right.ref}.${q(k)}`)
        .join(" AND ")
    : onCondition
  const projection = sharedKeys
    ? buildJoinProjectionSkippingRightCols(
        ctx,
        left.ref,
        right.ref,
        rightId,
        sharedKeys,
      )
    : "*"

  return `${ctePrefix}SELECT ${hintClause}${projection} FROM ${left.ref} ${sqlJoinType} JOIN ${right.ref} ON ${qualifiedOn}`
}

// ── Temporal Join ───────────────────────────────────────────────────

export function buildTemporalJoinQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const onCondition = node.props.on as string
  const asOf = node.props.asOf as string
  const streamId = node.props.stream as string
  const temporalId = node.props.temporal as string

  const stream = resolveJoinOperand(ctx, streamId, ctx.buildQuery)
  const temporal = resolveJoinOperand(ctx, temporalId, ctx.buildQuery)
  const ctes = [stream.cte, temporal.cte].filter(Boolean)
  const ctePrefix = ctes.length > 0 ? `WITH ${ctes.join(",\n")}\n` : ""

  const sharedKeys = sameNameJoinKeys(onCondition)
  const qualifiedOn = sharedKeys
    ? sharedKeys
        .map((k) => `${stream.ref}.${q(k)} = ${temporal.ref}.${q(k)}`)
        .join(" AND ")
    : onCondition

  // When the join keys have the same name on both sides, `SELECT *`
  // produces duplicate columns that break downstream sinks expecting one.
  // Project the left side fully and drop the duplicate join keys from the
  // right.
  const projection = sharedKeys
    ? buildJoinProjectionSkippingRightCols(
        ctx,
        stream.ref,
        temporal.ref,
        temporalId,
        sharedKeys,
      )
    : "*"

  return `${ctePrefix}SELECT ${projection} FROM ${stream.ref} LEFT JOIN ${temporal.ref} FOR SYSTEM_TIME AS OF ${stream.ref}.${q(asOf)} ON ${qualifiedOn}`
}

/**
 * Project `left.*, right.col1, right.col2, ...` — i.e., the left side fully
 * plus every right-side column except those in `skipCols`. Used to suppress
 * the duplicate join-key column(s) that `SELECT *` would otherwise produce
 * on shared-name joins. Falls back to `*` when the right side's schema
 * isn't directly knowable (currently: only `Source` nodes with
 * `props.schema` are introspectable).
 */
function buildJoinProjectionSkippingRightCols(
  ctx: BuildContext,
  leftRef: string,
  rightRef: string,
  rightNodeId: string,
  skipCols: readonly string[],
): string {
  const rightNode = ctx.nodeIndex.get(rightNodeId)
  const schema =
    rightNode?.kind === "Source"
      ? (rightNode.props.schema as SchemaDefinition | undefined)
      : undefined
  if (!schema) return "*"
  const skipSet = new Set(skipCols)
  const cols = Object.keys(schema.fields).filter((c) => !skipSet.has(c))
  if (cols.length === 0) return `${leftRef}.*`
  return `${leftRef}.*, ${cols.map((c) => `${rightRef}.${q(c)}`).join(", ")}`
}

// ── Lookup Join ─────────────────────────────────────────────────────

export function buildLookupJoinQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const onCondition = node.props.on as string
  const inputId = node.props.input as string
  const table = node.props.table as string
  const select = node.props.select as Record<string, string> | undefined

  const input = resolveJoinOperand(ctx, inputId, ctx.buildQuery)

  // Wrap the driving input in a CTE that adds PROCTIME() AS proc_time.
  // Lookup joins require a processing-time attribute on the left side;
  // keeping it local to this join avoids mutating every source's DDL.
  const procCteName = `_lookup_${node.id}`
  const procCte = `${q(procCteName)} AS (SELECT *, PROCTIME() AS proc_time FROM ${input.ref})`
  const ctes = [input.cte, procCte].filter(Boolean)
  const ctePrefix = `WITH ${ctes.join(",\n")}\n`

  const leftRef = q(procCteName)
  const rightRef = q(table)

  // Disambiguate same-name join keys (BUG-017 pattern) — qualify with
  // side refs and prune duplicate right columns from SELECT *.
  const sharedKeys = sameNameJoinKeys(onCondition)
  const qualifiedOn = sharedKeys
    ? sharedKeys
        .map((k) => `${leftRef}.${q(k)} = ${rightRef}.${q(k)}`)
        .join(" AND ")
    : onCondition

  let projection: string
  if (select) {
    projection = Object.entries(select)
      .map(([alias, expr]) => `${expr} AS ${q(alias)}`)
      .join(", ")
  } else if (sharedKeys) {
    projection = buildJoinProjectionSkippingRightCols(
      ctx,
      leftRef,
      rightRef,
      table,
      sharedKeys,
    )
  } else {
    projection = "*"
  }

  return `${ctePrefix}SELECT ${projection} FROM ${leftRef} LEFT JOIN ${rightRef} FOR SYSTEM_TIME AS OF ${leftRef}.proc_time ON ${qualifiedOn}`
}

// ── Interval Join ───────────────────────────────────────────────────

export function buildIntervalJoinQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const joinType = (node.props.type as string) ?? "inner"
  const onCondition = node.props.on as string
  const interval = node.props.interval as { from: string; to: string }
  const leftId = node.props.left as string
  const rightId = node.props.right as string

  const left = resolveJoinOperand(ctx, leftId, ctx.buildQuery)
  const right = resolveJoinOperand(ctx, rightId, ctx.buildQuery)
  const ctes = [left.cte, right.cte].filter(Boolean)
  const ctePrefix = ctes.length > 0 ? `WITH ${ctes.join(",\n")}\n` : ""

  const sqlJoinType = joinType === "inner" ? "" : `${joinType.toUpperCase()} `

  // Interval join: right.watermarkCol BETWEEN left.from AND left.to
  const rightNode = ctx.nodeIndex.get(rightId)
  const rightSchema = rightNode?.props.schema as
    | { watermark?: { column: string } }
    | undefined
  const rightTimeCol = rightSchema?.watermark?.column
  const betweenCol = rightTimeCol
    ? `${right.ref}.${q(rightTimeCol)}`
    : `${right.ref}.${interval.from}`

  // When the join key has the same column name on both sides, `SELECT *`
  // produces a duplicate column that breaks downstream sinks. Project left
  // fully and drop the duplicate join key from the right. Mirrors the fix
  // in buildTemporalJoinQuery / BUG-017.
  const sharedKeys = sameNameJoinKeys(onCondition)
  const projection = sharedKeys
    ? buildJoinProjectionSkippingRightCols(
        ctx,
        left.ref,
        right.ref,
        rightId,
        sharedKeys,
      )
    : "*"

  return `${ctePrefix}SELECT ${projection} FROM ${left.ref} ${sqlJoinType}JOIN ${right.ref} ON ${onCondition} AND ${betweenCol} BETWEEN ${left.ref}.${interval.from} AND ${left.ref}.${interval.to}`
}

// ── LateralJoin ─────────────────────────────────────────────────────

export function buildLateralJoinQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const funcName = node.props.function as string
  const args = node.props.args as readonly (string | number)[]
  const asFields = node.props.as as Record<string, string>
  const joinType = (node.props.type as string) ?? "cross"
  const inputId = node.props.input as string

  const inputRef = resolveRef(ctx, inputId)
  const aliases = Object.keys(asFields).map(q).join(", ")
  const argList = args
    .map((a) => (typeof a === "string" ? a : String(a)))
    .join(", ")

  const sqlJoinType = joinType === "left" ? "LEFT JOIN" : "JOIN"

  return `SELECT ${inputRef}.*, T.${Object.keys(asFields).map(q).join(`, T.`)} FROM ${inputRef} ${sqlJoinType} LATERAL TABLE(${funcName}(${argList})) AS T(${aliases})`
}

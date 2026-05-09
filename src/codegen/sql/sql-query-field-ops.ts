import type { ConstructNode } from "@/core/types.js"
import { resolveNodeSchema } from "../schema-introspect.js"
import {
  type BuildContext,
  pushFragment,
  shiftFragmentsSince,
} from "./sql-build-context.js"
import { quoteIdentifier as q } from "./sql-identifiers.js"
import { getUpstream } from "./sql-query-helpers.js"

/**
 * Field-shape transforms — Rename, Drop, Cast, Coalesce, AddField.
 *
 * What ties this group together: every builder needs the *upstream column
 * list* to emit an explicit SELECT projection (rather than `SELECT *`),
 * because each component edits one or more columns by name. Hence the
 * shared dependency on `resolveNodeSchema(child, ctx.nodeIndex)`. When
 * the schema isn't resolvable (`null`), the builder falls back to
 * `SELECT *` — the planner will fail later, but synthesis stays
 * deterministic and the dashboard still attributes the statement.
 *
 * Each builder also pushes per-column fragments where applicable (one
 * fragment per renamed/cast/coalesced column) so the dashboard can
 * highlight just the edited columns rather than the whole projection.
 */

// ── Rename ──────────────────────────────────────────────────────────

export function buildRenameQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const columns = node.props.columns as Record<string, string>
  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)

  const schema = resolveNodeSchema(node.children[0], ctx.nodeIndex)
  if (!schema) {
    // Unresolvable upstream — fall back to SELECT *
    if (upstream.isSimple) return `SELECT * FROM ${upstream.sourceRef}`
    return `SELECT * FROM (\n${upstream.sql}\n)`
  }

  // Build projections while tracking per-column offsets for renamed columns
  const colParts: { text: string; renamed: boolean }[] = schema.map((c) => {
    const newName = columns[c.name]
    return newName
      ? { text: `${q(c.name)} AS ${q(newName)}`, renamed: true }
      : { text: q(c.name), renamed: false }
  })

  const projections = colParts.map((p) => p.text).join(", ")
  const selectPart = `SELECT ${projections}`

  // Push a fragment for each renamed column (not the entire SELECT)
  const pushRenameFragments = (baseOffset: number): void => {
    let offset = baseOffset + "SELECT ".length
    for (let i = 0; i < colParts.length; i++) {
      if (colParts[i].renamed) {
        pushFragment(ctx, offset, colParts[i].text.length, node)
      }
      offset += colParts[i].text.length
      if (i < colParts.length - 1) offset += 2 // ", "
    }
  }

  if (upstream.isSimple) {
    const result = `${selectPart} FROM ${upstream.sourceRef}`
    pushRenameFragments(0)
    return result
  }
  const prefix = `${selectPart} FROM (\n`
  const result = `${prefix}${upstream.sql}\n)`
  shiftFragmentsSince(ctx, fragStart, prefix.length)
  pushRenameFragments(0)
  return result
}

// ── Drop ────────────────────────────────────────────────────────────

export function buildDropQuery(ctx: BuildContext, node: ConstructNode): string {
  const dropCols = new Set(node.props.columns as readonly string[])
  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)

  const schema = resolveNodeSchema(node.children[0], ctx.nodeIndex)
  if (!schema) {
    if (upstream.isSimple) return `SELECT * FROM ${upstream.sourceRef}`
    return `SELECT * FROM (\n${upstream.sql}\n)`
  }

  const projections = schema
    .filter((c) => !dropCols.has(c.name))
    .map((c) => q(c.name))
    .join(", ")

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

// ── Cast ────────────────────────────────────────────────────────────

export function buildCastQuery(ctx: BuildContext, node: ConstructNode): string {
  const castCols = node.props.columns as Record<string, string>
  const safe = node.props.safe as boolean | undefined
  const castFn = safe ? "TRY_CAST" : "CAST"
  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)

  const schema = resolveNodeSchema(node.children[0], ctx.nodeIndex)
  if (!schema) {
    if (upstream.isSimple) return `SELECT * FROM ${upstream.sourceRef}`
    return `SELECT * FROM (\n${upstream.sql}\n)`
  }

  // Build projections while tracking per-column offsets for cast columns
  const colParts: { text: string; cast: boolean }[] = schema.map((c) => {
    const targetType = castCols[c.name]
    return targetType
      ? {
          text: `${castFn}(${q(c.name)} AS ${targetType}) AS ${q(c.name)}`,
          cast: true,
        }
      : { text: q(c.name), cast: false }
  })

  const projections = colParts.map((p) => p.text).join(", ")
  const selectPart = `SELECT ${projections}`

  // Push a fragment for each cast column only
  const pushCastFragments = (baseOffset: number): void => {
    let offset = baseOffset + "SELECT ".length
    for (let i = 0; i < colParts.length; i++) {
      if (colParts[i].cast) {
        pushFragment(ctx, offset, colParts[i].text.length, node)
      }
      offset += colParts[i].text.length
      if (i < colParts.length - 1) offset += 2 // ", "
    }
  }

  if (upstream.isSimple) {
    const result = `${selectPart} FROM ${upstream.sourceRef}`
    pushCastFragments(0)
    return result
  }
  const prefix = `${selectPart} FROM (\n`
  const result = `${prefix}${upstream.sql}\n)`
  shiftFragmentsSince(ctx, fragStart, prefix.length)
  pushCastFragments(0)
  return result
}

// ── Coalesce ────────────────────────────────────────────────────────

export function buildCoalesceQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const coalesceCols = node.props.columns as Record<string, string>
  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)

  const schema = resolveNodeSchema(node.children[0], ctx.nodeIndex)
  if (!schema) {
    if (upstream.isSimple) return `SELECT * FROM ${upstream.sourceRef}`
    return `SELECT * FROM (\n${upstream.sql}\n)`
  }

  // Build projections while tracking per-column offsets for coalesced columns
  const colParts: { text: string; coalesced: boolean }[] = schema.map((c) => {
    const defaultExpr = coalesceCols[c.name]
    return defaultExpr
      ? {
          text: `COALESCE(${q(c.name)}, ${defaultExpr}) AS ${q(c.name)}`,
          coalesced: true,
        }
      : { text: q(c.name), coalesced: false }
  })

  const projections = colParts.map((p) => p.text).join(", ")
  const selectPart = `SELECT ${projections}`

  // Push a fragment for each coalesced column only
  const pushCoalesceFragments = (baseOffset: number): void => {
    let offset = baseOffset + "SELECT ".length
    for (let i = 0; i < colParts.length; i++) {
      if (colParts[i].coalesced) {
        pushFragment(ctx, offset, colParts[i].text.length, node)
      }
      offset += colParts[i].text.length
      if (i < colParts.length - 1) offset += 2 // ", "
    }
  }

  if (upstream.isSimple) {
    const result = `${selectPart} FROM ${upstream.sourceRef}`
    pushCoalesceFragments(0)
    return result
  }
  const prefix = `${selectPart} FROM (\n`
  const result = `${prefix}${upstream.sql}\n)`
  shiftFragmentsSince(ctx, fragStart, prefix.length)
  pushCoalesceFragments(0)
  return result
}

// ── AddField ────────────────────────────────────────────────────────

export function buildAddFieldQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const addCols = node.props.columns as Record<string, string>
  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)

  // Check for name collisions with upstream schema
  const schema = resolveNodeSchema(node.children[0], ctx.nodeIndex)
  if (schema) {
    const existingNames = new Set(schema.map((c) => c.name))
    for (const name of Object.keys(addCols)) {
      if (existingNames.has(name)) {
        throw new Error(
          `AddField name collision: field '${name}' already exists in the upstream schema`,
        )
      }
    }
  }

  const addedProjections = Object.entries(addCols)
    .map(([alias, expr]) => `${expr} AS ${q(alias)}`)
    .join(", ")

  const selectPart = `SELECT *, ${addedProjections}`
  // Fragment covers only the added fields (after "SELECT *, ")
  const addedOffset = "SELECT *, ".length
  const addedLength = addedProjections.length

  if (upstream.isSimple) {
    const result = `${selectPart} FROM ${upstream.sourceRef}`
    pushFragment(ctx, addedOffset, addedLength, node)
    return result
  }
  const prefix = `${selectPart} FROM (\n`
  const result = `${prefix}${upstream.sql}\n)`
  shiftFragmentsSince(ctx, fragStart, prefix.length)
  pushFragment(ctx, addedOffset, addedLength, node)
  return result
}

import type { ConstructNode } from "@/core/types.js"
import type { DmlEntry } from "./sql/sql-dml-types.js"
import { quoteIdentifier as q } from "./sql/sql-identifiers.js"
import { resolveSinkRef } from "./sql/sql-refs.js"
import type { BuildContext } from "./sql-build-context.js"
import { getUpstream } from "./sql-query-helpers.js"

/**
 * SideOutput and Validate — components that emit *two* INSERT
 * statements per pipeline: one to the main downstream sink and one to a
 * side path (filter rejects, validation failures). The pattern is the
 * same in both: walk the construct tree to find the side-path target,
 * inline the upstream as a CTE-or-subquery, and emit a parallel INSERT
 * with an inverted predicate (`WHERE NOT (...)` for SideOutput, the
 * negated rules for Validate plus a `_validation_error` CASE column).
 *
 * The `build*Query` functions emit only the *main path* SELECT (consumed
 * by the dispatcher when the sink walks the construct tree). The
 * `collect*Dml` functions emit the *side-path INSERT* and push it onto
 * the shared `DmlEntry[]` accumulator. Both halves are needed at the
 * same call site (the DML collector), which is why the file pairs them.
 */

// ── Side-path DML collector ──────────────────────────────────────────

/**
 * Recursively walk upstream nodes to find SideOutput/Validate nodes
 * that need to emit their side-path INSERT statements.
 */
export function collectSideDml(
  ctx: BuildContext,
  node: ConstructNode,
  entries: DmlEntry[],
): void {
  if (node.component === "SideOutput") {
    collectSideOutputDml(ctx, node, entries)
  } else if (node.component === "Validate") {
    collectValidateDml(ctx, node, entries)
  }

  // Continue recursing to find nested SideOutput/Validate
  for (const child of node.children) {
    collectSideDml(ctx, child, entries)
  }
}

// ── SideOutput ──────────────────────────────────────────────────────

export function buildSideOutputQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const condition = node.props.condition as string

  // Upstream is found from non-SideOutput.Sink children
  const upstreamChildren = node.children.filter(
    (c) => c.component !== "SideOutput.Sink",
  )
  const upstream =
    upstreamChildren.length > 0
      ? getUpstream(ctx, { children: upstreamChildren } as ConstructNode)
      : { sql: "SELECT * FROM unknown", sourceRef: "unknown", isSimple: false }

  if (upstream.isSimple) {
    return `SELECT * FROM ${upstream.sourceRef} WHERE NOT (${condition})`
  }
  return `SELECT * FROM (\n${upstream.sql}\n) WHERE NOT (${condition})`
}

export function collectSideOutputDml(
  ctx: BuildContext,
  sideOutputNode: ConstructNode,
  entries: DmlEntry[],
): void {
  const condition = sideOutputNode.props.condition as string
  const tag = sideOutputNode.props.tag as string | undefined

  // Find the SideOutput.Sink child and its sink
  const sideSinkWrapper = sideOutputNode.children.find(
    (c) => c.component === "SideOutput.Sink",
  )

  // Find upstream children (not SideOutput.Sink)
  const upstreamChildren = sideOutputNode.children.filter(
    (c) => c.component !== "SideOutput.Sink",
  )

  const upstream =
    upstreamChildren.length > 0
      ? getUpstream(ctx, { children: upstreamChildren } as ConstructNode)
      : { sql: "SELECT * FROM unknown", sourceRef: "unknown", isSimple: false }

  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  // Side path: matching records to the side sink
  if (sideSinkWrapper) {
    for (const child of sideSinkWrapper.children) {
      if (child.kind === "Sink") {
        const sinkRef = resolveSinkRef(child)
        const metaCols: string[] = []
        metaCols.push("CURRENT_TIMESTAMP AS `_side_ts`")
        if (tag) {
          metaCols.push(`'${tag}' AS \`_side_tag\``)
        }
        const selectList =
          metaCols.length > 0 ? `*, ${metaCols.join(", ")}` : "*"
        entries.push({
          sql: `INSERT INTO ${sinkRef}\nSELECT ${selectList} FROM ${fromClause} WHERE (${condition});`,
          contributors: [],
        })
      }
    }
  }

  // Main path continues — handled by the parent collectSinkDml
  // traversal finding the enclosing sink above the SideOutput.
  // We don't emit the main path INSERT here because the parent
  // sink will call buildQuery on SideOutput, which emits
  // SELECT ... WHERE NOT (condition).
}

// ── Validate ────────────────────────────────────────────────────────

export function buildValidateQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const rules = node.props.rules as {
    notNull?: readonly string[]
    range?: Record<string, [number, number]>
    expression?: Record<string, string>
  }

  // Upstream is found from non-Validate.Reject children
  const upstreamChildren = node.children.filter(
    (c) => c.component !== "Validate.Reject",
  )
  const upstream =
    upstreamChildren.length > 0
      ? getUpstream(ctx, { children: upstreamChildren } as ConstructNode)
      : { sql: "SELECT * FROM unknown", sourceRef: "unknown", isSimple: false }

  const validCondition = buildValidateCondition(rules)
  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  return `SELECT * FROM ${fromClause}\nWHERE ${validCondition}`
}

function buildValidateCondition(rules: {
  notNull?: readonly string[]
  range?: Record<string, [number, number]>
  expression?: Record<string, string>
}): string {
  const conditions: string[] = []

  if (rules.notNull) {
    for (const col of rules.notNull) {
      conditions.push(`${q(col)} IS NOT NULL`)
    }
  }

  if (rules.range) {
    for (const [col, [min, max]] of Object.entries(rules.range)) {
      conditions.push(`${q(col)} >= ${min} AND ${q(col)} <= ${max}`)
    }
  }

  if (rules.expression) {
    for (const expr of Object.values(rules.expression)) {
      conditions.push(expr)
    }
  }

  return conditions.join("\n    AND ")
}

function buildValidateErrorCase(rules: {
  notNull?: readonly string[]
  range?: Record<string, [number, number]>
  expression?: Record<string, string>
}): string {
  const cases: string[] = []

  if (rules.notNull) {
    for (const col of rules.notNull) {
      cases.push(`WHEN ${q(col)} IS NULL THEN 'notNull:${col}'`)
    }
  }

  if (rules.range) {
    for (const [col, [min, max]] of Object.entries(rules.range)) {
      cases.push(
        `WHEN ${q(col)} < ${min} OR ${q(col)} > ${max} THEN 'range:${col}[${min},${max}]'`,
      )
    }
  }

  if (rules.expression) {
    for (const [name, expr] of Object.entries(rules.expression)) {
      cases.push(`WHEN NOT (${expr}) THEN 'expression:${name}'`)
    }
  }

  return cases.map((c) => `      ${c}`).join("\n")
}

export function collectValidateDml(
  ctx: BuildContext,
  validateNode: ConstructNode,
  entries: DmlEntry[],
): void {
  const rules = validateNode.props.rules as {
    notNull?: readonly string[]
    range?: Record<string, [number, number]>
    expression?: Record<string, string>
  }

  // Find the Validate.Reject child and its sink
  const rejectWrapper = validateNode.children.find(
    (c) => c.component === "Validate.Reject",
  )

  // Find upstream children (not Validate.Reject)
  const upstreamChildren = validateNode.children.filter(
    (c) => c.component !== "Validate.Reject",
  )

  const upstream =
    upstreamChildren.length > 0
      ? getUpstream(ctx, { children: upstreamChildren } as ConstructNode)
      : { sql: "SELECT * FROM unknown", sourceRef: "unknown", isSimple: false }

  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`
  const validCondition = buildValidateCondition(rules)

  // Reject path: invalid records to the reject sink
  if (rejectWrapper) {
    for (const child of rejectWrapper.children) {
      if (child.kind === "Sink") {
        const sinkRef = resolveSinkRef(child)
        const errorCase = buildValidateErrorCase(rules)
        entries.push({
          sql: `INSERT INTO ${sinkRef}\nSELECT *,\n    CASE\n${errorCase}\n    END AS \`_validation_error\`,\n    CURRENT_TIMESTAMP AS \`_validated_at\`\nFROM ${fromClause}\nWHERE NOT (\n    ${validCondition}\n);`,
          contributors: [],
        })
      }
    }
  }

  // Valid path continues — handled by the parent collectSinkDml
  // traversal finding the enclosing sink above the Validate.
}

import { FlinkVersionCompat } from "@/core/flink-compat.js"
import type { ConstructNode } from "@/core/types.js"
import type { BuildContext } from "../sql-build-context.js"
import { quoteIdentifier as q } from "./sql-identifiers.js"

/**
 * `CREATE VIEW` and `CREATE MATERIALIZED TABLE` emission. Both wrap a
 * downstream query result with a top-level DDL statement; they're
 * grouped because both consume `ctx.buildQuery` to materialize the
 * `AS SELECT ...` body from the construct subtree.
 *
 * - `generateViewDdl` is the simple case: a logical view alias for an
 *   inline query.
 * - `generateMaterializedTableDdl` is Flink 2.x's first-class
 *   materialized-table primitive, with version-gated extras
 *   (FRESHNESS/REFRESH_MODE always; PARTITIONED BY only when set;
 *   DISTRIBUTED BY...HASH...INTO N BUCKETS requires Flink 2.2+).
 */

export function generateViewDdl(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const name = node.props.name as string

  // The view's children define the upstream query
  const upstream =
    node.children.length > 0
      ? ctx.buildQuery(ctx, node.children[0])
      : "SELECT * FROM unknown"

  return `CREATE VIEW ${q(name)} AS\n${upstream};`
}

export function generateMaterializedTableDdl(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const props = node.props
  const name = props.name as string
  const catalogName = props.catalogName as string
  const database = props.database as string | undefined

  // Version gate: require Flink >= 2.0
  const versionCheck = FlinkVersionCompat.checkFeature(
    "MATERIALIZED_TABLE",
    ctx.version,
  )
  if (versionCheck) {
    throw new Error(versionCheck.message)
  }

  // Freshness is required for Flink < 2.2
  if (
    !props.freshness &&
    !FlinkVersionCompat.isVersionAtLeast(ctx.version, "2.2")
  ) {
    throw new Error("MaterializedTable freshness is required for Flink < 2.2")
  }

  // Bucketing requires Flink >= 2.2
  if (props.bucketing) {
    const bucketCheck = FlinkVersionCompat.checkFeature(
      "MATERIALIZED_TABLE_BUCKETING",
      ctx.version,
    )
    if (bucketCheck) {
      throw new Error(bucketCheck.message)
    }
  }

  // Catalog-qualified table name
  const tableRef = database
    ? `${q(catalogName)}.${q(database)}.${q(name)}`
    : `${q(catalogName)}.${q(name)}`

  const parts: string[] = [`CREATE MATERIALIZED TABLE ${tableRef}`]

  // COMMENT clause
  if (props.comment) {
    parts.push(`  COMMENT '${String(props.comment)}'`)
  }

  // PARTITIONED BY clause
  const partitionedBy = props.partitionedBy as readonly string[] | undefined
  if (partitionedBy && partitionedBy.length > 0) {
    parts.push(`  PARTITIONED BY (${partitionedBy.map(q).join(", ")})`)
  }

  // DISTRIBUTED BY HASH ... INTO N BUCKETS (Flink 2.2+)
  if (props.bucketing) {
    const bucketing = props.bucketing as {
      columns: readonly string[]
      count: number
    }
    const bucketCols = bucketing.columns.map(q).join(", ")
    parts.push(
      `  DISTRIBUTED BY HASH(${bucketCols}) INTO ${bucketing.count} BUCKETS`,
    )
  }

  // WITH clause (table options)
  const withOpts = props.with as Record<string, string> | undefined
  if (withOpts && Object.keys(withOpts).length > 0) {
    const entries = Object.entries(withOpts)
      .map(([k, v]) => `    '${k}' = '${v}'`)
      .join(",\n")
    parts.push(`  WITH (\n${entries}\n  )`)
  }

  // FRESHNESS clause
  if (props.freshness) {
    parts.push(`  FRESHNESS = ${String(props.freshness)}`)
  }

  // REFRESH_MODE clause (omit for "automatic" — Flink auto-selects)
  const refreshMode = props.refreshMode as string | undefined
  if (refreshMode && refreshMode !== "automatic") {
    parts.push(`  REFRESH_MODE = ${refreshMode.toUpperCase()}`)
  }

  // AS SELECT (upstream query from children)
  const upstream =
    node.children.length > 0
      ? ctx.buildQuery(ctx, node.children[0])
      : "SELECT * FROM unknown"

  parts.push(`AS\n${upstream};`)

  return parts.join("\n")
}

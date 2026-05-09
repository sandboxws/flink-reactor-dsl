import type { SchemaDefinition } from "@/core/schema.js"
import type { ConstructNode } from "@/core/types.js"
import type { BuildContext } from "./sql-build-context.js"
import type { DmlEntry } from "./sql-dml-types.js"

/**
 * Per-statement metadata builders that surface in the dashboard's hover
 * tooltips. Pure: every helper takes a node (or computed entry) and returns
 * a structured `{ key, value }[]` describing what the statement does.
 *
 * `buildCommentBlock` formats the lines into the `-- ===... -- KEY : VALUE`
 * banner that prefaces every emitted statement. The banner is a comment in
 * the SQL output, not executable; downstream tooling re-parses it back into
 * the structured form.
 */

const RULER = `-- ${"=".repeat(68)}`
const SEP = `-- ${"-".repeat(68)}`

export function buildCommentBlock(
  type: string,
  entries: ReadonlyArray<{ key: string; value: string }>,
): string {
  const lines = [RULER, `-- ${type}`, SEP]
  for (const { key, value } of entries) {
    lines.push(`-- ${key.padEnd(12)}: ${value}`)
  }
  lines.push(RULER)
  return lines.join("\n")
}

export function connectorTypeFor(component: string): string {
  switch (component) {
    case "KafkaSource":
    case "KafkaSink":
      return "kafka"
    case "JdbcSource":
    case "JdbcSink":
      return "jdbc"
    case "DataGenSource":
      return "datagen"
    case "FileSystemSink":
      return "filesystem"
    case "IcebergSink":
      return "iceberg"
    case "PaimonSink":
      return "paimon"
    case "FlussSink":
    case "FlussSource":
      return "fluss"
    default:
      return component.toLowerCase().replace(/source$|sink$/i, "")
  }
}

function catalogTypeForComponent(component: string): string {
  switch (component) {
    case "PaimonCatalog":
      return "paimon"
    case "IcebergCatalog":
      return "iceberg"
    case "HiveCatalog":
      return "hive"
    case "JdbcCatalog":
      return "jdbc"
    case "GenericCatalog":
      return "generic"
    case "FlussCatalog":
      return "fluss"
    default:
      return component
  }
}

export function buildCatalogDetails(
  node: ConstructNode,
): Array<{ key: string; value: string }> {
  const props = node.props
  const details: Array<{ key: string; value: string }> = [
    { key: "id", value: node.id },
    { key: "type", value: catalogTypeForComponent(node.component) },
  ]
  if (props.defaultDatabase)
    details.push({ key: "database", value: String(props.defaultDatabase) })
  if (props.warehouse)
    details.push({ key: "warehouse", value: String(props.warehouse) })
  if (props.uri) details.push({ key: "uri", value: String(props.uri) })
  if (props.baseUrl)
    details.push({ key: "base-url", value: String(props.baseUrl) })
  if (props.hiveConfDir)
    details.push({ key: "hive-conf", value: String(props.hiveConfDir) })
  return details
}

export function buildSourceDetails(
  node: ConstructNode,
): Array<{ key: string; value: string }> {
  const props = node.props
  const details: Array<{ key: string; value: string }> = [
    { key: "id", value: node.id },
    { key: "type", value: connectorTypeFor(node.component) },
  ]

  switch (node.component) {
    case "KafkaSource":
      details.push({ key: "topic", value: (props.topic as string) ?? "" })
      details.push({ key: "format", value: (props.format as string) ?? "json" })
      if (props.bootstrapServers)
        details.push({
          key: "bootstrap",
          value: String(props.bootstrapServers),
        })
      details.push({
        key: "startup",
        value: (props.startupMode as string) ?? "earliest-offset",
      })
      if (props.consumerGroup)
        details.push({ key: "group-id", value: String(props.consumerGroup) })
      break
    case "JdbcSource":
      details.push({ key: "table", value: (props.table as string) ?? "" })
      details.push({ key: "url", value: (props.url as string) ?? "" })
      break
    case "FlussSource":
      if (props.catalogName)
        details.push({ key: "catalog", value: String(props.catalogName) })
      details.push({ key: "database", value: (props.database as string) ?? "" })
      details.push({ key: "table", value: (props.table as string) ?? "" })
      details.push({
        key: "startup",
        value: (props.scanStartupMode as string) ?? "initial",
      })
      if (props.scanStartupTimestampMs != null)
        details.push({
          key: "ts-ms",
          value: String(props.scanStartupTimestampMs),
        })
      details.push({
        key: "table-type",
        value: props.primaryKey ? "PrimaryKey (retract)" : "Log (append-only)",
      })
      break
    default: {
      const connector = (props.connector as string) ?? node.component
      details[1] = { key: "type", value: connector }
      if (props.format)
        details.push({ key: "format", value: String(props.format) })
      if (props.options) {
        for (const [k, v] of Object.entries(
          props.options as Record<string, string>,
        )) {
          details.push({ key: k, value: v })
        }
      }
      break
    }
  }

  return details
}

export function buildSourceSchema(
  node: ConstructNode,
): Array<{ name: string; type: string }> | undefined {
  const schemaDef = node.props.schema as SchemaDefinition | undefined
  if (!schemaDef) return undefined
  return Object.entries(schemaDef.fields).map(([name, type]) => ({
    name,
    type: String(type),
  }))
}

export function buildSinkDetails(
  node: ConstructNode,
): Array<{ key: string; value: string }> {
  const props = node.props
  const cType = connectorTypeFor(node.component)
  const details: Array<{ key: string; value: string }> = [
    { key: "id", value: node.id },
    { key: "type", value: cType },
  ]

  switch (node.component) {
    case "KafkaSink":
      details.push({ key: "topic", value: (props.topic as string) ?? "" })
      details.push({ key: "format", value: (props.format as string) ?? "json" })
      if (props.bootstrapServers)
        details.push({
          key: "bootstrap",
          value: String(props.bootstrapServers),
        })
      break
    case "JdbcSink":
      details.push({ key: "table", value: (props.table as string) ?? "" })
      details.push({ key: "url", value: (props.url as string) ?? "" })
      break
    case "FileSystemSink":
      details.push({ key: "path", value: (props.path as string) ?? "" })
      if (props.format)
        details.push({ key: "format", value: String(props.format) })
      break
    case "GenericSink": {
      const connector = (props.connector as string) ?? "generic"
      details[1] = { key: "type", value: connector }
      if (connector === "print" || connector === "blackhole")
        details.push({ key: "purpose", value: "debug / development sink" })
      if (props.options) {
        for (const [k, v] of Object.entries(
          props.options as Record<string, string>,
        )) {
          details.push({ key: k, value: v })
        }
      }
      break
    }
    case "FlussSink":
      if (props.catalogName)
        details.push({ key: "catalog", value: String(props.catalogName) })
      details.push({ key: "database", value: (props.database as string) ?? "" })
      details.push({ key: "table", value: (props.table as string) ?? "" })
      if (props.buckets != null)
        details.push({ key: "buckets", value: String(props.buckets) })
      if (props.primaryKey)
        details.push({
          key: "table-type",
          value: "PrimaryKey (upsert)",
        })
      else
        details.push({
          key: "table-type",
          value: "Log (append-only)",
        })
      break
    default:
      break
  }

  return details
}

export function getTransformDetail(
  node: ConstructNode,
): { key: string; value: string } | null {
  switch (node.component) {
    case "Filter":
      return node.props.condition
        ? { key: "rule", value: String(node.props.condition) }
        : null
    case "Aggregate":
      return node.props.groupBy
        ? {
            key: "group-by",
            value: Array.isArray(node.props.groupBy)
              ? (node.props.groupBy as string[]).join(", ")
              : String(node.props.groupBy),
          }
        : null
    case "TopN":
      return node.props.n ? { key: "top-n", value: String(node.props.n) } : null
    case "Deduplicate":
      return node.props.orderBy
        ? { key: "order-by", value: String(node.props.orderBy) }
        : null
    default:
      return null
  }
}

/**
 * Per-DML metadata builder for the dashboard's hover tooltip on
 * `INSERT INTO ... SELECT ...` statements.
 *
 * Lifts the sink name from the INSERT target, gathers contributors from
 * the entry's fragment list (sources directly; transforms via the
 * `nodeIndex`), and renders a `{ step, type, input, output, rule }`-
 * shaped detail block. For multi-transform chains it switches to a
 * `transforms: A → B → C` summary line. Falls back to parsing the FROM
 * clause when contributors are absent (defensive — production paths
 * should always populate fragments).
 */
export function buildDmlDetails(
  ctx: BuildContext,
  entry: DmlEntry,
): Array<{ key: string; value: string }> {
  const details: Array<{ key: string; value: string }> = []

  // Extract sink name from INSERT INTO `name`
  const sinkMatch = entry.sql.match(/INSERT INTO `([^`]+)`/)
  const sinkName = sinkMatch?.[1] ?? "unknown"

  // Collect unique sources and transforms from contributors
  const sourceIds: string[] = []
  const transformNodes: ConstructNode[] = []
  const seen = new Set<string>()
  for (const c of entry.contributors) {
    if (seen.has(c.origin.nodeId)) continue
    seen.add(c.origin.nodeId)
    if (c.origin.kind === "Source") sourceIds.push(c.origin.nodeId)
    if (c.origin.kind === "Transform") {
      const n = ctx.nodeIndex.get(c.origin.nodeId)
      if (n) transformNodes.push(n)
    }
  }

  // If no contributors, try parsing FROM clause
  if (sourceIds.length === 0) {
    const fromMatch = entry.sql.match(/FROM `([^`]+)`/)
    if (fromMatch) sourceIds.push(fromMatch[1])
  }

  // Build details based on transform chain
  if (transformNodes.length === 1) {
    const t = transformNodes[0]
    details.push({ key: "step", value: t.id })
    details.push({ key: "type", value: t.component })
    details.push({ key: "input", value: sourceIds.join(", ") || "unknown" })
    details.push({ key: "output", value: sinkName })
    const detail = getTransformDetail(t)
    if (detail) details.push(detail)
  } else if (transformNodes.length > 1) {
    details.push({
      key: "transforms",
      value: transformNodes.map((t) => t.component).join(" → "),
    })
    details.push({ key: "input", value: sourceIds.join(", ") || "unknown" })
    details.push({ key: "output", value: sinkName })
  } else {
    // No transforms — direct source to sink
    details.push({ key: "input", value: sourceIds.join(", ") || "unknown" })
    details.push({ key: "output", value: sinkName })
  }

  return details
}

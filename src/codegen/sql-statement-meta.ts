import type { SchemaDefinition } from "@/core/schema.js"
import type { ConstructNode } from "@/core/types.js"

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

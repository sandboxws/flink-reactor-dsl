import type {
  CatalogMeta,
  ConnectorMeta,
  ConstructNode,
  PipelineManifest,
} from "./types.js"

// ── Connector type resolution ────────────────────────────────────────
// Inlined from sql-generator.ts to avoid circular imports
// (sql-generator imports from core/tap, so core cannot import back).

function connectorTypeFor(component: string): string {
  switch (component) {
    case "KafkaSource":
    case "KafkaSink":
      return "kafka"
    case "JdbcSource":
    case "JdbcSink":
      return "jdbc"
    case "FileSystemSink":
      return "filesystem"
    case "IcebergSink":
      return "iceberg"
    case "PaimonSink":
      return "paimon"
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
    default:
      return component
  }
}

// ── Connector property extraction ────────────────────────────────────
// Mirrors extractConnectorProperties from tap.ts for manifest use.

function extractConnectorProperties(
  node: ConstructNode,
): Record<string, string> {
  const props = node.props
  const result: Record<string, string> = {}

  switch (node.component) {
    case "KafkaSource":
      result.connector = "kafka"
      result.topic = props.topic as string
      result.format = (props.format as string) ?? "json"
      if (props.bootstrapServers) {
        result["properties.bootstrap.servers"] =
          props.bootstrapServers as string
      }
      if (props.consumerGroup) {
        result["properties.group.id"] = props.consumerGroup as string
      }
      break

    case "KafkaSink":
      result.connector = "kafka"
      result.topic = props.topic as string
      result.format = (props.format as string) ?? "json"
      if (props.bootstrapServers) {
        result["properties.bootstrap.servers"] =
          props.bootstrapServers as string
      }
      break

    case "JdbcSource":
      result.connector = "jdbc"
      result.url = props.url as string
      result["table-name"] = props.table as string
      break

    case "JdbcSink":
      result.connector = "jdbc"
      result.url = props.url as string
      result["table-name"] = props.table as string
      break

    case "GenericSource":
    case "GenericSink":
      result.connector = props.connector as string
      if (props.format) result.format = props.format as string
      if (props.options) {
        Object.assign(result, props.options as Record<string, string>)
      }
      break

    case "FileSystemSink":
      result.connector = "filesystem"
      result.path = props.path as string
      if (props.format) result.format = props.format as string
      break

    case "PaimonSink":
      result.connector = "paimon"
      break

    case "IcebergSink":
      result.connector = "iceberg"
      break
  }

  return result
}

// ── Schema extraction ────────────────────────────────────────────────

function extractSchemaColumns(
  node: ConstructNode,
): { name: string; type: string }[] | null {
  const schema = node.props.schema as
    | { fields: Record<string, string> }
    | undefined
  if (schema?.fields) {
    return Object.entries(schema.fields).map(([name, type]) => ({
      name,
      type,
    }))
  }
  return null
}

// ── Resource identifier extraction ───────────────────────────────────

function extractResource(
  node: ConstructNode,
  connectorType: string,
  connectorProps: Record<string, string>,
): string {
  const props = node.props

  switch (connectorType) {
    case "kafka":
    case "upsert-kafka":
      return (props.topic as string) ?? connectorProps.topic ?? ""

    case "iceberg":
    case "paimon": {
      const catalog = (props.catalog as string) ?? ""
      const database = (props.database as string) ?? ""
      const table = (props.table as string) ?? ""
      if (catalog || database || table) {
        return [catalog, database, table].filter(Boolean).join(".")
      }
      return ""
    }

    case "jdbc":
      return (props.table as string) ?? connectorProps["table-name"] ?? ""

    case "filesystem":
      return (props.path as string) ?? connectorProps.path ?? ""

    default:
      // Best effort: try common property names
      return (
        (props.topic as string) ??
        (props.table as string) ??
        (props.path as string) ??
        ""
      )
  }
}

// ── Main manifest generation ─────────────────────────────────────────

/**
 * Walk the construct tree and generate a PipelineManifest containing
 * metadata for all sources, sinks, and catalogs in the pipeline.
 */
export function generatePipelineManifest(
  tree: ConstructNode,
): PipelineManifest {
  const pipelineName = (tree.props.name as string) ?? tree.id
  const sources: ConnectorMeta[] = []
  const sinks: ConnectorMeta[] = []
  const catalogs: CatalogMeta[] = []

  function walk(node: ConstructNode): void {
    if (node.kind === "Source" || node.kind === "Sink") {
      const connectorType = connectorTypeFor(node.component)
      const connectorProps = extractConnectorProperties(node)
      const resource = extractResource(node, connectorType, connectorProps)
      const schema = extractSchemaColumns(node)
      const role = node.kind === "Source" ? "source" : "sink"

      const meta: ConnectorMeta = {
        nodeId: node.id,
        componentName: node.component,
        connectorType,
        role,
        resource,
        connectorProperties: connectorProps,
        schema,
      }

      if (role === "source") {
        sources.push(meta)
      } else {
        sinks.push(meta)
      }
    }

    if (node.kind === "Catalog") {
      const catalogType = catalogTypeForComponent(node.component)
      const catalogName =
        (node.props.name as string) ?? (node.props.catalogName as string) ?? ""

      const catalogMeta: CatalogMeta = {
        catalogName,
        catalogType,
        ...(node.props.uri ? { uri: node.props.uri as string } : {}),
        ...(node.props.warehouse
          ? { warehouse: node.props.warehouse as string }
          : {}),
      }

      catalogs.push(catalogMeta)
    }

    // Recurse into children
    for (const child of node.children) {
      walk(child)
    }
  }

  walk(tree)

  return {
    pipelineName,
    sources,
    sinks,
    catalogs,
    generatedAt: new Date().toISOString(),
  }
}

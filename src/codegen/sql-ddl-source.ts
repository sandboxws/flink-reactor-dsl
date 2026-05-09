import type { SchemaDefinition } from "@/core/schema.js"
import type { ConstructNode } from "@/core/types.js"
import { quoteIdentifier as q } from "./sql/sql-identifiers.js"

/**
 * Source-side CREATE TABLE emission. Splits into three regular shapes
 * and one catalog-managed special case (Apache Fluss 0.9+).
 *
 * Pure helpers — no synthesis state required. Schema columns,
 * constraints, and connector-specific WITH clauses are all derived from
 * the node's props.
 *
 * The Fluss catalog-managed path emits a `CREATE DATABASE / CREATE TABLE
 * IF NOT EXISTS / CREATE TEMPORARY VIEW` triple instead of the standard
 * `CREATE TABLE … WITH ('connector'='fluss', …)` form. Apache Fluss
 * 0.9.0-incubating registers only `FlinkCatalogFactory` (not a
 * `DynamicTableFactory`), so the standalone form fails at deploy time
 * with `Cannot discover a connector using option: 'connector'='fluss'`.
 */

export function generateSourceDdl(node: ConstructNode): string | null {
  if (node.component === "CatalogSource") return null

  // FlussSource is catalog-managed. See file header for rationale.
  if (node.component === "FlussSource" && node.props.catalogName) {
    return generateCatalogManagedSourceDdl(node)
  }

  const props = node.props
  const schema = props.schema as SchemaDefinition
  const tableName = node.id

  const columns = generateColumns(schema)
  const constraints = generateConstraints(schema, props)
  const withClause = generateSourceWithClause(node)

  const parts = [`CREATE TABLE ${q(tableName)} (`, columns]

  if (constraints.length > 0) {
    parts[parts.length - 1] += ","
    parts.push(constraints)
  }

  parts.push(`) WITH (\n${withClause}\n);`)

  return parts.join("\n")
}

function generateCatalogManagedSourceDdl(node: ConstructNode): string {
  const props = node.props
  const tableName = node.id
  const catalogName = String(props.catalogName)
  const database = String(props.database)
  const table = String(props.table)
  const fullName = `${q(catalogName)}.${q(database)}.${q(table)}`
  const schema = props.schema as SchemaDefinition | undefined

  // Self-bootstrap the upstream catalog table so the consumer-side view
  // below resolves regardless of whether a producer pipeline (e.g. Flink
  // CDC ingest) has already materialized it. `IF NOT EXISTS` makes us a
  // no-op when the producer wins the race; when we win, downstream
  // producers still write into the table we created.
  //
  // Watermarks are stripped from the catalog body — they're query-level,
  // not storage-level, and Fluss/Paimon catalogs reject them at storage
  // declaration time. The consumer-side TEMPORARY TABLE below re-applies
  // the watermark on top of a SELECT against the catalog.
  const stmts: string[] = []
  if (schema && Object.keys(schema.fields).length > 0) {
    const storageSchema: SchemaDefinition = { ...schema, watermark: undefined }
    const columns = generateColumns(storageSchema)
    const constraints = generateConstraints(storageSchema, props)
    const tableBody =
      constraints.length > 0 ? `${columns},\n${constraints}` : columns
    stmts.push(
      `CREATE DATABASE IF NOT EXISTS ${q(catalogName)}.${q(database)};`,
      `CREATE TABLE IF NOT EXISTS ${fullName} (\n${tableBody}\n);`,
    )
  }

  // A TEMPORARY VIEW re-exposes the catalog table into default_catalog
  // under a short alias usable in downstream FROM clauses. `LIKE` would
  // copy columns but leave the WITH clause empty, and the resulting
  // alias fails planner-time connector discovery for catalog-only
  // factories (Fluss 0.9, Paimon).
  //
  // Watermarks declared on the FlussSource schema are intentionally
  // dropped here — they're query-level metadata that Fluss's storage
  // catalog rejects, and views can't carry WATERMARK clauses. Pipelines
  // that need event-time semantics on a Fluss source should declare the
  // watermark on the producer-side CREATE TABLE (i.e. via the FlussSink
  // that materializes the table).
  stmts.push(
    `CREATE TEMPORARY VIEW ${q(tableName)} AS SELECT * FROM ${fullName};`,
  )

  return stmts.join("\n")
}

export function generateColumns(schema: SchemaDefinition): string {
  const lines: string[] = []

  for (const [name, type] of Object.entries(schema.fields)) {
    lines.push(`  ${q(name)} ${type}`)
  }

  for (const meta of schema.metadataColumns) {
    let line = `  ${q(meta.column)} ${meta.type} METADATA`
    if (meta.from) line += ` FROM '${meta.from}'`
    if (meta.isVirtual) line += " VIRTUAL"
    lines.push(line)
  }

  if (schema.watermark) {
    lines.push(
      `  WATERMARK FOR ${q(schema.watermark.column)} AS ${schema.watermark.expression}`,
    )
  }

  return lines.join(",\n")
}

export function generateConstraints(
  schema: SchemaDefinition,
  props: Record<string, unknown>,
): string {
  const parts: string[] = []

  if (schema.primaryKey) {
    const cols = schema.primaryKey.columns.map(q).join(", ")
    parts.push(`  PRIMARY KEY (${cols}) NOT ENFORCED`)
  }

  if (!schema.primaryKey && props.primaryKey) {
    const cols = (props.primaryKey as readonly string[]).map(q).join(", ")
    parts.push(`  PRIMARY KEY (${cols}) NOT ENFORCED`)
  }

  return parts.join(",\n")
}

function generateSourceWithClause(node: ConstructNode): string {
  const props = node.props
  const withProps: Record<string, string> = {}

  switch (node.component) {
    case "KafkaSource": {
      const schema = props.schema as SchemaDefinition | undefined
      const hasPk =
        (schema?.primaryKey?.columns?.length ?? 0) > 0 ||
        ((props.primaryKey as readonly string[] | undefined)?.length ?? 0) > 0
      const rawFormat = (props.format as string) ?? "json"
      // Changelog formats (debezium-json, canal-json) support PK on regular kafka connector.
      // Plain formats (json, avro, csv) need upsert-kafka for PK support.
      const isChangelogFormat =
        rawFormat === "debezium-json" ||
        rawFormat === "canal-json" ||
        rawFormat === "debezium-avro" ||
        rawFormat === "debezium-protobuf" ||
        rawFormat === "maxwell-json"
      if (hasPk && !isChangelogFormat) {
        withProps.connector = "upsert-kafka"
        withProps.topic = props.topic as string
        withProps["key.format"] = "json"
        withProps["value.format"] = rawFormat
      } else {
        withProps.connector = "kafka"
        withProps.topic = props.topic as string
        withProps.format = rawFormat
      }
      // Registry-backed CDC formats require a Schema Registry endpoint.
      // The prop is enforced upstream by validation; emit when present.
      if (
        (rawFormat === "debezium-avro" || rawFormat === "debezium-protobuf") &&
        props.schemaRegistryUrl
      ) {
        withProps[`${rawFormat}.schema-registry.url`] =
          props.schemaRegistryUrl as string
      }
      if (props.bootstrapServers) {
        withProps["properties.bootstrap.servers"] =
          props.bootstrapServers as string
      }
      // upsert-kafka doesn't support scan.startup.mode
      const isUpsertKafka = hasPk && !isChangelogFormat
      if (!isUpsertKafka) {
        withProps["scan.startup.mode"] =
          (props.startupMode as string) ?? "earliest-offset"
      }
      if (props.consumerGroup) {
        withProps["properties.group.id"] = props.consumerGroup as string
      }
      break
    }
    case "JdbcSource":
      withProps.connector = "jdbc"
      withProps.url = props.url as string
      withProps["table-name"] = props.table as string
      if (props.lookupCache) {
        const cache = props.lookupCache as { maxRows: number; ttl: string }
        withProps["lookup.cache.max-rows"] = String(cache.maxRows)
        withProps["lookup.cache.ttl"] = cache.ttl
      }
      break
    case "GenericSource":
    case "DataGenSource":
      withProps.connector = props.connector as string
      if (props.format) withProps.format = props.format as string
      if (props.options) {
        Object.assign(withProps, props.options as Record<string, string>)
      }
      break
    case "FlussSource": {
      withProps.connector = "fluss"
      // The catalog handle stores `catalogName` on the source — surface it
      // verbatim so the runtime can route the connector to the right Fluss
      // server when the catalog DDL has already registered bootstrap.servers
      // there.
      withProps.catalog = String(props.catalogName)
      withProps.database = props.database as string
      withProps.table = props.table as string
      withProps["scan.startup.mode"] =
        (props.scanStartupMode as string) ?? "initial"
      if (
        props.scanStartupMode === "timestamp" &&
        props.scanStartupTimestampMs != null
      ) {
        withProps["scan.startup.timestamp"] = String(
          props.scanStartupTimestampMs,
        )
      }
      break
    }
  }

  return Object.entries(withProps)
    .map(([k, v]) => `  '${k}' = '${v}'`)
    .join(",\n")
}

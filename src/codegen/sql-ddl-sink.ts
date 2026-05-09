import type { ConstructNode } from "@/core/types.js"
import { toInsertOnlyFormat } from "./sql/sql-duration.js"
import { quoteIdentifier as q } from "./sql/sql-identifiers.js"
import type { SinkMetadata } from "./sql-sink-metadata.js"

/**
 * Sink-side CREATE TABLE emission. Three regular shapes plus one
 * catalog-managed special case:
 *
 *   - Standard sinks (`KafkaSink`, `JdbcSink`, `FileSystemSink`,
 *     `GenericSink`) emit a single `CREATE TABLE … WITH ('connector'=…)`.
 *   - upsert-kafka shape (`KafkaSink` with retract changelog + PK):
 *     `CREATE TABLE … PRIMARY KEY (…) NOT ENFORCED WITH (...)` and the
 *     connector swaps to `upsert-kafka` with split key/value formats.
 *   - JDBC upsert shape: same PRIMARY KEY clause emitted but the connector
 *     stays `jdbc`; the planner picks upsert mode based on the PK.
 *   - Catalog-managed (`IcebergSink`, `PaimonSink`, `FlussSink`):
 *     emits `CREATE DATABASE IF NOT EXISTS / CREATE TABLE IF NOT EXISTS`
 *     under the catalog's namespace, since these connectors register only
 *     a `FlinkCatalogFactory` (not a `DynamicTableFactory`) and the
 *     standalone `WITH ('connector'='paimon'…)` form fails at deploy time.
 *
 * `resolvePartitionExpression` is intentionally exported — both the
 * sink-side DDL (for physical column declarations) and the DML insert
 * wrapping (for projecting computed partition columns) need it.
 */

export function generateSinkDdl(
  node: ConstructNode,
  metadata?: SinkMetadata,
  pipelineBootstrap?: string,
): string {
  const props = node.props
  const tableName = node.id

  if (props.catalogName) {
    return generateCatalogManagedSinkDdl(node, metadata)
  }

  const withClause = generateSinkWithClause(node, metadata, pipelineBootstrap)
  const parts: string[] = []

  // Build column definitions from resolved schema
  const columnDefs = metadata?.schema
    ? metadata.schema.map((c) => `  ${q(c.name)} ${c.type}`).join(",\n")
    : null

  // Determine if we need a PRIMARY KEY (for upsert-kafka sinks)
  const needsUpsertPk =
    metadata?.changelogMode === "retract" &&
    node.component === "KafkaSink" &&
    metadata.primaryKey &&
    metadata.primaryKey.length > 0

  // JdbcSink needs PK for upsert: explicit keyFields or auto-propagated from retract mode
  const jdbcNeedsPk =
    node.component === "JdbcSink" &&
    ((props.upsertMode && props.keyFields) ||
      (metadata?.changelogMode === "retract" &&
        metadata.primaryKey &&
        metadata.primaryKey.length > 0))

  if (jdbcNeedsPk) {
    const cols = props.keyFields
      ? (props.keyFields as readonly string[]).map(q).join(", ")
      : (metadata?.primaryKey ?? []).map(q).join(", ")
    if (columnDefs) {
      parts.push(
        `CREATE TABLE ${q(tableName)} (`,
        `${columnDefs},`,
        `  PRIMARY KEY (${cols}) NOT ENFORCED`,
        `) WITH (\n${withClause}\n);`,
      )
    } else {
      parts.push(
        `CREATE TABLE ${q(tableName)} (`,
        `  PRIMARY KEY (${cols}) NOT ENFORCED`,
        `) WITH (\n${withClause}\n);`,
      )
    }
  } else if (node.component === "FileSystemSink" && props.partitionBy) {
    const rawPartCols = props.partitionBy as readonly string[]
    // Flink PARTITIONED BY only accepts physical column references.
    // When partitionBy contains function calls (e.g. DATE(col)), generate
    // physical partition columns and reference those names instead.
    const physicalPartCols: string[] = []
    const partColRefs: string[] = []
    for (const expr of rawPartCols) {
      const funcMatch = expr.match(/^(\w+)\((\w+)\)$/i)
      if (funcMatch) {
        const [, func, col] = funcMatch
        const resolved = resolvePartitionExpression(func, col)
        physicalPartCols.push(`  ${q(resolved.name)} ${resolved.type}`)
        partColRefs.push(q(resolved.name))
      } else {
        partColRefs.push(q(expr))
      }
    }
    const partClause = partColRefs.join(", ")
    const allColumnDefs = columnDefs
      ? physicalPartCols.length > 0
        ? `${columnDefs},\n${physicalPartCols.join(",\n")}`
        : columnDefs
      : physicalPartCols.length > 0
        ? physicalPartCols.join(",\n")
        : null
    if (allColumnDefs) {
      parts.push(
        `CREATE TABLE ${q(tableName)} (`,
        allColumnDefs,
        `) PARTITIONED BY (${partClause}) WITH (\n${withClause}\n);`,
      )
    } else {
      parts.push(
        `CREATE TABLE ${q(tableName)} PARTITIONED BY (${partClause}) WITH (\n${withClause}\n);`,
      )
    }
  } else if (needsUpsertPk && columnDefs) {
    const pkCols = metadata?.primaryKey?.map(q).join(", ")
    parts.push(
      `CREATE TABLE ${q(tableName)} (`,
      `${columnDefs},`,
      `  PRIMARY KEY (${pkCols}) NOT ENFORCED`,
      `) WITH (\n${withClause}\n);`,
    )
  } else if (columnDefs) {
    parts.push(
      `CREATE TABLE ${q(tableName)} (`,
      columnDefs,
      `) WITH (\n${withClause}\n);`,
    )
  } else {
    parts.push(`CREATE TABLE ${q(tableName)} WITH (\n${withClause}\n);`)
  }

  return parts.join("\n")
}

/**
 * Emit DDL for a catalog-managed sink (IcebergSink, PaimonSink, FlussSink).
 *
 * Produces idempotent CREATE DATABASE + CREATE TABLE statements scoped to the
 * sink's catalog. Without these, fresh REST catalogs have no databases/tables
 * for INSERT to target. IF NOT EXISTS makes the emission safe across
 * cluster restarts and across pipelines that share a database.
 */
function generateCatalogManagedSinkDdl(
  node: ConstructNode,
  metadata?: SinkMetadata,
): string {
  const props = node.props
  const catalogName = String(props.catalogName)
  const database = String(props.database)
  const table = String(props.table)
  const fullName = `${q(catalogName)}.${q(database)}.${q(table)}`
  const dbName = `${q(catalogName)}.${q(database)}`

  const stmts: string[] = [`CREATE DATABASE IF NOT EXISTS ${dbName};`]

  if (!metadata?.schema) {
    return stmts.join("\n")
  }

  const colDefs = metadata.schema
    .map((c) => `  ${q(c.name)} ${c.type}`)
    .join(",\n")

  const pk = props.primaryKey as readonly string[] | undefined
  const inner: string[] = [colDefs]
  if (pk && pk.length > 0) {
    inner.push(`  PRIMARY KEY (${pk.map(q).join(", ")}) NOT ENFORCED`)
  }

  const tableProps = collectCatalogManagedTableProps(node)
  const withClause =
    tableProps.length > 0
      ? ` WITH (\n${tableProps.map(([k, v]) => `  '${k}' = '${v}'`).join(",\n")}\n)`
      : ""

  stmts.push(
    `CREATE TABLE IF NOT EXISTS ${fullName} (\n${inner.join(",\n")}\n)${withClause};`,
  )

  return stmts.join("\n")
}

function collectCatalogManagedTableProps(
  node: ConstructNode,
): [string, string][] {
  if (node.component === "IcebergSink") return collectIcebergTableProps(node)
  if (node.component === "PaimonSink") return collectPaimonTableProps(node)
  if (node.component === "FlussSink") return collectFlussTableProps(node)
  return []
}

function collectPaimonTableProps(node: ConstructNode): [string, string][] {
  if (node.component !== "PaimonSink") return []
  const props = node.props
  const out: [string, string][] = []

  if (props.mergeEngine) {
    out.push(["merge-engine", String(props.mergeEngine)])
  }
  if (props.changelogProducer) {
    out.push(["changelog-producer", String(props.changelogProducer)])
  }
  if (props.sequenceField) {
    out.push(["sequence.field", String(props.sequenceField)])
  }
  if (props.bucket != null) {
    out.push(["bucket", String(props.bucket)])
  }
  // bucket-key:
  //   - explicit value wins
  //   - else fall back to primaryKey only when the user is actually tuning
  //     bucketing (bucket prop set) — emitting bucket-key is meaningless
  //     unless bucket is also set, and the "omitted CDC props" contract says
  //     to leave the WITH clause empty otherwise
  const explicitBucketKey = props.bucketKey as readonly string[] | undefined
  const primaryKey = props.primaryKey as readonly string[] | undefined
  const bucketKey =
    Array.isArray(explicitBucketKey) && explicitBucketKey.length > 0
      ? explicitBucketKey
      : props.bucket != null &&
          Array.isArray(primaryKey) &&
          primaryKey.length > 0
        ? primaryKey
        : undefined
  if (bucketKey) {
    out.push(["bucket-key", bucketKey.join(",")])
  }
  if (props.fullCompactionDeltaCommits != null) {
    out.push([
      "full-compaction.delta-commits",
      String(props.fullCompactionDeltaCommits),
    ])
  }
  if (props.writeBufferSizeMB != null) {
    out.push([
      "write-buffer-size",
      String(Number(props.writeBufferSizeMB) * 1048576),
    ])
  }
  if (props.snapshotNumRetainedMin != null) {
    out.push([
      "snapshot.num-retained.min",
      String(props.snapshotNumRetainedMin),
    ])
  }
  if (props.snapshotNumRetainedMax != null) {
    out.push([
      "snapshot.num-retained.max",
      String(props.snapshotNumRetainedMax),
    ])
  }

  return out
}

function collectFlussTableProps(node: ConstructNode): [string, string][] {
  const props = node.props
  const out: [string, string][] = []
  if (props.buckets != null) {
    out.push(["bucket.num", String(props.buckets)])
  }
  return out
}

function collectIcebergTableProps(node: ConstructNode): [string, string][] {
  if (node.component !== "IcebergSink") return []
  const props = node.props
  const out: [string, string][] = []

  if (props.formatVersion) {
    out.push(["format-version", String(props.formatVersion)])
  }
  if (props.upsertEnabled) {
    out.push(["write.upsert.enabled", "true"])
  }
  const equalityCols =
    (props.equalityFieldColumns as readonly string[] | undefined) ??
    (props.upsertEnabled
      ? (props.primaryKey as readonly string[] | undefined)
      : undefined)
  if (equalityCols && equalityCols.length > 0) {
    out.push(["equality-field-columns", equalityCols.join(",")])
  }
  if (props.commitIntervalSeconds) {
    out.push([
      "commit-interval-ms",
      String(Number(props.commitIntervalSeconds) * 1000),
    ])
  }
  if (props.writeDistributionMode) {
    out.push(["write.distribution-mode", String(props.writeDistributionMode)])
  }
  if (props.targetFileSizeMB) {
    out.push([
      "write.target-file-size-bytes",
      String(Number(props.targetFileSizeMB) * 1048576),
    ])
  }
  if (props.writeParquetCompression) {
    out.push([
      "write.parquet.compression-codec",
      String(props.writeParquetCompression),
    ])
  }

  return out
}

/**
 * Map a partition function expression to a physical column and SQL expression.
 * Flink PARTITIONED BY requires physical columns, not computed columns.
 * E.g. DATE(event_time) → { name: "dt", type: "DATE", expr: "CAST(`event_time` AS DATE)" }
 *
 * Exported because the DML side needs the same mapping when it wraps
 * `INSERT INTO sink … SELECT *, <expr> AS <col>` for FileSystemSink
 * partitions; the DDL emits the column declaration, the DML projects it.
 */
export function resolvePartitionExpression(
  func: string,
  col: string,
): { name: string; type: string; expr: string } {
  const upperFunc = func.toUpperCase()
  switch (upperFunc) {
    case "DATE":
      return {
        name: "dt",
        type: "DATE",
        expr: `CAST(${q(col)} AS DATE)`,
      }
    case "HOUR":
      return {
        name: "hr",
        type: "BIGINT",
        expr: `HOUR(CAST(${q(col)} AS TIMESTAMP))`,
      }
    case "YEAR":
      return {
        name: "yr",
        type: "BIGINT",
        expr: `YEAR(CAST(${q(col)} AS TIMESTAMP))`,
      }
    case "MONTH":
      return {
        name: "mo",
        type: "BIGINT",
        expr: `MONTH(CAST(${q(col)} AS TIMESTAMP))`,
      }
    case "DAY":
      return {
        name: "dy",
        type: "BIGINT",
        expr: `DAY(CAST(${q(col)} AS TIMESTAMP))`,
      }
    default: {
      const name = `_p_${func.toLowerCase()}`
      return {
        name,
        type: "STRING",
        expr: `${upperFunc}(${q(col)})`,
      }
    }
  }
}

function generateSinkWithClause(
  node: ConstructNode,
  metadata?: SinkMetadata,
  pipelineBootstrap?: string,
): string {
  const props = node.props
  const withProps: Record<string, string> = {}

  switch (node.component) {
    case "KafkaSink": {
      const needsUpsert =
        metadata?.changelogMode === "retract" &&
        metadata.primaryKey &&
        metadata.primaryKey.length > 0
      const rawFormat = (props.format as string) ?? "json"
      if (needsUpsert) {
        withProps.connector = "upsert-kafka"
        withProps.topic = props.topic as string
        withProps["key.format"] = "json"
        // upsert-kafka requires an insert-only value format — strip changelog
        // formats (debezium-json, canal-json) to their base serialization
        withProps["value.format"] = toInsertOnlyFormat(rawFormat)
      } else {
        withProps.connector = "kafka"
        withProps.topic = props.topic as string
        withProps.format = rawFormat
      }
      // Explicit prop takes priority; fall back to pipeline-level bootstrap
      const bootstrap = (props.bootstrapServers as string) ?? pipelineBootstrap
      if (bootstrap) {
        withProps["properties.bootstrap.servers"] = bootstrap
      }
      if (
        !needsUpsert &&
        (rawFormat === "debezium-avro" || rawFormat === "debezium-protobuf") &&
        props.schemaRegistryUrl
      ) {
        withProps[`${rawFormat}.schema-registry.url`] =
          props.schemaRegistryUrl as string
      }
      break
    }
    case "JdbcSink":
      withProps.connector = "jdbc"
      withProps.url = props.url as string
      withProps["table-name"] = props.table as string
      break
    case "FileSystemSink":
      withProps.connector = "filesystem"
      withProps.path = props.path as string
      if (props.format) withProps.format = props.format as string
      break
    case "GenericSink":
      withProps.connector = props.connector as string
      if (props.options) {
        Object.assign(withProps, props.options as Record<string, string>)
      }
      break
  }

  return Object.entries(withProps)
    .map(([k, v]) => `  '${k}' = '${v}'`)
    .join(",\n")
}

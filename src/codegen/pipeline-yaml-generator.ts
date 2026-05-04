// ── Flink CDC 3.6 Pipeline YAML generator ───────────────────────────
// For pipelines whose source is a Pipeline Connector (e.g.
// PostgresCdcPipelineSource), the runtime is flink-cdc-cli.jar reading
// a pipeline.yaml document — NOT Flink SQL. This module emits that YAML.
//
// Shape (3 top-level stanzas):
//   source:   postgres connector props
//   sink:     iceberg / paimon connector props forwarded from the sink node
//   pipeline: job metadata (name, parallelism, schema.change.behavior)

import type { SecretRef } from "@/core/secret-ref.js"
import { isSecretRef, renderSecretPlaceholder } from "@/core/secret-ref.js"
import type { ConstructNode } from "@/core/types.js"
import { toYaml } from "./crd-generator.js"
import { hasPipelineConnectorSource } from "./sql-generator.js"

// ── Types ───────────────────────────────────────────────────────────

export interface GeneratePipelineYamlOptions {
  /** Overrides the default `schema.change.behavior: evolve` value. */
  readonly schemaChangeBehavior?: "evolve" | "lenient" | "try_evolve" | "ignore"
}

// ── Helpers ─────────────────────────────────────────────────────────

function findFirstByComponent(
  node: ConstructNode,
  component: string,
): ConstructNode | undefined {
  if (node.component === component) return node
  for (const c of node.children) {
    const found = findFirstByComponent(c, component)
    if (found) return found
  }
  return undefined
}

function findSinkNode(node: ConstructNode): ConstructNode | undefined {
  if (node.kind === "Sink") return node
  for (const c of node.children) {
    const s = findSinkNode(c)
    if (s) return s
  }
  return undefined
}

function findCatalogNode(
  root: ConstructNode,
  catalogName: string,
): ConstructNode | undefined {
  if (
    root.kind === "Catalog" &&
    (root.props.name as string | undefined) === catalogName
  ) {
    return root
  }
  for (const c of root.children) {
    const found = findCatalogNode(c, catalogName)
    if (found) return found
  }
  return undefined
}

function renderValue(value: unknown): unknown {
  if (isSecretRef(value)) return renderSecretPlaceholder(value as SecretRef)
  return value
}

// ── Source stanza builders ──────────────────────────────────────────

function deriveSlotName(pipelineName: string): string {
  return `fr_${pipelineName.replace(/[^a-z0-9_]/gi, "_").toLowerCase()}_slot`
}

function derivePublicationName(pipelineName: string): string {
  return `fr_${pipelineName.replace(/[^a-z0-9_]/gi, "_").toLowerCase()}_pub`
}

function buildPostgresSourceStanza(
  node: ConstructNode,
  pipelineName: string,
): Record<string, unknown> {
  const p = node.props
  const tableList = p.tableList as readonly string[]

  // Flink CDC 3.6 collapsed the per-coordinate options
  // (`database-name` + `schema-name` + `table-name`) into a single
  // `tables` option carrying fully-qualified `<db>.<schema>.<table>`
  // patterns. `tableList` already arrives as `<schema>.<table>`
  // strings on the construct node; prepend the database name to land
  // in the new format.
  const fqTables = tableList
    .map((t) =>
      t.includes(".") && t.split(".").length >= 2
        ? `${p.database}.${t}`
        : `${p.database}.${t}`,
    )
    .join(",")

  // Flink CDC 3.6 dropped `publication.name` and `scan.snapshot.enabled`
  // from the Postgres pipeline source factory. The publication is now
  // implicitly the one CDC creates as a side-effect of the slot lifecycle,
  // and snapshot toggling moved entirely onto `scan.startup.mode`
  // (`initial` keeps snapshot+log; `latest-offset` skips snapshot;
  // `snapshot` for snapshot-only).
  const stanza: Record<string, unknown> = {
    type: "postgres",
    hostname: p.hostname,
    port: p.port ?? 5432,
    username: p.username,
    password: renderValue(p.password),
    tables: fqTables,
    "decoding.plugin.name": p.decodingPluginName ?? "pgoutput",
    "slot.name": p.replicationSlotName ?? deriveSlotName(pipelineName),
    // Debezium's default `publication.autocreate.mode` is `all_tables`,
    // which requires PostgreSQL superuser to run `CREATE PUBLICATION ...
    // FOR ALL TABLES`. `filtered` instead emits `FOR TABLE <captured>`
    // which only needs `CREATE` on the database (granted to the
    // dedicated `flink_cdc` role). Pass-through happens via the
    // `debezium.` prefix recognised by PostgresDataSourceFactory.
    "debezium.publication.autocreate.mode": "filtered",
  }

  // Expose connector-specific metadata columns to downstream transforms.
  // The 3.6 PostgresDataSource registers `op_ts` as a SupportedMetadataColumn
  // (source-commit timestamp). Connector-specific columns differ from the
  // CDC runtime's universal ones (`__schema_name__`, `__table_name__`, etc.)
  // — those are always-on; this list opts in to the per-connector ones.
  // Only declared when downstream transforms reference them via
  // `eventTimeColumn`, otherwise we'd ship a dangling column nothing reads.
  if (p.eventTimeColumn) {
    stanza["metadata.list"] = "op_ts"
  }

  // snapshotMode → scan.startup.mode (replaces the older
  // scan.snapshot.enabled toggle).
  const snapshotMode = (p.snapshotMode as string | undefined) ?? "initial"
  if (snapshotMode === "never") {
    stanza["scan.startup.mode"] = "latest-offset"
  } else if (snapshotMode === "initial_only") {
    stanza["scan.startup.mode"] = "snapshot"
  } else {
    stanza["scan.startup.mode"] = (p.startupMode as string) ?? "initial"
  }

  if (p.chunkSize !== undefined) {
    stanza["scan.incremental.snapshot.chunk.size"] = p.chunkSize
  }
  if (p.parallelism !== undefined) {
    stanza["scan.incremental.snapshot.chunk.key-column.parallelism"] =
      p.parallelism
  }
  if (p.heartbeatIntervalMs !== undefined) {
    stanza["heartbeat.interval.ms"] = p.heartbeatIntervalMs
  }
  if (p.slotDropOnStop === true) {
    stanza["slot.drop.on.stop"] = true
  }

  return stanza
}

// ── Sink stanza builders ────────────────────────────────────────────

function buildIcebergSinkStanza(
  sinkNode: ConstructNode,
  root: ConstructNode,
): Record<string, unknown> {
  const p = sinkNode.props
  const catalogName = p.catalogName as string
  const catalogNode = findCatalogNode(root, catalogName)
  const stanza: Record<string, unknown> = {
    type: "iceberg",
    "catalog.name": catalogName,
    database: p.database,
    table: p.table,
  }
  if (catalogNode) {
    const cp = catalogNode.props
    stanza["catalog.properties.type"] =
      (cp.catalogType as string | undefined) ?? "rest"
    if (cp.uri) stanza["catalog.properties.uri"] = cp.uri
    if (cp.warehouse) stanza["catalog.properties.warehouse"] = cp.warehouse
  }

  // upsertEnabled implies format-version 2 — render it explicitly even when
  // the user omitted formatVersion, so the emitted options form a complete,
  // self-consistent MoR configuration.
  const upsertEnabled = p.upsertEnabled === true
  const effectiveFormatVersion =
    p.formatVersion ?? (upsertEnabled ? 2 : undefined)
  if (effectiveFormatVersion !== undefined) {
    stanza["table.properties.format-version"] = String(effectiveFormatVersion)
  }
  if (upsertEnabled) {
    stanza["table.properties.write.upsert.enabled"] = "true"
  }

  // Equality-delete columns: explicit equalityFieldColumns wins over primaryKey.
  const equalityFields =
    Array.isArray(p.equalityFieldColumns) && p.equalityFieldColumns.length > 0
      ? (p.equalityFieldColumns as readonly string[])
      : Array.isArray(p.primaryKey) && p.primaryKey.length > 0 && upsertEnabled
        ? (p.primaryKey as readonly string[])
        : undefined
  if (equalityFields) {
    stanza["table.properties.equality-field-columns"] = equalityFields.join(",")
  }

  if (Array.isArray(p.primaryKey) && p.primaryKey.length > 0) {
    stanza["table.properties.primary-key"] = (
      p.primaryKey as readonly string[]
    ).join(",")
  }

  if (p.commitIntervalSeconds !== undefined) {
    stanza["table.properties.commit-interval-ms"] = String(
      (p.commitIntervalSeconds as number) * 1000,
    )
  }
  if (p.writeDistributionMode !== undefined) {
    stanza["table.properties.write.distribution-mode"] =
      p.writeDistributionMode as string
  }
  if (p.targetFileSizeMB !== undefined) {
    stanza["table.properties.write.target-file-size-bytes"] = String(
      (p.targetFileSizeMB as number) * 1024 * 1024,
    )
  }
  if (p.writeParquetCompression !== undefined) {
    stanza["table.properties.write.parquet.compression-codec"] =
      p.writeParquetCompression as string
  }

  return stanza
}

function buildPaimonSinkStanza(
  sinkNode: ConstructNode,
  root: ConstructNode,
): Record<string, unknown> {
  const p = sinkNode.props
  const catalogName = p.catalogName as string
  const catalogNode = findCatalogNode(root, catalogName)
  const stanza: Record<string, unknown> = {
    type: "paimon",
    "catalog.name": catalogName,
    database: p.database,
    table: p.table,
  }
  if (catalogNode?.props.warehouse) {
    stanza["catalog.properties.warehouse"] = catalogNode.props.warehouse
  }
  if (catalogNode?.props.metastore) {
    stanza["catalog.properties.metastore"] = catalogNode.props.metastore
  }
  if (Array.isArray(p.primaryKey) && p.primaryKey.length > 0) {
    stanza["table.properties.primary-key"] = (
      p.primaryKey as readonly string[]
    ).join(",")
  }
  if (p.mergeEngine) stanza["table.properties.merge-engine"] = p.mergeEngine
  if (p.changelogProducer) {
    stanza["table.properties.changelog-producer"] = p.changelogProducer
  }
  if (p.sequenceField) {
    stanza["table.properties.sequence.field"] = p.sequenceField
  }
  if (p.bucket != null) {
    stanza["table.properties.bucket"] = String(p.bucket)
  }
  const explicitBucketKey = p.bucketKey as readonly string[] | undefined
  const pk = p.primaryKey as readonly string[] | undefined
  const bucketKey =
    Array.isArray(explicitBucketKey) && explicitBucketKey.length > 0
      ? explicitBucketKey
      : p.bucket != null && Array.isArray(pk) && pk.length > 0
        ? pk
        : undefined
  if (bucketKey) {
    stanza["table.properties.bucket-key"] = bucketKey.join(",")
  }
  if (p.fullCompactionDeltaCommits != null) {
    stanza["table.properties.full-compaction.delta-commits"] = String(
      p.fullCompactionDeltaCommits,
    )
  }
  if (p.writeBufferSizeMB != null) {
    stanza["table.properties.write-buffer-size"] = String(
      Number(p.writeBufferSizeMB) * 1048576,
    )
  }
  if (p.snapshotNumRetainedMin != null) {
    stanza["table.properties.snapshot.num-retained.min"] = String(
      p.snapshotNumRetainedMin,
    )
  }
  if (p.snapshotNumRetainedMax != null) {
    stanza["table.properties.snapshot.num-retained.max"] = String(
      p.snapshotNumRetainedMax,
    )
  }
  return stanza
}

function buildFlussSinkStanza(
  sinkNode: ConstructNode,
  root: ConstructNode,
): Record<string, unknown> {
  const p = sinkNode.props
  const catalogName = p.catalogName as string
  const catalogNode = findCatalogNode(root, catalogName)
  const pipelineName =
    (root.props.name as string | undefined) ?? "fluss-pipeline"

  const stanza: Record<string, unknown> = {
    type: "fluss",
    name: `${pipelineName}-fluss-sink`,
  }

  if (catalogNode?.props.bootstrapServers) {
    stanza["bootstrap.servers"] = catalogNode.props.bootstrapServers
  }

  // Flink CDC 3.6's `flink-cdc-pipeline-connector-fluss` exposes a flat
  // option surface: only `bucket.num`, `bucket.key`, `bootstrap.servers`,
  // and the SASL props are honoured by FlussDataSinkFactory. The older
  // `table.properties.*` namespace was the SQL-connector convention and
  // is rejected by the 3.6 pipeline factory's validateExcept() check.
  //
  // Both `bucket.num` and `bucket.key` use the per-table form
  // `<schema.table>:<value>;<schema.table>:<value>` — a flat scalar is
  // rejected with "Invalid bucket {number,key} configuration". Pipeline
  // CDC fans out multiple upstream tables through one sink stanza, so the
  // FlussSink's flat `buckets` and `primaryKey` props can't be applied
  // to one of them without misrouting the others. Resolving the upstream
  // table set requires walking back up the DAG to the source's
  // tableList prop; until that's wired, omit both options and let Fluss
  // fall back to defaults (round-robin bucketing, source-PK as bucket
  // key, server-default bucket count). Per-table overrides should land
  // on the source/transform side, not the sink, since they're
  // table-scoped concepts.

  // Forward SASL credentials when the catalog declares them. The password
  // is rendered through the same `${env:VAR}` placeholder path Postgres
  // uses, so the same secretRef ergonomics apply on both sides of the YAML.
  if (catalogNode) {
    const cp = catalogNode.props
    if (cp.securityProtocol) {
      stanza["properties.client.security.protocol"] = cp.securityProtocol
    }
    if (cp.saslMechanism) {
      stanza["properties.client.security.sasl.mechanism"] = cp.saslMechanism
    }
    if (cp.saslUsername) {
      stanza["properties.client.security.sasl.username"] = cp.saslUsername
    }
    if (cp.saslPassword !== undefined) {
      stanza["properties.client.security.sasl.password"] = renderValue(
        cp.saslPassword,
      )
    }
  }

  // schema.evolution.behavior moved out of the sink stanza in Flink CDC
  // 3.6 — schema-change handling is now a pipeline-level concern (see
  // `schema.change.behavior` emitted in buildPipelineStanza). Leaving it
  // on the sink causes FactoryHelper.validateExcept() to reject the YAML.

  return stanza
}

function buildSinkStanza(
  sinkNode: ConstructNode,
  root: ConstructNode,
): Record<string, unknown> {
  switch (sinkNode.component) {
    case "IcebergSink":
      return buildIcebergSinkStanza(sinkNode, root)
    case "PaimonSink":
      return buildPaimonSinkStanza(sinkNode, root)
    case "FlussSink":
      return buildFlussSinkStanza(sinkNode, root)
    default:
      throw new Error(
        `Pipeline Connector cannot emit a sink stanza for component '${sinkNode.component}'. ` +
          `Supported sinks: IcebergSink, PaimonSink, FlussSink. Connect to a supported sink or fall back to the Kafka-hop topology.`,
      )
  }
}

// ── Public API ──────────────────────────────────────────────────────

/**
 * Generate a Flink CDC 3.6 pipeline.yaml document for a Pipeline-Connector
 * pipeline. Returns `null` when the tree has no Pipeline Connector source.
 *
 * The output is deterministic: stanza key order is stable, and any
 * identical input produces byte-identical output.
 */
export function generatePipelineYaml(
  pipelineNode: ConstructNode,
  options: GeneratePipelineYamlOptions = {},
): string | null {
  if (!hasPipelineConnectorSource(pipelineNode)) return null

  const pipelineName =
    (pipelineNode.props.name as string | undefined) ?? "pipeline"

  // v1 only supports Postgres — when we add more Pipeline Connector sources
  // we can dispatch on component here.
  const source = findFirstByComponent(pipelineNode, "PostgresCdcPipelineSource")
  if (!source) return null

  const sink = findSinkNode(pipelineNode)
  if (!sink) {
    throw new Error(
      `Pipeline '${pipelineName}' has a PostgresCdcPipelineSource but no downstream sink`,
    )
  }

  const transform = buildTransformStanzaList(source)

  const doc: Record<string, unknown> = {
    source: buildPostgresSourceStanza(source, pipelineName),
    ...(transform ? { transform } : {}),
    sink: buildSinkStanza(sink, pipelineNode),
    pipeline: {
      name: pipelineName,
      ...(pipelineNode.props.parallelism !== undefined
        ? { parallelism: pipelineNode.props.parallelism }
        : {}),
      "schema.change.behavior": options.schemaChangeBehavior ?? "evolve",
    },
  }

  return toYaml(doc)
}

/**
 * Project Debezium's source-commit timestamp (`__op_ts__`) as a real
 * column on every captured table when the source declares an
 * `eventTimeColumn`. Returns one transform entry per upstream table so
 * each schema event independently carries the new column — using a
 * regex `source-table` would technically work but couples the projection
 * to whatever else lives in the postgres schema.
 */
function buildTransformStanzaList(
  source: ConstructNode,
): readonly Record<string, unknown>[] | undefined {
  const eventTimeColumn = source.props.eventTimeColumn as string | undefined
  if (!eventTimeColumn) return undefined

  const database = source.props.database as string
  const tableList = source.props.tableList as readonly string[]

  return tableList.map((t) => ({
    "source-table": t.includes(".") ? `${database}.${t}` : `${database}.${t}`,
    // CAST keeps the column type explicit and matches whatever
    // downstream Schema(...) declarations expect — avoid surprising
    // implicit conversions if a consumer declares TIMESTAMP(3) without
    // _LTZ.
    // Flink CDC requires `*` (all-columns) to be escaped as `\*` in YAML
    // because YAML treats a bare `*` as an anchor-reference operator; the
    // CDC parser unescapes it before evaluating the projection. `op_ts`
    // is the postgres connector's source-commit-timestamp metadata column
    // (a SupportedMetadataColumn on PostgresDataSource — different from
    // the universal `__namespace_name__`/`__schema_name__` doubly-
    // underscored constants). It must be declared via `metadata.list:
    // op_ts` on the source for the parser to find it.
    //
    // OpTsMetadataColumn.getType() returns BIGINT (epoch milliseconds),
    // not TIMESTAMP_LTZ. A bare rename `op_ts AS event_time` would land
    // a BIGINT column in Fluss, which then can't carry watermarks
    // downstream and conflicts on subsequent schema events. Use
    // `TO_TIMESTAMP_LTZ(epoch, 3)` — Flink's standard helper for the
    // conversion — to pin the output column's type as TIMESTAMP_LTZ(3)
    // at the projection level. `CAST(... AS TIMESTAMP_LTZ(3))` would also
    // work conceptually but Calcite's parser rejects the Flink-specific
    // `TIMESTAMP_LTZ` keyword in CAST grammar.
    projection: `\\*, TO_TIMESTAMP_LTZ(op_ts, 3) AS ${eventTimeColumn}`,
  }))
}

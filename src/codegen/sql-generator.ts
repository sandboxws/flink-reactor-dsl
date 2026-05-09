import { Either } from "effect"
import { SqlGenerationError } from "@/core/errors.js"
import { FlinkVersionCompat } from "@/core/flink-compat.js"
import type { PluginDdlGenerator, PluginSqlGenerator } from "@/core/plugin.js"
import type { SchemaDefinition } from "@/core/schema.js"
import type { ValidationDiagnostic } from "@/core/synth-context.js"
import { SynthContext } from "@/core/synth-context.js"
import { generateTapMetadata } from "@/core/tap.js"
import type {
  ConstructNode,
  FlinkMajorVersion,
  TapManifest,
} from "@/core/types.js"
import type { OptimizeOptions } from "./pipeline-optimizer.js"
import { optimizePipeline } from "./pipeline-optimizer.js"
import {
  collectTransformChain,
  findDeepestSource,
  indexTree,
  type ResolvedColumn,
  resolveNodeSchema,
  resolveTransformSchema,
} from "./schema-introspect.js"
import {
  type BuildContext,
  beginFragmentCollection,
  createBuildContext,
  endFragmentCollection,
  enterSynthesis,
  exitSynthesis,
  pushFragment,
  type SqlFragment,
  type StatementOrigin,
  shiftFragmentsSince,
} from "./sql-build-context.js"
import { toInsertOnlyFormat, toInterval } from "./sql-duration.js"
// `q` is the historical alias used by the ~96 inline call sites in this
// file. The shared helper additionally escapes embedded backticks, which
// the previous local copy did not.
import { quoteIdentifier as q } from "./sql-identifiers.js"
import { generateSetStatements } from "./sql-set-statements.js"
import {
  buildCatalogDetails,
  buildCommentBlock,
  buildSinkDetails,
  buildSourceDetails,
  buildSourceSchema,
  getTransformDetail,
} from "./sql-statement-meta.js"
import { verifySql } from "./sql-verifier.js"

// Re-export the synthesis types from sql-build-context.ts so existing
// public consumers (e.g. browser.ts) keep their imports stable.
export type { SqlFragment, StatementOrigin }

// ── Pipeline Connector detection ────────────────────────────────────

/**
 * Components that represent a Flink CDC Pipeline Connector source. These
 * drive a fundamentally different runtime (flink-cdc-cli + pipeline.yaml)
 * than the SQL pipelines this file generates, so the SQL generator
 * short-circuits when one is present.
 */
const PIPELINE_CONNECTOR_SOURCES: ReadonlySet<string> = new Set([
  "PostgresCdcPipelineSource",
])

/** True iff the tree contains at least one Flink CDC Pipeline Connector source. */
export function hasPipelineConnectorSource(node: ConstructNode): boolean {
  if (PIPELINE_CONNECTOR_SOURCES.has(node.component)) return true
  for (const c of node.children) {
    if (hasPipelineConnectorSource(c)) return true
  }
  return false
}

// ── Public API ──────────────────────────────────────────────────────
//
// Synthesis state lives on a per-call `BuildContext` (see
// sql-build-context.ts) threaded through every internal helper as the
// first argument. The reentrancy tripwire fires when a plugin tries to
// re-enter `generateSql` synchronously — `BuildContext.synthDepth.value`
// is set to 1 on entry and reset on exit; nested calls throw.

export interface GenerateSqlOptions {
  readonly flinkVersion?: FlinkMajorVersion
  /** Plugin-provided SQL query generators (component name → generator) */
  readonly pluginSqlGenerators?: ReadonlyMap<string, PluginSqlGenerator>
  /** Plugin-provided DDL generators (component name → generator) */
  readonly pluginDdlGenerators?: ReadonlyMap<string, PluginDdlGenerator>
  /** Enable dev mode: auto-taps all sinks for maximum visibility */
  readonly devMode?: boolean
  /** Disable tap metadata generation entirely */
  readonly noTap?: boolean
  /** Enable pipeline optimization passes (default: false) */
  readonly optimize?: boolean | OptimizeOptions
  /**
   * ISO-8601 timestamp stamped onto the tap manifest's `generatedAt` field.
   * CLI/orchestration paths pass `new Date().toISOString()`; tests and
   * snapshots leave it unset so the sentinel keeps output deterministic.
   */
  readonly synthesizedAt?: string
}

/** Sentinel used when no `synthesizedAt` is provided. Deliberately fake so
 *  callers that should be passing real time (CLI commands) are easy to spot. */
const SYNTHESIZED_AT_SENTINEL = "1970-01-01T00:00:00.000Z"

export type SqlSection =
  | "configuration"
  | "catalogs"
  | "functions"
  | "sources"
  | "sinks"
  | "views"
  | "materialized-tables"
  | "pipeline"

/** Metadata for a single SQL statement, used by dashboard for hover tooltips. */
export interface StatementMeta {
  /** Human-readable label (e.g., 'KafkaSource: raw_events'). */
  readonly label: string
  /** Section this statement belongs to. */
  readonly section: SqlSection
  /** Node kind. Absent for comment-only entries. */
  readonly kind?: string
  /** Component class name (e.g., "KafkaSource"). */
  readonly component?: string
  /** Key-value details for tooltip body. */
  readonly details: ReadonlyArray<{ key: string; value: string }>
  /** Schema columns, if applicable (sources/sinks). */
  readonly schema?: ReadonlyArray<{ name: string; type: string }>
}

export interface GenerateSqlResult {
  readonly statements: readonly string[]
  readonly sql: string
  readonly diagnostics: readonly ValidationDiagnostic[]
  /** Maps each statement index to the node that produced it. Entries may be absent for synthetic statements (SET, STATEMENT SET wrappers). */
  readonly statementOrigins: ReadonlyMap<number, StatementOrigin>
  /** All contributing nodes per statement, with optional sub-statement spans. */
  readonly statementContributors: ReadonlyMap<number, readonly SqlFragment[]>
  /** Indices of comment-only statements (not executable SQL). */
  readonly commentIndices: ReadonlySet<number>
  /** Per-statement metadata for hover tooltips (keyed by statement index). */
  readonly statementMeta: ReadonlyMap<number, StatementMeta>
}

/**
 * Generate Flink SQL from a Pipeline construct tree.
 *
 * Walks the construct tree, builds a DAG, then emits SQL in
 * deterministic order:
 *   1. SET statements (pipeline config)
 *   2. CREATE CATALOG
 *   3. CREATE TABLE (sources)
 *   4. CREATE TABLE (sinks)
 *   5. INSERT INTO / STATEMENT SET
 */
export function generateSql(
  pipelineNode: ConstructNode,
  options: GenerateSqlOptions = {},
): GenerateSqlResult {
  const version = options.flinkVersion ?? "2.2"
  // The ctx.nodeIndex is rebuilt below after optimisation; seed the context
  // with an empty map so reentrancy is checked before optimisation runs.
  const ctx = createBuildContext({
    version,
    nodeIndex: new Map(),
    pluginSqlGenerators: options.pluginSqlGenerators,
    pluginDdlGenerators: options.pluginDdlGenerators,
  })
  enterSynthesis(ctx)
  try {
    return generateSqlImpl(ctx, pipelineNode, options)
  } finally {
    exitSynthesis(ctx)
  }
}

function generateSqlImpl(
  ctx: BuildContext,
  pipelineNode: ConstructNode,
  options: GenerateSqlOptions,
): GenerateSqlResult {
  const pluginDdl = ctx.pluginDdlGenerators

  // Run optimization passes if enabled
  let currentTree = pipelineNode
  let optimizerSets: readonly string[] = []
  if (options.optimize) {
    const optimizeOpts =
      typeof options.optimize === "object" ? options.optimize : {}
    const result = optimizePipeline(currentTree, ctx.version, optimizeOpts)
    currentTree = result.tree
    optimizerSets = result.additionalSets
  }

  // Build the per-synthesis node index. The optimised tree is what
  // every downstream builder walks; populate the ctx-owned index here so
  // the entire synthesis sees the same view.
  indexTree(currentTree, ctx.nodeIndex)

  const synthCtx = new SynthContext()
  synthCtx.buildFromTree(currentTree)

  const statements: string[] = []
  const statementOrigins = new Map<number, StatementOrigin>()
  const statementContributors = new Map<number, readonly SqlFragment[]>()
  const commentIndices = new Set<number>()
  const statementMeta = new Map<number, StatementMeta>()

  /** Push a statement and record which node produced it. */
  function emit(sql: string, node?: ConstructNode): void {
    if (node) {
      statementOrigins.set(statements.length, {
        nodeId: node.id,
        component: node.component,
        kind: node.kind,
      })
    }
    statements.push(sql)
  }

  /** Push a comment-only statement (comment block). */
  function emitComment(comment: string, meta?: StatementMeta): void {
    const idx = statements.length
    commentIndices.add(idx)
    if (meta) statementMeta.set(idx, meta)
    statements.push(comment)
  }

  // Pipeline Connector short-circuit:
  // When the pipeline is driven by a Flink CDC Pipeline Connector source
  // (e.g. PostgresCdcPipelineSource), the runtime is flink-cdc-cli.jar
  // reading `pipeline.yaml` — there is no Flink SQL to execute. Emit only
  // a banner explaining where the runtime definition lives so existing
  // tooling that reads the SQL output doesn't crash on an empty file.
  if (hasPipelineConnectorSource(currentTree)) {
    const pipelineName =
      (currentTree.props.name as string | undefined) ?? "(unnamed)"
    const details: Array<{ key: string; value: string }> = [
      { key: "name", value: pipelineName },
      { key: "runtime", value: "flink-cdc-cli" },
      { key: "artifact", value: "pipeline.yaml" },
    ]
    emitComment(buildCommentBlock("PIPELINE CONNECTOR", details), {
      label: "Pipeline Connector",
      section: "configuration",
      details,
    })
    emitComment(
      "-- This pipeline is a Flink CDC Pipeline Connector job.\n" +
        "-- The runtime definition lives in pipeline.yaml (ConfigMap-mounted),\n" +
        "-- not in Flink SQL. Inspect pipeline.yaml and deployment.yaml for the\n" +
        "-- full topology.",
    )
    return {
      statements,
      sql: `${statements.join("\n\n")}\n`,
      diagnostics: [],
      statementOrigins,
      statementContributors,
      commentIndices,
      statementMeta,
    }
  }

  // 1. SET statements from pipeline props
  const setStatements = generateSetStatements(currentTree, ctx.version)
  const allSets = [...setStatements, ...optimizerSets]
  if (allSets.length > 0) {
    const pProps = currentTree.props
    const details: Array<{ key: string; value: string }> = []
    if (pProps.name) details.push({ key: "name", value: String(pProps.name) })
    if (pProps.parallelism !== undefined)
      details.push({ key: "parallelism", value: String(pProps.parallelism) })
    if (pProps.mode) details.push({ key: "mode", value: String(pProps.mode) })
    if (pProps.checkpoint) {
      const cp = pProps.checkpoint as { interval: string; mode?: string }
      details.push({ key: "checkpoint", value: cp.interval })
      if (cp.mode) details.push({ key: "cp-mode", value: cp.mode })
    }
    if (pProps.stateBackend)
      details.push({ key: "state", value: String(pProps.stateBackend) })
    if (pProps.stateTtl)
      details.push({ key: "state-ttl", value: String(pProps.stateTtl) })

    emitComment(buildCommentBlock("PIPELINE", details), {
      label: "Pipeline",
      section: "configuration",
      details,
    })
    for (const s of allSets) {
      emit(s, currentTree)
    }
  }

  // 2. CREATE CATALOG
  const catalogs = synthCtx.getNodesByKind("Catalog")
  for (const cat of catalogs) {
    const details = buildCatalogDetails(cat)
    emitComment(buildCommentBlock("CATALOG", details), {
      label: `Catalog: ${cat.id}`,
      section: "catalogs",
      kind: cat.kind,
      component: cat.component,
      details,
    })
    emit(generateCatalogDdl(cat), cat)
  }

  // 2.5. CREATE FUNCTION (UDFs)
  const udfs = synthCtx.getNodesByKind("UDF")
  for (const udf of udfs) {
    const funcName = udf.props.name as string
    const className = udf.props.className as string
    const details: Array<{ key: string; value: string }> = [
      { key: "name", value: funcName },
      { key: "class", value: className },
      { key: "language", value: (udf.props.language as string) ?? "java" },
    ]
    emitComment(buildCommentBlock("FUNCTION", details), {
      label: `UDF: ${funcName}`,
      section: "functions",
      kind: udf.kind,
      component: "UDF",
      details,
    })
    emit(generateUdfDdl(udf), udf)
  }

  // 3. CREATE TABLE (sources)
  const sources = synthCtx.getNodesByKind("Source")
  for (const src of sources) {
    const pluginGen = pluginDdl?.get(src.component)
    let ddl: string | null = null
    if (pluginGen) {
      ddl = pluginGen(src)
    } else {
      ddl = generateSourceDdl(src)
    }
    if (ddl) {
      const details = buildSourceDetails(src)
      const schema = buildSourceSchema(src)
      emitComment(buildCommentBlock("SOURCE TABLE", details), {
        label: `Source: ${src.id}`,
        section: "sources",
        kind: src.kind,
        component: src.component,
        details,
        schema,
      })
      emit(ddl, src)
    }
  }

  // 4. CREATE TABLE (sinks) — resolve schemas then generate DDL
  const sinks = synthCtx.getNodesByKind("Sink")
  const sinkMeta = resolveSinkMetadata(ctx, currentTree)

  // Resolve pipeline-level bootstrapServers from any KafkaSource so
  // KafkaSinks that omit it still produce self-contained DDL.
  const pipelineBootstrap = sources
    .filter((s) => s.component === "KafkaSource" && s.props.bootstrapServers)
    .map((s) => s.props.bootstrapServers as string)[0]

  for (const sink of sinks) {
    const pluginGen = pluginDdl?.get(sink.component)
    const resolved = sinkMeta.get(sink.id)
    const details = buildSinkDetails(sink)
    const schema = resolved?.schema
      ? resolved.schema.map((c) => ({ name: c.name, type: c.type }))
      : undefined
    emitComment(buildCommentBlock("SINK TABLE", details), {
      label: `Sink: ${sink.id}`,
      section: "sinks",
      kind: sink.kind,
      component: sink.component,
      details,
      schema,
    })
    if (pluginGen) {
      const ddl = pluginGen(sink)
      if (ddl) emit(ddl, sink)
    } else {
      emit(generateSinkDdl(sink, resolved, pipelineBootstrap), sink)
    }
  }

  // 4.5. CREATE VIEW
  const views = synthCtx.getNodesByKind("View")
  for (const view of views) {
    if (view.children.length > 0) {
      const viewName = view.props.name as string
      const details: Array<{ key: string; value: string }> = [
        { key: "name", value: viewName },
      ]
      emitComment(buildCommentBlock("VIEW", details), {
        label: `View: ${viewName}`,
        section: "views",
        kind: view.kind,
        component: "View",
        details,
      })
      emit(generateViewDdl(ctx, view), view)
    }
  }

  // 4.75. CREATE MATERIALIZED TABLE
  const matTables = synthCtx.getNodesByKind("MaterializedTable")
  for (const matTable of matTables) {
    const mtName = matTable.props.name as string
    const details: Array<{ key: string; value: string }> = [
      { key: "name", value: mtName },
    ]
    if (matTable.props.catalogName)
      details.push({
        key: "catalog",
        value: String(matTable.props.catalogName),
      })
    if (matTable.props.freshness)
      details.push({
        key: "freshness",
        value: String(matTable.props.freshness),
      })
    emitComment(buildCommentBlock("MATERIALIZED TABLE", details), {
      label: `MaterializedTable: ${mtName}`,
      section: "materialized-tables",
      kind: matTable.kind,
      component: "MaterializedTable",
      details,
    })
    emit(generateMaterializedTableDdl(ctx, matTable), matTable)
  }

  // 5. DML: INSERT INTO / STATEMENT SET
  const dmlEntries = generateDml(ctx, currentTree, sinkMeta)
  for (const entry of dmlEntries) {
    const details = buildDmlDetails(ctx, entry)
    emitComment(buildCommentBlock("TRANSFORMATION", details), {
      label: `Transformation: ${details.find((d) => d.key === "output")?.value ?? "unknown"}`,
      section: "pipeline",
      details,
    })
  }
  if (dmlEntries.length === 1) {
    const entry = dmlEntries[0]
    if (entry.contributors.length > 0) {
      statementContributors.set(statements.length, entry.contributors)
    }
    statements.push(entry.sql)
  } else if (dmlEntries.length > 1) {
    // Wrap multiple DML in EXECUTE STATEMENT SET, shifting fragment offsets
    const prefix = "EXECUTE STATEMENT SET BEGIN\n"
    const allContributors: SqlFragment[] = []
    let currentOffset = prefix.length
    for (const entry of dmlEntries) {
      for (const c of entry.contributors) {
        allContributors.push({ ...c, offset: c.offset + currentOffset })
      }
      currentOffset += entry.sql.length + 1 // +1 for \n between statements
    }
    if (allContributors.length > 0) {
      statementContributors.set(statements.length, allContributors)
    }
    statements.push(`${prefix}${dmlEntries.map((d) => d.sql).join("\n")}\nEND;`)
  }

  // Filter out comment-only entries before SQL verification (comments aren't executable)
  const executableStatements = statements.filter(
    (_, i) => !commentIndices.has(i),
  )
  const diagnostics = verifySql(executableStatements)

  return {
    statements,
    sql: `${statements.join("\n\n")}\n`,
    diagnostics,
    statementOrigins,
    statementContributors,
    commentIndices,
    statementMeta,
  }
}

function buildDmlDetails(
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

/**
 * Generate a TapManifest from a Pipeline construct tree.
 *
 * This is a post-processing step that runs after (or independently of)
 * main SQL generation. It walks the tree, collects tapped nodes, and
 * produces observation metadata. Production SQL is never modified.
 *
 * Returns null if tap generation is disabled or no taps are found.
 */
export function generateTapManifest(
  pipelineNode: ConstructNode,
  options: GenerateSqlOptions = {},
): { manifest: TapManifest | null; diagnostics: ValidationDiagnostic[] } {
  if (options.noTap) {
    return { manifest: null, diagnostics: [] }
  }

  const version = options.flinkVersion ?? "2.2"
  const devMode = options.devMode ?? false
  const pipelineName = (pipelineNode.props.name as string) ?? "unnamed"

  const { taps, diagnostics } = generateTapMetadata(
    pipelineNode,
    pipelineName,
    version,
    devMode,
  )

  if (taps.length === 0) {
    return { manifest: null, diagnostics }
  }

  const manifest: TapManifest = {
    pipelineName,
    flinkVersion: version,
    generatedAt: options.synthesizedAt ?? SYNTHESIZED_AT_SENTINEL,
    taps,
  }

  return { manifest, diagnostics }
}

// ── CREATE CATALOG DDL ──────────────────────────────────────────────

function generateCatalogDdl(node: ConstructNode): string {
  const props = node.props
  const name = props.name as string
  const withProps: Record<string, string> = {}

  switch (node.component) {
    case "PaimonCatalog":
      withProps.type = "paimon"
      withProps.warehouse = props.warehouse as string
      if (props.metastore) withProps.metastore = props.metastore as string
      if (props.s3Endpoint)
        withProps["s3.endpoint"] = props.s3Endpoint as string
      if (props.s3AccessKey)
        withProps["s3.access-key"] = props.s3AccessKey as string
      if (props.s3SecretKey)
        withProps["s3.secret-key"] = props.s3SecretKey as string
      if (props.s3PathStyleAccess !== undefined)
        withProps["s3.path-style-access"] = String(props.s3PathStyleAccess)
      break
    case "IcebergCatalog":
      withProps.type = "iceberg"
      withProps["catalog-type"] = props.catalogType as string
      withProps.uri = props.uri as string
      if (props.warehouse) withProps.warehouse = props.warehouse as string
      break
    case "HiveCatalog":
      withProps.type = "hive"
      withProps["hive-conf-dir"] = props.hiveConfDir as string
      break
    case "JdbcCatalog":
      withProps.type = "jdbc"
      withProps["base-url"] = props.baseUrl as string
      withProps["default-database"] = props.defaultDatabase as string
      break
    case "GenericCatalog":
      withProps.type = props.type as string
      if (props.options) {
        Object.assign(withProps, props.options as Record<string, string>)
      }
      break
    case "FlussCatalog":
      withProps.type = "fluss"
      withProps["bootstrap.servers"] = props.bootstrapServers as string
      break
  }

  const withClause = Object.entries(withProps)
    .map(([k, v]) => `  '${k}' = '${v}'`)
    .join(",\n")

  return `CREATE CATALOG ${q(name)} WITH (\n${withClause}\n);`
}

// ── CREATE FUNCTION DDL (UDFs) ──────────────────────────────────────

function generateUdfDdl(node: ConstructNode): string {
  const name = node.props.name as string
  const className = node.props.className as string
  return `CREATE FUNCTION ${q(name)} AS '${className}';`
}

// ── CREATE TABLE DDL (Sources) ──────────────────────────────────────

function generateSourceDdl(node: ConstructNode): string | null {
  if (node.component === "CatalogSource") return null

  // FlussSource is catalog-managed. Apache Fluss 0.9.0-incubating registers
  // only `FlinkCatalogFactory` (not a `DynamicTableFactory`), so emitting
  // a standalone `CREATE TABLE … WITH ('connector'='fluss', …)` fails at
  // deploy time with `Cannot discover a connector using option:
  // 'connector'='fluss'`. Derive an alias in default_catalog from the
  // catalog-qualified table via `CREATE TABLE LIKE` — Fluss handles
  // routing internally through the catalog binding.
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

function generateColumns(schema: SchemaDefinition): string {
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

function generateConstraints(
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

// ── CREATE TABLE DDL (Sinks) ────────────────────────────────────────

function generateSinkDdl(
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
 * Emit DDL for a catalog-managed sink (currently IcebergSink).
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
 */
function resolvePartitionExpression(
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

// ── Sink schema resolution ──────────────────────────────────────────

type ChangelogMode = "append-only" | "retract"

interface SinkMetadata {
  readonly schema: ResolvedColumn[]
  readonly changelogMode: ChangelogMode
  readonly primaryKey?: readonly string[]
}

/**
 * Walk the pipeline tree to resolve metadata (schema, changelog mode, primary key) for all sinks.
 *
 * Handles both patterns:
 * - Reverse-nesting (Sink → Transform → Source): sink's children are its upstream
 * - Forward-reading with Route (Pipeline → Source, Route → Branch → Transform, Sink):
 *   sink's upstream comes from the Route's source via branch transforms
 */
function resolveSinkMetadata(
  ctx: BuildContext,
  pipelineNode: ConstructNode,
): Map<string, SinkMetadata> {
  const result = new Map<string, SinkMetadata>()

  function resolveSourceChangelogMode(source: ConstructNode): ChangelogMode {
    const mode = source.props.changelogMode as string | undefined
    if (mode === "retract") return "retract"
    // A KafkaSource with a schema primary key is emitted as upsert-kafka,
    // which carries upsert (retract-equivalent) changelog semantics even
    // though its format (json/avro) isn't listed as CDC by inferChangelogMode.
    if (source.component === "KafkaSource") {
      const schema = source.props.schema as
        | { primaryKey?: { columns?: readonly string[] } }
        | undefined
      if ((schema?.primaryKey?.columns?.length ?? 0) > 0) return "retract"
    }
    return "append-only"
  }

  function resolveSourcePrimaryKey(
    source: ConstructNode,
  ): readonly string[] | undefined {
    const schema = source.props.schema as
      | { primaryKey?: { columns: readonly string[] } }
      | undefined
    if (schema?.primaryKey?.columns) return schema.primaryKey.columns
    // Sources whose schema doesn't carry a PK but whose component-level prop
    // does (e.g. FlussSource on a Fluss PrimaryKey table) — surface the prop
    // PK so downstream sinks can see the upstream key.
    const propPk = source.props.primaryKey as readonly string[] | undefined
    if (Array.isArray(propPk) && propPk.length > 0) return propPk
    return undefined
  }

  /**
   * Propagate changelog mode through a transform chain.
   *
   * - Unbounded Aggregate (no window) → retract (emits updates as values change)
   * - Windowed Aggregate (inside TumbleWindow/SlideWindow/SessionWindow) → append-only
   *   (each window fires a single final result)
   * - Other transforms preserve the upstream changelog mode.
   */
  function propagateChangelogMode(
    transforms: readonly ConstructNode[],
    upstreamMode: ChangelogMode,
  ): ChangelogMode {
    let mode = upstreamMode
    const hasWindow = transforms.some((t) => t.kind === "Window")
    for (const t of transforms) {
      // Only unbounded aggregation produces retract; windowed aggregation is append-only
      if (t.component === "Aggregate" && !hasWindow) {
        mode = "retract"
      }
      // Session windows produce retract (sessions can merge on late events)
      if (t.component === "SessionWindow") {
        mode = "retract"
      }
    }
    return mode
  }

  /**
   * Propagate primary key through a transform chain.
   * - Filter/Map/Deduplicate/TopN: PK preserved from upstream
   * - Aggregate: PK becomes the groupBy columns
   * - Window with Aggregate child: PK becomes the groupBy columns
   */
  function propagatePrimaryKey(
    transforms: readonly ConstructNode[],
    upstreamPk: readonly string[] | undefined,
  ): readonly string[] | undefined {
    let pk = upstreamPk
    for (const t of transforms) {
      if (t.component === "Aggregate") {
        pk = t.props.groupBy as readonly string[]
      } else if (t.kind === "Window") {
        // Extract PK from Aggregate child inside the window
        const aggChild = t.children?.find((c) => c.component === "Aggregate")
        if (aggChild) {
          pk = aggChild.props.groupBy as readonly string[]
        }
      } else if (t.component === "Rename" && pk) {
        const columns = t.props.columns as Record<string, string>
        pk = pk.map((col) => columns[col] ?? col)
      } else if (t.component === "Drop" && pk) {
        const dropCols = new Set(t.props.columns as readonly string[])
        if (pk.some((col) => dropCols.has(col))) {
          pk = undefined
        }
      }
    }
    return pk
  }

  function walk(node: ConstructNode, parent?: ConstructNode): void {
    if (node.kind === "Sink") {
      // Pattern 1: sink has children (reverse-nesting)
      if (node.children.length > 0) {
        const upstream = node.children[0]
        const schema = resolveNodeSchema(upstream, ctx.nodeIndex)
        // Walk to source to get changelog mode
        const sourceNode = findDeepestSource(upstream)
        const changelogMode = sourceNode
          ? propagateChangelogMode(
              collectTransformChain(node.children[0]),
              resolveSourceChangelogMode(sourceNode),
            )
          : "append-only"
        const primaryKey = sourceNode
          ? propagatePrimaryKey(
              collectTransformChain(node.children[0]),
              resolveSourcePrimaryKey(sourceNode),
            )
          : undefined
        if (schema) result.set(node.id, { schema, changelogMode, primaryKey })
        return
      }
      // Pattern 2: forward-reading JSX — resolve from preceding siblings
      if (parent) {
        const sinkIndex = parent.children.indexOf(node)
        let schema: ResolvedColumn[] | null = null
        let changelogMode: ChangelogMode = "append-only"
        let primaryKey: readonly string[] | undefined
        for (let i = sinkIndex - 1; i >= 0; i--) {
          const sibling = parent.children[i]
          // Start from Sources, or from self-contained nodes that embed their
          // own source data (e.g. Union with source children, LookupJoin).
          // Window nodes with only Aggregate children are NOT self-contained —
          // they need upstream data and are applied as intermediate transforms.
          if (sibling.kind === "Source") {
            // Sources always resolve
          } else if (sibling.kind === "RawSQL") {
            // RawSQL is source-like — declares its own outputSchema
          } else if (
            (sibling.kind === "Transform" ||
              sibling.kind === "Window" ||
              sibling.kind === "Join" ||
              sibling.kind === "CEP") &&
            sibling.children.length > 0 &&
            findDeepestSource(sibling) !== null
          ) {
            // Self-contained node (has its own source data)
          } else {
            continue
          }
          const startSchema = resolveNodeSchema(sibling, ctx.nodeIndex)
          if (!startSchema) continue
          schema = startSchema
          if (sibling.kind === "Source") {
            changelogMode = resolveSourceChangelogMode(sibling)
            primaryKey = resolveSourcePrimaryKey(sibling)
          } else if (sibling.kind === "Join") {
            // Anti/semi joins produce retract (result changes when right side changes)
            const joinType = sibling.props.type as string | undefined
            if (joinType === "anti" || joinType === "semi") {
              changelogMode = "retract"
            }
            // Inner/left/right/full joins are retract when either input is retract
            // (e.g. CDC / upsert-kafka source on the broadcast side of a join).
            const left = sibling.children[0]
            const right = sibling.children[1]
            const leftDeepest = left ? findDeepestSource(left) : null
            const rightDeepest = right ? findDeepestSource(right) : null
            if (
              (leftDeepest &&
                resolveSourceChangelogMode(leftDeepest) === "retract") ||
              (rightDeepest &&
                resolveSourceChangelogMode(rightDeepest) === "retract")
            ) {
              changelogMode = "retract"
            }
            // Prefer a same-name join key as the sink's PK (natural key of
            // the joined result). Fall back to the left source's PK.
            const onClause = sibling.props.on as string | undefined
            const sharedKeys = onClause ? sameNameJoinKeys(onClause) : null
            if (sharedKeys) {
              primaryKey = sharedKeys
            } else if (leftDeepest) {
              primaryKey = resolveSourcePrimaryKey(leftDeepest)
            }
          }
          // Propagate through intermediate transforms
          const transforms: ConstructNode[] = []
          for (let j = i + 1; j < sinkIndex; j++) {
            const transform = parent.children[j]
            if (
              transform.kind === "Transform" ||
              transform.kind === "Window" ||
              transform.kind === "RawSQL"
            ) {
              transforms.push(transform)
              if (schema) schema = resolveTransformSchema(transform, schema)
            }
          }
          changelogMode = propagateChangelogMode(transforms, changelogMode)
          primaryKey = propagatePrimaryKey(transforms, primaryKey)
          break
        }
        if (schema) result.set(node.id, { schema, changelogMode, primaryKey })
      }
      return
    }

    if (node.component === "Route") {
      // Resolve upstream schema for the Route using the same backward walk
      // as sink resolution — find the Source and propagate through transforms.
      let routeSchema: ResolvedColumn[] | null = null
      let baseChangelogMode: ChangelogMode = "append-only"
      let basePk: readonly string[] | undefined
      if (parent) {
        const routeIndex = parent.children.indexOf(node)
        for (let i = routeIndex - 1; i >= 0; i--) {
          const sibling = parent.children[i]
          if (sibling.kind === "Source") {
            // Source — resolve directly
          } else if (sibling.kind === "RawSQL") {
            // RawSQL is source-like — declares its own outputSchema
          } else if (
            (sibling.kind === "Transform" ||
              sibling.kind === "Window" ||
              sibling.kind === "Join" ||
              sibling.kind === "CEP") &&
            sibling.children.length > 0 &&
            findDeepestSource(sibling) !== null
          ) {
            // Self-contained node
          } else {
            continue
          }
          const startSchema = resolveNodeSchema(sibling, ctx.nodeIndex)
          if (!startSchema) continue
          routeSchema = startSchema
          if (sibling.kind === "Source") {
            baseChangelogMode = resolveSourceChangelogMode(sibling)
            basePk = resolveSourcePrimaryKey(sibling)
          } else if (sibling.kind === "Join") {
            const joinType = sibling.props.type as string | undefined
            if (joinType === "anti" || joinType === "semi") {
              baseChangelogMode = "retract"
            }
            const leftSource = findDeepestSource(sibling)
            if (leftSource) basePk = resolveSourcePrimaryKey(leftSource)
          }
          // Propagate through intermediate transforms between start and Route
          const transforms: ConstructNode[] = []
          for (let j = i + 1; j < routeIndex; j++) {
            const transform = parent.children[j]
            if (
              transform.kind === "Transform" ||
              transform.kind === "Window" ||
              transform.kind === "RawSQL"
            ) {
              transforms.push(transform)
              if (routeSchema)
                routeSchema = resolveTransformSchema(transform, routeSchema)
            }
          }
          baseChangelogMode = propagateChangelogMode(
            transforms,
            baseChangelogMode,
          )
          basePk = propagatePrimaryKey(transforms, basePk)
          break
        }
      }

      // Resolve metadata for each sink in each branch
      const branches = node.children.filter(
        (c) =>
          c.component === "Route.Branch" || c.component === "Route.Default",
      )
      for (const branch of branches) {
        const transforms = branch.children.filter((c) => c.kind !== "Sink")
        const sinks = branch.children.filter((c) => c.kind === "Sink")

        // Propagate schema through transforms
        let schema = routeSchema
        for (const transform of transforms) {
          if (!schema) break
          schema = resolveTransformSchema(transform, schema)
        }

        const changelogMode = propagateChangelogMode(
          transforms,
          baseChangelogMode,
        )
        const primaryKey = propagatePrimaryKey(transforms, basePk)

        if (schema) {
          for (const sink of sinks) {
            result.set(sink.id, { schema, changelogMode, primaryKey })
          }
        }
      }
      return
    }

    // Recurse
    for (const child of node.children) {
      walk(child, node)
    }
  }

  walk(pipelineNode)
  return result
}

// ── DML generation ──────────────────────────────────────────────────

/*
 * The construct tree uses parent→child ordering where the DOWNSTREAM
 * node is the parent. For example:
 *
 *   Pipeline
 *     └── KafkaSink (children: [Filter])
 *           └── Filter (children: [KafkaSource])
 *                 └── KafkaSource
 *
 * To build SQL we walk from each Sink node DOWN through its children
 * (toward sources) to assemble the SELECT expression.
 */

/** A DML statement with its fragment contributors. */
interface DmlEntry {
  sql: string
  contributors: SqlFragment[]
}

function generateDml(
  ctx: BuildContext,
  pipelineNode: ConstructNode,
  sinkMeta?: Map<string, SinkMetadata>,
): DmlEntry[] {
  const entries: DmlEntry[] = []

  // Collect every watermark column declared on any Source in this pipeline.
  // Used at the sink boundary (BUG-029) to detect when the INSERT's query
  // carries more than one rowtime attribute — Flink rejects that; wrap the
  // INSERT with CASTs to demote all-but-one.
  const rowtimeCols = new Set<string>()
  for (const n of ctx.nodeIndex.values()) {
    if (n.kind !== "Source") continue
    const wm = (
      n.props.schema as { watermark?: { column: string } } | undefined
    )?.watermark?.column
    if (wm) rowtimeCols.add(wm)
  }

  collectSinkDml(ctx, pipelineNode, entries, undefined, sinkMeta, rowtimeCols)

  return entries
}

/**
 * When the sink's resolved schema contains two or more columns that are
 * watermarked rowtime attributes on any upstream source, Flink rejects
 * the INSERT with "more than one rowtime attribute column" (BUG-029).
 *
 * Wrap the query so that each rowtime column but the first (in schema
 * order) is emitted as `CAST(col AS <type>) AS col`, demoting it to plain
 * TIMESTAMP without rowtime metadata. Returns the original query unchanged
 * when fewer than two rowtime columns reach the sink.
 */
function wrapSinkQueryForMultiRowtime(
  sinkQuery: string,
  sinkSchema: readonly { name: string; type: string }[] | undefined,
  rowtimeCols: ReadonlySet<string>,
): string {
  if (!sinkSchema || sinkSchema.length === 0) return sinkQuery
  const rowtimesInSink = sinkSchema
    .map((c) => c.name)
    .filter((n) => rowtimeCols.has(n))
  if (rowtimesInSink.length < 2) return sinkQuery
  // Keep the first rowtime in schema order; demote the rest.
  const demote = new Set(rowtimesInSink.slice(1))
  const cols = sinkSchema.map((c) =>
    demote.has(c.name)
      ? `CAST(${q(c.name)} AS ${c.type}) AS ${q(c.name)}`
      : q(c.name),
  )
  return `SELECT ${cols.join(", ")} FROM (\n${sinkQuery}\n)`
}

/**
 * Build a query by chaining preceding siblings in forward-reading JSX order.
 *
 * In `<Pipeline><Source/><Filter/><KafkaSink/></Pipeline>`, the siblings
 * before the sink form a linear chain: Source → Filter → (sink).
 * Each transform operates on the output of the preceding sibling.
 * We use VirtualRef to temporarily inject upstream into childless transforms.
 */
function buildSiblingChainQuery(
  ctx: BuildContext,
  parent: ConstructNode,
  sinkIndex: number,
): string {
  // Collect preceding siblings that form the data chain.
  //
  // Under <StatementSet>, siblings interleave independent (Source, …, Sink)
  // pairs — each sink must bind to its nearest preceding Source, not the
  // first one in the parent. Start the chain at the most recent preceding
  // Source; in the common single-pair case this is idx 0, so behavior is
  // unchanged.
  let chainStart = 0
  for (let i = sinkIndex - 1; i >= 0; i--) {
    const k = parent.children[i].kind
    // RawSQL is source-like for chain-start purposes — a self-contained
    // SQL body opens a new chain segment just like a Source does.
    if (k === "Source" || k === "RawSQL") {
      chainStart = i
      break
    }
  }
  const chain: ConstructNode[] = []
  for (let i = chainStart; i < sinkIndex; i++) {
    const sibling = parent.children[i]
    if (
      sibling.kind === "Source" ||
      sibling.kind === "Transform" ||
      sibling.kind === "Window" ||
      sibling.kind === "Join" ||
      sibling.kind === "CEP" ||
      sibling.kind === "RawSQL"
    ) {
      chain.push(sibling)
    }
  }

  if (chain.length === 0) return "SELECT * FROM unknown"

  // If only a source, just reference it
  if (chain.length === 1 && chain[0].kind === "Source") {
    return buildQuery(ctx, chain[0])
  }

  // Chain: first element is the source, rest are transforms
  let currentUpstream: { sql: string; sourceRef: string; isSimple: boolean } = {
    sql: buildQuery(ctx, chain[0]),
    sourceRef: chain[0].kind === "Source" ? q(chain[0].id) : chain[0].id,
    isSimple: chain[0].kind === "Source",
  }

  // Track schema through the chain for schema-aware transforms (Rename, Drop, Cast, etc.)
  let chainSchema: ResolvedColumn[] | null =
    chain[0].kind === "Source" || chain[0].kind === "RawSQL"
      ? resolveNodeSchema(chain[0], ctx.nodeIndex)
      : null

  const chainFragStart = ctx.fragments?.length ?? 0

  for (let i = 1; i < chain.length; i++) {
    const transform = chain[i]
    const prevSql = currentUpstream.sql
    const fragBeforeIter = ctx.fragments?.length ?? 0

    const rawRef = currentUpstream.sourceRef.replace(/^`|`$/g, "")

    // Attach _schema to VirtualRef so schema-aware transforms can resolve upstream columns
    const virtualProps: Record<string, unknown> = {}
    if (!currentUpstream.isSimple) virtualProps._sql = currentUpstream.sql
    if (chainSchema) virtualProps._schema = chainSchema

    const virtualSource: ConstructNode = currentUpstream.isSimple
      ? {
          id: rawRef,
          kind: "Source",
          component: "VirtualRef",
          props: virtualProps,
          children: [],
        }
      : {
          id: `_subquery_${transform.id}`,
          kind: "Source",
          component: "VirtualRef",
          props: virtualProps,
          children: [],
        }

    // For Window nodes with children (e.g. TumbleWindow wrapping Aggregate),
    // add VirtualRef as a direct child of the Window alongside the Aggregate.
    // buildWindowQuery looks for a non-Aggregate child as `sourceChild`.
    const injectionTarget = transform
    let injectionMode: "replace" | "append" = "replace"
    if (transform.kind === "Window" && transform.children.length > 0) {
      injectionMode = "append"
    } else if (transform.children.length > 0) {
      // Non-Window transform already has children (reverse-nesting) — just build it normally
      currentUpstream = {
        sql: buildQuery(ctx, transform),
        sourceRef: transform.id,
        isSimple: false,
      }
      chainSchema = resolveNodeSchema(transform, ctx.nodeIndex)
      continue
    }

    const savedChildren = injectionTarget.children
    if (injectionMode === "append") {
      // Add VirtualRef alongside existing children (e.g. Window's Aggregate)
      ;(injectionTarget as { children: ConstructNode[] }).children = [
        ...savedChildren,
        virtualSource,
      ]
    } else {
      ;(injectionTarget as { children: ConstructNode[] }).children = [
        virtualSource,
      ]
    }
    const sql = buildQuery(ctx, transform)
    ;(injectionTarget as { children: ConstructNode[] }).children = savedChildren

    // Shift fragments from previous iterations to account for embedding.
    // When the previous SQL is wrapped inside the new query (e.g. as a subquery),
    // earlier fragments need their offsets adjusted to where prevSql appears in sql.
    if (
      !currentUpstream.isSimple &&
      ctx.fragments &&
      fragBeforeIter > chainFragStart
    ) {
      const embedIdx = sql.indexOf(prevSql)
      if (embedIdx >= 0) {
        for (let f = chainFragStart; f < fragBeforeIter; f++) {
          ctx.fragments[f] = {
            ...ctx.fragments[f],
            offset: ctx.fragments[f].offset + embedIdx,
          }
        }
      }
    }

    // Propagate schema through the transform
    if (chainSchema) {
      chainSchema = resolveTransformSchema(transform, chainSchema)
    }

    currentUpstream = { sql, sourceRef: transform.id, isSimple: false }
  }

  return currentUpstream.sql
}

function collectSinkDml(
  ctx: BuildContext,
  node: ConstructNode,
  entries: DmlEntry[],
  parent?: ConstructNode,
  sinkMeta?: Map<string, SinkMetadata>,
  rowtimeCols?: ReadonlySet<string>,
): void {
  if (node.kind === "Sink") {
    const sinkRef = resolveSinkRef(node)

    // Collect fragments during query building
    const fragments = beginFragmentCollection(ctx)
    let upstream: string
    if (node.children.length > 0) {
      // Pattern 1: reverse-nesting — sink wraps its upstream as children
      upstream = buildQuery(ctx, node.children[0])
    } else if (parent) {
      // Pattern 2: forward-reading JSX — sink is a sibling of its upstream
      const sinkIndex = parent.children.indexOf(node)
      upstream = buildSiblingChainQuery(ctx, parent, sinkIndex)
    } else {
      upstream = "SELECT * FROM unknown"
    }
    endFragmentCollection(ctx)

    // FileSystemSink with function-based partitionBy: wrap the query
    // to add computed partition columns (e.g. CAST(event_time AS DATE) AS dt)
    if (node.component === "FileSystemSink" && node.props.partitionBy) {
      const rawPartCols = node.props.partitionBy as readonly string[]
      const partProjections: string[] = []
      for (const expr of rawPartCols) {
        const funcMatch = expr.match(/^(\w+)\((\w+)\)$/i)
        if (funcMatch) {
          const [, func, col] = funcMatch
          const resolved = resolvePartitionExpression(func, col)
          partProjections.push(`${resolved.expr} AS ${q(resolved.name)}`)
        }
      }
      if (partProjections.length > 0) {
        upstream = `SELECT *, ${partProjections.join(", ")} FROM (\n${upstream}\n)`
      }
    }

    if (rowtimeCols) {
      upstream = wrapSinkQueryForMultiRowtime(
        upstream,
        sinkMeta?.get(node.id)?.schema,
        rowtimeCols,
      )
    }

    const insertPrefix = `INSERT INTO ${sinkRef}\n`
    const fullSql = `${insertPrefix}${upstream};`

    // Shift all query fragments by the INSERT INTO prefix length
    const contributors: SqlFragment[] = fragments.map((f) => ({
      ...f,
      offset: f.offset + insertPrefix.length,
    }))
    // Add the INSERT INTO target as a Sink fragment
    contributors.unshift({
      offset: 0,
      length: insertPrefix.length - 1, // exclude the trailing \n
      origin: { nodeId: node.id, component: node.component, kind: node.kind },
    })

    entries.push({ sql: fullSql, contributors })

    // Recurse into upstream to find SideOutput/Validate side-path DML
    for (const child of node.children) {
      collectSideDml(ctx, child, entries)
    }
    return
  }

  // Route generates multiple INSERT statements (one per branch)
  if (node.component === "Route") {
    collectRouteDml(ctx, node, entries, parent, sinkMeta, rowtimeCols)
    return
  }

  // StatementSet is a transparent passthrough — recurse into children
  if (node.component === "StatementSet") {
    for (const child of node.children) {
      collectSinkDml(ctx, child, entries, node, sinkMeta, rowtimeCols)
    }
    return
  }

  // SideOutput generates two INSERT statements (main + side)
  if (node.component === "SideOutput") {
    collectSideOutputDml(ctx, node, entries)
    return
  }

  // Validate generates two INSERT statements (valid + rejected)
  if (node.component === "Validate") {
    collectValidateDml(ctx, node, entries)
    return
  }

  // Recurse into children to find sinks
  for (const child of node.children) {
    collectSinkDml(ctx, child, entries, node, sinkMeta, rowtimeCols)
  }
}

/**
 * Find Route's upstream data source.
 *
 * Strategy:
 * 1. Look for non-Branch/Default children of Route (programmatic API pattern)
 * 2. If none found, look at preceding siblings in the parent (JSX forward-reading pattern)
 */
function resolveRouteUpstream(
  ctx: BuildContext,
  routeNode: ConstructNode,
  parent?: ConstructNode,
): { sql: string; sourceRef: string; isSimple: boolean } {
  // Pattern 1: upstream is a direct non-Branch child of Route
  const upstreamChildren = routeNode.children.filter(
    (c) => c.component !== "Route.Branch" && c.component !== "Route.Default",
  )
  if (upstreamChildren.length > 0) {
    const up = getUpstream(ctx, { children: upstreamChildren } as ConstructNode)
    return up
  }

  // Pattern 2: upstream is a preceding sibling chain in parent (JSX forward-reading)
  // Build the full chain (e.g. Source → Map) rather than just the nearest sibling.
  if (parent) {
    const routeIndex = parent.children.indexOf(routeNode)
    const chainSql = buildSiblingChainQuery(ctx, parent, routeIndex)
    if (chainSql !== "SELECT * FROM unknown") {
      // Check if the chain is a simple table reference
      const simpleMatch = chainSql.match(/^SELECT \* FROM (`[^`]+`)$/)
      if (simpleMatch) {
        return { sql: chainSql, sourceRef: simpleMatch[1], isSimple: true }
      }
      return { sql: chainSql, sourceRef: "unknown", isSimple: false }
    }
  }

  return { sql: "SELECT * FROM unknown", sourceRef: "unknown", isSimple: false }
}

/**
 * Build a query chain through transforms within a Route.Branch.
 *
 * Branch children are in forward reading order: [Transform1, Transform2, ..., Sink].
 * Data flow: source → (branch condition) → Transform1 → Transform2 → ... → Sink
 *
 * When the first transform is a Filter, the branch condition is merged into
 * the Filter's WHERE clause with AND for cleaner output. For aggregation
 * branches, the branch condition becomes a WHERE before the GROUP BY.
 */
function buildBranchQuery(
  ctx: BuildContext,
  transforms: readonly ConstructNode[],
  upstream: { sql: string; sourceRef: string; isSimple: boolean },
  branchCondition: string | undefined,
): string {
  // No transforms, no condition: just reference the source
  if (transforms.length === 0 && !branchCondition) {
    return upstream.isSimple
      ? `SELECT * FROM ${upstream.sourceRef}`
      : upstream.sql
  }

  // No transforms, just branch condition
  if (transforms.length === 0 && branchCondition) {
    return upstream.isSimple
      ? `SELECT * FROM ${upstream.sourceRef}\nWHERE ${branchCondition}`
      : `SELECT * FROM (\n${upstream.sql}\n)\nWHERE ${branchCondition}`
  }

  // Has transforms — chain them with the source as starting point.
  // The branch condition applies BEFORE transforms (it selects which rows go to this branch).

  // Optimization: if first transform is Filter, merge branch condition into Filter's WHERE
  const firstTransform = transforms[0]
  let startIndex = 0
  let currentUpstream = upstream

  if (branchCondition && firstTransform.component === "Filter") {
    // Merge: Filter WHERE <filterCondition> AND <branchCondition>
    const filterCondition = firstTransform.props.condition as string
    const mergedSql = upstream.isSimple
      ? `SELECT * FROM ${upstream.sourceRef}\nWHERE ${filterCondition} AND ${branchCondition}`
      : `SELECT * FROM (\n${upstream.sql}\n)\nWHERE ${filterCondition} AND ${branchCondition}`
    currentUpstream = {
      sql: mergedSql,
      sourceRef: upstream.sourceRef,
      isSimple: false,
    }
    startIndex = 1 // skip the first Filter since we already applied it
  } else if (branchCondition) {
    // Apply branch condition as a WHERE on the source, before transforms
    const filteredSql = upstream.isSimple
      ? `SELECT * FROM ${upstream.sourceRef}\nWHERE ${branchCondition}`
      : `SELECT * FROM (\n${upstream.sql}\n)\nWHERE ${branchCondition}`
    currentUpstream = {
      sql: filteredSql,
      sourceRef: upstream.sourceRef,
      isSimple: false,
    }
  }

  // Track schema through the chain for schema-aware transforms
  let chainSchema: ResolvedColumn[] | null = null
  if (currentUpstream.isSimple) {
    const rawRef = currentUpstream.sourceRef.replace(/^`|`$/g, "")
    const upstreamNode = ctx.nodeIndex.get(rawRef)
    if (upstreamNode)
      chainSchema = resolveNodeSchema(upstreamNode, ctx.nodeIndex)
  }

  // Chain remaining transforms using VirtualRef for temporary child injection
  const branchFragStart = ctx.fragments?.length ?? 0

  for (let i = startIndex; i < transforms.length; i++) {
    const transform = transforms[i]
    const prevSql = currentUpstream.sql
    const fragBeforeIter = ctx.fragments?.length ?? 0
    const savedChildren = transform.children

    // Strip backticks from sourceRef to avoid double-quoting when q() is applied
    const rawRef = currentUpstream.sourceRef.replace(/^`|`$/g, "")

    const virtualProps: Record<string, unknown> = {}
    if (!currentUpstream.isSimple) virtualProps._sql = currentUpstream.sql
    if (chainSchema) virtualProps._schema = chainSchema

    const virtualSource: ConstructNode = currentUpstream.isSimple
      ? {
          id: rawRef,
          kind: "Source",
          component: "VirtualRef",
          props: virtualProps,
          children: [],
        }
      : {
          id: `_subquery_${transform.id}`,
          kind: "Source",
          component: "VirtualRef",
          props: virtualProps,
          children: [],
        }

    // For Window nodes with children (e.g. Aggregate), append VirtualRef
    // alongside existing children instead of replacing them.
    if (transform.kind === "Window" && savedChildren.length > 0) {
      ;(transform as { children: ConstructNode[] }).children = [
        ...savedChildren,
        virtualSource,
      ]
    } else {
      ;(transform as { children: ConstructNode[] }).children = [virtualSource]
    }
    const sql = buildQuery(ctx, transform)
    ;(transform as { children: ConstructNode[] }).children = savedChildren

    // Shift fragments from previous iterations to account for embedding
    if (
      !currentUpstream.isSimple &&
      ctx.fragments &&
      fragBeforeIter > branchFragStart
    ) {
      const embedIdx = sql.indexOf(prevSql)
      if (embedIdx >= 0) {
        for (let f = branchFragStart; f < fragBeforeIter; f++) {
          ctx.fragments[f] = {
            ...ctx.fragments[f],
            offset: ctx.fragments[f].offset + embedIdx,
          }
        }
      }
    }

    // Propagate schema through the transform
    if (chainSchema) {
      chainSchema = resolveTransformSchema(transform, chainSchema)
    }

    currentUpstream = { sql, sourceRef: transform.id, isSimple: false }
  }

  return currentUpstream.sql
}

function collectRouteDml(
  ctx: BuildContext,
  routeNode: ConstructNode,
  entries: DmlEntry[],
  parent?: ConstructNode,
  sinkMeta?: Map<string, SinkMetadata>,
  rowtimeCols?: ReadonlySet<string>,
): void {
  const branches = routeNode.children.filter(
    (c) => c.component === "Route.Branch" || c.component === "Route.Default",
  )

  const upstream = resolveRouteUpstream(ctx, routeNode, parent)

  for (const branch of branches) {
    // Normalize: treat condition "true" as no condition (it selects all rows)
    const rawCondition = branch.props.condition as string | undefined
    const condition =
      rawCondition?.trim().toLowerCase() === "true" ? undefined : rawCondition

    // Separate transforms and sinks within the branch
    const transforms = branch.children.filter((c) => c.kind !== "Sink")
    const sinks = branch.children.filter((c) => c.kind === "Sink")

    // Collect fragments during branch query building
    const fragments = beginFragmentCollection(ctx)
    const query = buildBranchQuery(ctx, transforms, upstream, condition)
    endFragmentCollection(ctx)

    // A branch without an `Aggregate` but with a passthrough window
    // upstream generates `SELECT * FROM TABLE(TUMBLE/HOP/SESSION(...))`,
    // which materializes a `window_time` ROWTIME column in addition to
    // `window_start`/`window_end`. Sinks inferred from the chain only
    // expect the latter two, and Flink additionally rejects INSERTs with
    // two ROWTIME attributes (source watermark + window_time). Project
    // the sink's resolved columns explicitly so `window_time` is dropped.
    // `TumbleWindow → Aggregate` (reverse-nesting) already projects
    // explicitly via `buildWindowQuery`, so count a Window with an
    // Aggregate child as a projection too.
    const branchHasAggregate = transforms.some(
      (t) =>
        t.component === "Aggregate" ||
        (t.kind === "Window" &&
          t.children.some((c) => c.component === "Aggregate")),
    )

    for (const sink of sinks) {
      const sinkRef = resolveSinkRef(sink)
      let sinkQuery = query

      if (!branchHasAggregate) {
        const sinkSchema = sinkMeta?.get(sink.id)?.schema
        if (
          sinkSchema &&
          sinkSchema.length > 0 &&
          sinkSchema.some((c) => c.name === "window_start") &&
          sinkSchema.every((c) => c.name !== "window_time")
        ) {
          const colList = sinkSchema.map((c) => q(c.name)).join(", ")
          sinkQuery = `SELECT ${colList} FROM (\n${sinkQuery}\n)`
        }
      }

      // FileSystemSink with function-based partitionBy: wrap the query
      // to add computed partition columns
      if (sink.component === "FileSystemSink" && sink.props.partitionBy) {
        const rawPartCols = sink.props.partitionBy as readonly string[]
        const partProjections: string[] = []
        for (const expr of rawPartCols) {
          const funcMatch = expr.match(/^(\w+)\((\w+)\)$/i)
          if (funcMatch) {
            const [, func, col] = funcMatch
            const resolved = resolvePartitionExpression(func, col)
            partProjections.push(`${resolved.expr} AS ${q(resolved.name)}`)
          }
        }
        if (partProjections.length > 0) {
          sinkQuery = `SELECT *, ${partProjections.join(", ")} FROM (\n${sinkQuery}\n)`
        }
      }

      if (rowtimeCols) {
        sinkQuery = wrapSinkQueryForMultiRowtime(
          sinkQuery,
          sinkMeta?.get(sink.id)?.schema,
          rowtimeCols,
        )
      }

      const insertPrefix = `INSERT INTO ${sinkRef}\n`
      const fullSql = `${insertPrefix}${sinkQuery};`

      const contributors: SqlFragment[] = fragments.map((f) => ({
        ...f,
        offset: f.offset + insertPrefix.length,
      }))
      contributors.unshift({
        offset: 0,
        length: insertPrefix.length - 1,
        origin: {
          nodeId: sink.id,
          component: sink.component,
          kind: sink.kind,
        },
      })

      entries.push({ sql: fullSql, contributors })
    }
  }
}

function resolveSinkRef(sink: ConstructNode): string {
  if (sink.props.catalogName) {
    return `${q(String(sink.props.catalogName))}.${q(String(sink.props.database))}.${q(String(sink.props.table))}`
  }
  return q(sink.id)
}

// ── Query building ──────────────────────────────────────────────────

/*
 * buildQuery walks a node and its children (toward sources) to
 * produce a SQL SELECT expression. Each transform type adds its
 * own SQL clause on top of the upstream query.
 */

function buildQuery(ctx: BuildContext, node: ConstructNode): string {
  // Check plugin SQL generators first (allows overriding built-in components)
  const pluginGen = ctx.pluginSqlGenerators?.get(node.component)
  if (pluginGen) {
    return pluginGen(node, ctx.nodeIndex)
  }

  switch (node.component) {
    // Virtual reference (used internally by Route branch chaining)
    case "VirtualRef":
      if (node.props._sql) return node.props._sql as string
      return `SELECT * FROM ${q(node.id)}`

    // Sources — terminal unless they have child transforms
    case "KafkaSource":
    case "JdbcSource":
    case "GenericSource":
    case "DataGenSource":
    case "FlussSource":
      if (node.children.length > 0) {
        // Source wraps child transforms (e.g. <KafkaSource><Map/></KafkaSource>)
        // Build query by chaining children on top of the source reference.
        let result = `SELECT * FROM ${q(node.id)}`
        for (const child of node.children) {
          const virtualSource: ConstructNode = {
            id: node.id,
            kind: "Source",
            component: "VirtualRef",
            props: {},
            children: [],
          }
          const saved = child.children
          ;(child as { children: ConstructNode[] }).children = [virtualSource]
          result = buildQuery(ctx, child)
          ;(child as { children: ConstructNode[] }).children = saved
        }
        return result
      }
      return `SELECT * FROM ${q(node.id)}`
    case "CatalogSource":
      return `SELECT * FROM ${q(String(node.props.catalogName))}.${q(String(node.props.database))}.${q(String(node.props.table))}`

    // Transforms
    case "Filter":
      return buildFilterQuery(ctx, node)
    case "Map":
      return buildMapQuery(ctx, node)
    case "FlatMap":
      return buildFlatMapQuery(ctx, node)
    case "Aggregate":
      return buildAggregateQuery(ctx, node)
    case "Union":
      return buildUnionQuery(ctx, node)
    case "Deduplicate":
      return buildDeduplicateQuery(ctx, node)
    case "TopN":
      return buildTopNQuery(ctx, node)

    // Field transforms
    case "Rename":
      return buildRenameQuery(ctx, node)
    case "Drop":
      return buildDropQuery(ctx, node)
    case "Cast":
      return buildCastQuery(ctx, node)
    case "Coalesce":
      return buildCoalesceQuery(ctx, node)
    case "AddField":
      return buildAddFieldQuery(ctx, node)

    // Joins
    case "Join":
      return buildJoinQuery(ctx, node)
    case "TemporalJoin":
      return buildTemporalJoinQuery(ctx, node)
    case "LookupJoin":
      return buildLookupJoinQuery(ctx, node)
    case "IntervalJoin":
      return buildIntervalJoinQuery(ctx, node)

    // Windows
    case "TumbleWindow":
    case "SlideWindow":
    case "SessionWindow":
      return buildWindowQuery(ctx, node)

    // Escape hatches
    case "Query":
      return buildQueryComponentQuery(ctx, node)
    case "RawSQL":
      return buildRawSqlQuery(node)
    case "Qualify":
      return buildQualifyQuery(ctx, node)

    // CEP
    case "MatchRecognize":
      return buildMatchRecognizeQuery(ctx, node)

    // Validate — main path (valid records)
    case "Validate":
      return buildValidateQuery(ctx, node)

    // View — reference by name
    case "View":
      return `SELECT * FROM ${q(node.props.name as string)}`

    // SideOutput — main path (non-matching records)
    case "SideOutput":
      return buildSideOutputQuery(ctx, node)

    // LateralJoin — LATERAL TABLE TVF join
    case "LateralJoin":
      return buildLateralJoinQuery(ctx, node)

    default:
      // Unknown — try first child
      if (node.children.length > 0) {
        return buildQuery(ctx, node.children[0])
      }
      return `SELECT * FROM ${q(node.id)}`
  }
}

/**
 * Get the upstream SQL and source table reference from a node's children.
 */
function getUpstream(
  ctx: BuildContext,
  node: ConstructNode,
): {
  sql: string
  sourceRef: string
  isSimple: boolean
} {
  if (node.children.length === 0) {
    return {
      sql: "SELECT * FROM unknown",
      sourceRef: "unknown",
      isSimple: false,
    }
  }

  const child = node.children[0]

  // VirtualRef: either a simple table reference or an embedded subquery
  if (child.component === "VirtualRef") {
    if (child.props._sql) {
      return {
        sql: child.props._sql as string,
        sourceRef: child.id,
        isSimple: false,
      }
    }
    return {
      sql: `SELECT * FROM ${q(child.id)}`,
      sourceRef: q(child.id),
      isSimple: true,
    }
  }

  // If the child is a source and no plugin overrides it, return a simple reference
  if (
    child.kind === "Source" &&
    !ctx.pluginSqlGenerators?.has(child.component)
  ) {
    const ref =
      child.component === "CatalogSource"
        ? `${q(String(child.props.catalogName))}.${q(String(child.props.database))}.${q(String(child.props.table))}`
        : q(child.id)
    return { sql: `SELECT * FROM ${ref}`, sourceRef: ref, isSimple: true }
  }

  // Otherwise build the child query
  const childSql = buildQuery(ctx, child)
  return { sql: childSql, sourceRef: child.id, isSimple: false }
}

// ── Filter ──────────────────────────────────────────────────────────

function buildFilterQuery(ctx: BuildContext, node: ConstructNode): string {
  const condition = node.props.condition as string
  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)

  const wherePart = ` WHERE ${condition}`

  if (upstream.isSimple) {
    const result = `SELECT * FROM ${upstream.sourceRef}${wherePart}`
    pushFragment(ctx, result.length - wherePart.length, wherePart.length, node)
    return result
  }
  const prefix = `SELECT * FROM (\n`
  const result = `${prefix}${upstream.sql}\n)${wherePart}`
  shiftFragmentsSince(ctx, fragStart, prefix.length)
  pushFragment(ctx, result.length - wherePart.length, wherePart.length, node)
  return result
}

// ── Map ─────────────────────────────────────────────────────────────

function buildMapQuery(ctx: BuildContext, node: ConstructNode): string {
  const select = node.props.select as Record<string, string>
  const projections = Object.entries(select)
    .map(([alias, expr]) => `${expr} AS ${q(alias)}`)
    .join(", ")

  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)

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

// ── FlatMap ─────────────────────────────────────────────────────────

function buildFlatMapQuery(ctx: BuildContext, node: ConstructNode): string {
  const unnestField = node.props.unnest as string
  const asFields = node.props.as as Record<string, string>
  const aliases = Object.keys(asFields).map(q).join(", ")

  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)
  const ref = upstream.sourceRef

  // sourceRef is already backtick-quoted for simple refs — use directly
  const selectPart = `SELECT ${ref}.*, ${aliases}`
  const fromPart = upstream.isSimple
    ? ref
    : `(\n${upstream.sql}\n) AS ${q(ref)}`
  const crossJoinPart = `CROSS JOIN UNNEST(${ref}.${q(unnestField)}) AS T(${aliases})`
  const result = `${selectPart} FROM ${fromPart} ${crossJoinPart}`

  if (!upstream.isSimple) {
    const prefix = `${selectPart} FROM (\n`
    shiftFragmentsSince(ctx, fragStart, prefix.length)
  }

  // Track: the added aliases in SELECT and the CROSS JOIN UNNEST are FlatMap's contribution
  pushFragment(ctx, 0, selectPart.length, node)
  pushFragment(
    ctx,
    result.length - crossJoinPart.length,
    crossJoinPart.length,
    node,
  )

  return result
}

// ── Aggregate ───────────────────────────────────────────────────────

function buildAggregateQuery(ctx: BuildContext, node: ConstructNode): string {
  // Forward-nesting (Aggregate { children: Window }): emit the single
  // `SELECT … FROM TABLE(TVF(...)) GROUP BY …` pattern by delegating to
  // buildWindowQuery with a pseudo-Window that carries this Aggregate as
  // a child. The default path below would wrap the TVF in a
  // `SELECT * FROM (TABLE(TVF))` subquery, which strips the time-attribute
  // metadata on window_start/window_end and breaks downstream TemporalJoin
  // `FOR SYSTEM_TIME AS OF` validation (BUG-030).
  const firstChild = node.children[0]
  if (
    firstChild &&
    (firstChild.component === "TumbleWindow" ||
      firstChild.component === "SlideWindow" ||
      firstChild.component === "SessionWindow")
  ) {
    const pseudoWindow: ConstructNode = {
      ...firstChild,
      children: [...firstChild.children, node],
    }
    return buildWindowQuery(ctx, pseudoWindow)
  }

  const groupBy = node.props.groupBy as readonly string[]
  const select = node.props.select as Record<string, string>
  const groupBySet = new Set(groupBy)

  // Detect window TVF metadata columns referenced in select expressions.
  // When the upstream is a windowed TVF (TUMBLE/HOP/SESSION), Flink requires
  // window_start and window_end to appear in GROUP BY if used in SELECT.
  const windowMetaCols = ["window_start", "window_end"] as const
  const referencedWindowCols = windowMetaCols.filter((wc) =>
    Object.values(select).some((expr) => expr === wc),
  )

  const groupColParts = groupBy.map(q)
  for (const wc of referencedWindowCols) {
    groupColParts.push(wc)
  }
  const groupCols = groupColParts.join(", ")

  const projections = [
    ...groupBy.map(q),
    // Skip select entries that duplicate a groupBy column
    ...Object.entries(select)
      .filter(([alias]) => !groupBySet.has(alias))
      .map(([alias, expr]) => `${expr} AS ${q(alias)}`),
  ].join(", ")

  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)

  const selectPart = `SELECT ${projections}`
  const groupByPart = ` GROUP BY ${groupCols}`

  if (upstream.isSimple) {
    const result = `${selectPart} FROM ${upstream.sourceRef}${groupByPart}`
    pushFragment(ctx, 0, selectPart.length, node)
    pushFragment(
      ctx,
      result.length - groupByPart.length,
      groupByPart.length,
      node,
    )
    return result
  }
  const prefix = `${selectPart} FROM (\n`
  const result = `${prefix}${upstream.sql}\n)${groupByPart}`
  shiftFragmentsSince(ctx, fragStart, prefix.length)
  pushFragment(ctx, 0, selectPart.length, node)
  pushFragment(
    ctx,
    result.length - groupByPart.length,
    groupByPart.length,
    node,
  )
  return result
}

// ── Union ───────────────────────────────────────────────────────────

function buildUnionQuery(ctx: BuildContext, node: ConstructNode): string {
  const parts: string[] = []

  for (const child of node.children) {
    parts.push(buildQuery(ctx, child))
  }

  if (parts.length === 0) return "SELECT * FROM unknown"
  const result = parts.join("\nUNION ALL\n")

  // Track each "UNION ALL" keyword as a Union fragment
  if (parts.length > 1) {
    let offset = parts[0].length + 1 // +1 for the \n before "UNION ALL"
    for (let i = 1; i < parts.length; i++) {
      pushFragment(ctx, offset, "UNION ALL".length, node)
      offset += "UNION ALL".length + 1 + parts[i].length + 1 // +1 for \n after, +1 for \n before next
    }
  }

  return result
}

// ── Deduplicate ─────────────────────────────────────────────────────

function buildDeduplicateQuery(ctx: BuildContext, node: ConstructNode): string {
  const key = node.props.key as readonly string[]
  const order = node.props.order as string
  const keep = node.props.keep as "first" | "last"

  const partitionBy = key.map(q).join(", ")
  const orderDir = keep === "first" ? "ASC" : "DESC"

  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)
  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  if (FlinkVersionCompat.isVersionAtLeast(ctx.version, "2.0")) {
    const selectPart = `SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${q(order)} ${orderDir}) AS rownum`
    const qualifyPart = "QUALIFY rownum = 1"
    const result = `${selectPart}\nFROM ${fromClause}\n${qualifyPart}`
    if (!upstream.isSimple)
      shiftFragmentsSince(
        ctx,
        fragStart,
        selectPart.length + 1 + "FROM (\n".length,
      )
    pushFragment(ctx, 0, selectPart.length, node)
    pushFragment(
      ctx,
      result.length - qualifyPart.length,
      qualifyPart.length,
      node,
    )
    return result
  }

  const innerSelect = `  SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${q(order)} ${orderDir}) AS rownum`
  const wherePart = ") WHERE rownum = 1"
  const result = `SELECT * FROM (\n${innerSelect}\n  FROM ${fromClause}\n${wherePart}`
  if (!upstream.isSimple)
    shiftFragmentsSince(
      ctx,
      fragStart,
      "SELECT * FROM (\n".length + innerSelect.length + 1 + "  FROM (\n".length,
    )
  pushFragment(ctx, 0, "SELECT * FROM (".length, node)
  pushFragment(ctx, "SELECT * FROM (\n".length, innerSelect.length, node)
  pushFragment(ctx, result.length - wherePart.length, wherePart.length, node)
  return result
}

// ── TopN ────────────────────────────────────────────────────────────

function buildTopNQuery(ctx: BuildContext, node: ConstructNode): string {
  const partitionBy = (node.props.partitionBy as readonly string[])
    .map(q)
    .join(", ")
  const orderBy = node.props.orderBy as Record<string, "ASC" | "DESC">
  const n = node.props.n as number

  const orderClause = Object.entries(orderBy)
    .map(([field, dir]) => `${q(field)} ${dir}`)
    .join(", ")

  const fragStart = ctx.fragments?.length ?? 0
  const upstream = getUpstream(ctx, node)
  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  if (FlinkVersionCompat.isVersionAtLeast(ctx.version, "2.0")) {
    const selectPart = `SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${orderClause}) AS rownum`
    const qualifyPart = `QUALIFY rownum <= ${n}`
    const result = `${selectPart}\nFROM ${fromClause}\n${qualifyPart}`
    if (!upstream.isSimple)
      shiftFragmentsSince(
        ctx,
        fragStart,
        selectPart.length + 1 + "FROM (\n".length,
      )
    pushFragment(ctx, 0, selectPart.length, node)
    pushFragment(
      ctx,
      result.length - qualifyPart.length,
      qualifyPart.length,
      node,
    )
    return result
  }

  const innerSelect = `  SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${orderClause}) AS rownum`
  const wherePart = `) WHERE rownum <= ${n}`
  const result = `SELECT * FROM (\n${innerSelect}\n  FROM ${fromClause}\n${wherePart}`
  if (!upstream.isSimple)
    shiftFragmentsSince(
      ctx,
      fragStart,
      "SELECT * FROM (\n".length + innerSelect.length + 1 + "  FROM (\n".length,
    )
  pushFragment(ctx, 0, "SELECT * FROM (".length, node)
  pushFragment(ctx, "SELECT * FROM (\n".length, innerSelect.length, node)
  pushFragment(ctx, result.length - wherePart.length, wherePart.length, node)
  return result
}

// ── Qualify (escape hatch) ───────────────────────────────────────────

function buildQualifyQuery(ctx: BuildContext, node: ConstructNode): string {
  const condition = node.props.condition as string
  const windowExpr = node.props.window as string | undefined

  const upstream = getUpstream(ctx, node)

  const selectList = windowExpr ? `*, ${windowExpr}` : "*"
  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  return [
    `SELECT ${selectList}`,
    `FROM ${fromClause}`,
    `QUALIFY ${condition}`,
  ].join("\n")
}

// ── Rename ──────────────────────────────────────────────────────────

function buildRenameQuery(ctx: BuildContext, node: ConstructNode): string {
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

function buildDropQuery(ctx: BuildContext, node: ConstructNode): string {
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

function buildCastQuery(ctx: BuildContext, node: ConstructNode): string {
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

function buildCoalesceQuery(ctx: BuildContext, node: ConstructNode): string {
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

function buildAddFieldQuery(ctx: BuildContext, node: ConstructNode): string {
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

// ── Join ────────────────────────────────────────────────────────────

function buildJoinQuery(ctx: BuildContext, node: ConstructNode): string {
  const joinType = (node.props.type as string) ?? "inner"
  const onCondition = node.props.on as string
  const hints = node.props.hints as { broadcast?: "left" | "right" } | undefined
  const leftId = node.props.left as string
  const rightId = node.props.right as string

  const left = resolveJoinOperand(ctx, leftId)
  const right = resolveJoinOperand(ctx, rightId)
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

function buildTemporalJoinQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const onCondition = node.props.on as string
  const asOf = node.props.asOf as string
  const streamId = node.props.stream as string
  const temporalId = node.props.temporal as string

  const stream = resolveJoinOperand(ctx, streamId)
  const temporal = resolveJoinOperand(ctx, temporalId)
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
 * Returns the shared column names when an ON clause joins on the same
 * column on both sides. Accepts three forms:
 *   `col = col`                          — unqualified, common in templates
 *   `left.col = right.col`               — qualified, same column, different qualifiers
 *   `a.c1 = b.c1 AND a.c2 = b.c2 [...]`  — compound of either form above
 *
 * All forms signal "join where both sides share these keys"; the codegen
 * disambiguates by qualifying with side-refs and pruning duplicate
 * projections. Returns null when any sub-clause isn't a same-name equality.
 */
function sameNameJoinKeys(on: string): string[] | null {
  const clauses = on.split(/\s+AND\s+/i)
  const keys: string[] = []
  for (const clause of clauses) {
    const key = sameNameJoinKeyScalar(clause)
    if (!key) return null
    keys.push(key)
  }
  return keys.length > 0 ? keys : null
}

function sameNameJoinKeyScalar(on: string): string | null {
  const unq = on.match(/^\s*([A-Za-z_][\w]*)\s*=\s*([A-Za-z_][\w]*)\s*$/)
  if (unq && unq[1] === unq[2]) return unq[1]
  const qual = on.match(
    /^\s*[A-Za-z_][\w]*\.([A-Za-z_][\w]*)\s*=\s*[A-Za-z_][\w]*\.([A-Za-z_][\w]*)\s*$/,
  )
  if (qual && qual[1] === qual[2]) return qual[1]
  return null
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

function buildLookupJoinQuery(ctx: BuildContext, node: ConstructNode): string {
  const onCondition = node.props.on as string
  const inputId = node.props.input as string
  const table = node.props.table as string
  const select = node.props.select as Record<string, string> | undefined

  const input = resolveJoinOperand(ctx, inputId)

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

function buildIntervalJoinQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const joinType = (node.props.type as string) ?? "inner"
  const onCondition = node.props.on as string
  const interval = node.props.interval as { from: string; to: string }
  const leftId = node.props.left as string
  const rightId = node.props.right as string

  const left = resolveJoinOperand(ctx, leftId)
  const right = resolveJoinOperand(ctx, rightId)
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

// ── Window ──────────────────────────────────────────────────────────

function buildWindowQuery(ctx: BuildContext, node: ConstructNode): string {
  const windowCol = node.props.on as string

  // Find the Aggregate child (if windowed aggregation)
  const aggChild = node.children.find((c) => c.component === "Aggregate")

  // Find the source feeding the window (non-Aggregate child)
  const sourceChild = node.children.find((c) => c.component !== "Aggregate")
  const upstream = sourceChild
    ? getUpstream(ctx, { children: [sourceChild] } as ConstructNode)
    : getUpstream(ctx, node)
  // Flink TVF syntax requires TABLE <identifier>, not TABLE (<subquery>).
  // When the upstream is a subquery, lift it into a CTE so the TVF gets
  // a named table reference.
  let ctePrefix = ""
  let sourceRef: string
  if (upstream.isSimple) {
    sourceRef = upstream.sourceRef
  } else {
    const cteName = `_windowed_input`
    ctePrefix = `WITH ${q(cteName)} AS (\n${upstream.sql}\n)\n`
    sourceRef = q(cteName)
  }

  const tvf = buildWindowTvf(node, sourceRef, windowCol)

  if (!aggChild) {
    return `${ctePrefix}SELECT * FROM TABLE(\n  ${tvf}\n)`
  }

  const groupBy = aggChild.props.groupBy as readonly string[]
  const select = aggChild.props.select as Record<string, string>

  const groupCols = [...groupBy.map(q), "window_start", "window_end"].join(", ")

  const groupBySet = new Set(groupBy)
  const projections = [
    ...groupBy.map(q),
    // Skip select entries that duplicate a groupBy column
    ...Object.entries(select)
      .filter(([alias]) => !groupBySet.has(alias))
      .map(([alias, expr]) => `${expr} AS ${q(alias)}`),
    "window_start",
    "window_end",
  ].join(", ")

  return `${ctePrefix}SELECT ${projections} FROM TABLE(\n  ${tvf}\n) GROUP BY ${groupCols}`
}

function buildWindowTvf(
  node: ConstructNode,
  sourceRef: string,
  windowCol: string,
): string {
  switch (node.component) {
    case "TumbleWindow": {
      const size = toInterval(node.props.size as string)
      return `TUMBLE(TABLE ${sourceRef}, DESCRIPTOR(${q(windowCol)}), ${size})`
    }
    case "SlideWindow": {
      const size = toInterval(node.props.size as string)
      const slide = toInterval(node.props.slide as string)
      return `HOP(TABLE ${sourceRef}, DESCRIPTOR(${q(windowCol)}), ${slide}, ${size})`
    }
    case "SessionWindow": {
      const gap = toInterval(node.props.gap as string)
      return `SESSION(TABLE ${sourceRef}, DESCRIPTOR(${q(windowCol)}), ${gap})`
    }
    default:
      return `UNKNOWN_WINDOW(TABLE ${sourceRef})`
  }
}

// ── Query component ─────────────────────────────────────────────────

const QUERY_CLAUSE_TYPES = new Set([
  "Query.Select",
  "Query.Where",
  "Query.GroupBy",
  "Query.Having",
  "Query.OrderBy",
])

function buildQueryComponentQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  // Separate clause children from upstream data children
  const clauses = node.children.filter((c) =>
    QUERY_CLAUSE_TYPES.has(c.component),
  )
  const upstreamChildren = node.children.filter(
    (c) => !QUERY_CLAUSE_TYPES.has(c.component),
  )

  // Resolve upstream
  const upstream =
    upstreamChildren.length > 0
      ? getUpstream(ctx, { children: upstreamChildren } as ConstructNode)
      : { sql: "SELECT * FROM unknown", sourceRef: "unknown", isSimple: false }

  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  // Find clause nodes
  const selectNode = clauses.find((c) => c.component === "Query.Select")
  const whereNode = clauses.find((c) => c.component === "Query.Where")
  const groupByNode = clauses.find((c) => c.component === "Query.GroupBy")
  const havingNode = clauses.find((c) => c.component === "Query.Having")
  const orderByNode = clauses.find((c) => c.component === "Query.OrderBy")

  // Build SELECT projections
  const columns = selectNode?.props.columns as Record<
    string,
    | string
    | {
        func: string
        args?: readonly (string | number)[]
        window?: string
        over?: {
          partitionBy?: readonly string[]
          orderBy?: Record<string, "ASC" | "DESC">
        }
      }
  >
  const windows = selectNode?.props.windows as
    | Record<
        string,
        {
          partitionBy?: readonly string[]
          orderBy?: Record<string, "ASC" | "DESC">
        }
      >
    | undefined

  const projections = Object.entries(columns)
    .map(([alias, expr]) => {
      if (typeof expr === "string") {
        return `${expr} AS ${q(alias)}`
      }
      return `${buildWindowFunctionExpr(expr)} AS ${q(alias)}`
    })
    .join(", ")

  const parts: string[] = [`SELECT ${projections} FROM ${fromClause}`]

  // WHERE
  if (whereNode) {
    parts.push(`WHERE ${whereNode.props.condition as string}`)
  }

  // GROUP BY
  if (groupByNode) {
    const groupCols = (groupByNode.props.columns as readonly string[])
      .map(q)
      .join(", ")
    parts.push(`GROUP BY ${groupCols}`)
  }

  // HAVING
  if (havingNode) {
    parts.push(`HAVING ${havingNode.props.condition as string}`)
  }

  // ORDER BY
  if (orderByNode) {
    const orderCols = orderByNode.props.columns as Record<
      string,
      "ASC" | "DESC"
    >
    const orderClause = Object.entries(orderCols)
      .map(([col, dir]) => `${q(col)} ${dir}`)
      .join(", ")
    parts.push(`ORDER BY ${orderClause}`)
  }

  // WINDOW clause (named windows)
  if (windows && Object.keys(windows).length > 0) {
    const windowDefs = Object.entries(windows)
      .map(([name, spec]) => `${q(name)} AS (${buildWindowSpecSql(spec)})`)
      .join(", ")
    parts.push(`WINDOW ${windowDefs}`)
  }

  return parts.join("\n")
}

function buildWindowFunctionExpr(expr: {
  func: string
  args?: readonly (string | number)[]
  window?: string
  over?: {
    partitionBy?: readonly string[]
    orderBy?: Record<string, "ASC" | "DESC">
  }
}): string {
  const args = expr.args ? expr.args.join(", ") : ""
  const funcCall = `${expr.func}(${args})`

  if (expr.window) {
    return `${funcCall} OVER ${q(expr.window)}`
  }

  if (expr.over) {
    return `${funcCall} OVER (${buildWindowSpecSql(expr.over)})`
  }

  return funcCall
}

function buildWindowSpecSql(spec: {
  partitionBy?: readonly string[]
  orderBy?: Record<string, "ASC" | "DESC">
}): string {
  const parts: string[] = []

  if (spec.partitionBy && spec.partitionBy.length > 0) {
    parts.push(`PARTITION BY ${spec.partitionBy.map(q).join(", ")}`)
  }

  if (spec.orderBy) {
    const orderClause = Object.entries(spec.orderBy)
      .map(([col, dir]) => `${q(col)} ${dir}`)
      .join(", ")
    parts.push(`ORDER BY ${orderClause}`)
  }

  return parts.join(" ")
}

// ── RawSQL ──────────────────────────────────────────────────────────

function buildRawSqlQuery(node: ConstructNode): string {
  const sql = node.props.sql as string
  return sql
}

// ── MatchRecognize (CEP) ────────────────────────────────────────────

function buildMatchRecognizeQuery(
  ctx: BuildContext,
  node: ConstructNode,
): string {
  const pattern = node.props.pattern as string
  const define = node.props.define as Record<string, string>
  const measures = node.props.measures as Record<string, string>
  const after = node.props.after as string | undefined
  const partitionBy = node.props.partitionBy as readonly string[] | undefined
  const orderBy = node.props.orderBy as string | undefined

  const upstream = getUpstream(ctx, node)
  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  const parts: string[] = []

  if (partitionBy && partitionBy.length > 0) {
    parts.push(`  PARTITION BY ${partitionBy.map(q).join(", ")}`)
  }

  if (orderBy) {
    parts.push(`  ORDER BY ${q(orderBy)}`)
  }

  const measuresClause = Object.entries(measures)
    .map(([alias, expr]) => `    ${expr} AS ${q(alias)}`)
    .join(",\n")
  parts.push(`  MEASURES\n${measuresClause}`)

  if (after) {
    const strategy =
      after === "NEXT_ROW" ? "SKIP TO NEXT ROW" : "SKIP PAST LAST ROW"
    parts.push(`  AFTER MATCH ${strategy}`)
  }

  parts.push(`  PATTERN (${pattern})`)

  const defineClause = Object.entries(define)
    .map(([variable, condition]) => `    ${variable} AS ${condition}`)
    .join(",\n")
  parts.push(`  DEFINE\n${defineClause}`)

  return `SELECT *\nFROM ${fromClause}\nMATCH_RECOGNIZE (\n${parts.join("\n")}\n)`
}

// ── Side-path DML collector ──────────────────────────────────────────

/**
 * Recursively walk upstream nodes to find SideOutput/Validate nodes
 * that need to emit their side-path INSERT statements.
 */
function collectSideDml(
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

function buildSideOutputQuery(ctx: BuildContext, node: ConstructNode): string {
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

function collectSideOutputDml(
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

function buildValidateQuery(ctx: BuildContext, node: ConstructNode): string {
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

function collectValidateDml(
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

// ── View ────────────────────────────────────────────────────────────

function generateViewDdl(ctx: BuildContext, node: ConstructNode): string {
  const name = node.props.name as string

  // The view's children define the upstream query
  const upstream =
    node.children.length > 0
      ? buildQuery(ctx, node.children[0])
      : "SELECT * FROM unknown"

  return `CREATE VIEW ${q(name)} AS\n${upstream};`
}

// ── MaterializedTable DDL ───────────────────────────────────────────

function generateMaterializedTableDdl(
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
      ? buildQuery(ctx, node.children[0])
      : "SELECT * FROM unknown"

  parts.push(`AS\n${upstream};`)

  return parts.join("\n")
}

// ── LateralJoin ─────────────────────────────────────────────────────

function buildLateralJoinQuery(ctx: BuildContext, node: ConstructNode): string {
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

// ── Ref resolution ──────────────────────────────────────────────────

function resolveRef(ctx: BuildContext, nodeId: string): string {
  const node = ctx.nodeIndex.get(nodeId)
  if (!node) return q(nodeId)

  if (node.component === "CatalogSource") {
    return `${q(String(node.props.catalogName))}.${q(String(node.props.database))}.${q(String(node.props.table))}`
  }

  return q(nodeId)
}

/**
 * Resolve a join operand (left/right input).
 * If the operand is a Source, returns a simple table reference.
 * If the operand is a non-Source (e.g. another Join), inlines its SQL
 * as a CTE so the join can reference it by name.
 */
function resolveJoinOperand(
  ctx: BuildContext,
  nodeId: string,
): { ref: string; cte: string | null } {
  const node = ctx.nodeIndex.get(nodeId)
  if (!node || node.kind === "Source") {
    return { ref: resolveRef(ctx, nodeId), cte: null }
  }
  // Non-source operand — build its query and wrap as a CTE
  const sql = buildQuery(ctx, node)
  const cteName = q(nodeId)
  return { ref: cteName, cte: `${cteName} AS (\n${sql}\n)` }
}

// ── Effect-typed variant ─────────────────────────────────────────────

/**
 * Generate SQL returning Either with typed error.
 * Synchronous, no I/O — uses Either for pure error signaling.
 */
export function generateSqlEither(
  pipelineNode: ConstructNode,
  options: GenerateSqlOptions = {},
): Either.Either<GenerateSqlResult, SqlGenerationError> {
  try {
    return Either.right(generateSql(pipelineNode, options))
  } catch (err) {
    return Either.left(
      new SqlGenerationError({
        message: err instanceof Error ? err.message : String(err),
        component: pipelineNode.component,
        nodeId: pipelineNode.id,
      }),
    )
  }
}

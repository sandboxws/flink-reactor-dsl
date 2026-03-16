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
import { verifySql } from "./sql-verifier.js"

// ── Module-scoped version ───────────────────────────────────────────
// Set at the start of generateSql() and read by builders that need
// version-aware SQL generation (e.g. QUALIFY). Since codegen is
// synchronous, there's no reentrancy risk.
let _synthVersion: FlinkMajorVersion = "2.0"

// ── Module-scoped fragment collector ────────────────────────────────
// Tracks which construct nodes contributed which byte ranges within
// the current DML statement being built. Set by collectSinkDml before
// query building, cleared afterward.
let _fragments: SqlFragment[] | null = null

function beginFragmentCollection(): SqlFragment[] {
  const fragments: SqlFragment[] = []
  _fragments = fragments
  return fragments
}

function endFragmentCollection(): void {
  _fragments = null
}

/** Push a fragment for the current node's contribution. No-op when not collecting. */
function pushFragment(
  offset: number,
  length: number,
  node: ConstructNode,
): void {
  if (_fragments) {
    _fragments.push({
      offset,
      length,
      origin: { nodeId: node.id, component: node.component, kind: node.kind },
    })
  }
}

/** Shift all fragments from startIndex onward by delta bytes. */
function shiftFragmentsSince(startIndex: number, delta: number): void {
  if (!_fragments) return
  for (let i = startIndex; i < _fragments.length; i++) {
    _fragments[i] = { ..._fragments[i], offset: _fragments[i].offset + delta }
  }
}

// ── Public API ──────────────────────────────────────────────────────

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
}

/** Maps a statement index to the construct node that produced it. */
export interface StatementOrigin {
  /** Unique node ID from the ConstructNode tree */
  readonly nodeId: string
  /** Component class name (e.g. "KafkaSource", "Filter", "Aggregate") */
  readonly component: string
  /** Node kind (e.g. "Source", "Sink", "Transform", "Pipeline") */
  readonly kind: string
}

/** A fragment of a SQL statement attributed to a construct node. */
export interface SqlFragment {
  /** Byte offset from the start of the statement. */
  readonly offset: number
  /** Length in bytes. */
  readonly length: number
  /** The construct node that produced this fragment. */
  readonly origin: StatementOrigin
}

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
  const version = options.flinkVersion ?? "2.0"
  _synthVersion = version
  const pluginSql = options.pluginSqlGenerators
  const pluginDdl = options.pluginDdlGenerators

  // Run optimization passes if enabled
  let currentTree = pipelineNode
  let optimizerSets: readonly string[] = []
  if (options.optimize) {
    const optimizeOpts =
      typeof options.optimize === "object" ? options.optimize : {}
    const result = optimizePipeline(currentTree, version, optimizeOpts)
    currentTree = result.tree
    optimizerSets = result.additionalSets
  }

  // Build a node index for lookups by ID
  const nodeIndex = new Map<string, ConstructNode>()
  indexTree(currentTree, nodeIndex)

  const ctx = new SynthContext()
  ctx.buildFromTree(currentTree)

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

  // 1. SET statements from pipeline props
  const setStatements = generateSetStatements(currentTree, version)
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
  const catalogs = ctx.getNodesByKind("Catalog")
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
  const udfs = ctx.getNodesByKind("UDF")
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
  const sources = ctx.getNodesByKind("Source")
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
  const sinks = ctx.getNodesByKind("Sink")
  const sinkMeta = resolveSinkMetadata(currentTree, nodeIndex)

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
  const views = ctx.getNodesByKind("View")
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
      emit(generateViewDdl(view, nodeIndex, pluginSql), view)
    }
  }

  // 4.75. CREATE MATERIALIZED TABLE
  const matTables = ctx.getNodesByKind("MaterializedTable")
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
    emit(
      generateMaterializedTableDdl(matTable, nodeIndex, pluginSql, version),
      matTable,
    )
  }

  // 5. DML: INSERT INTO / STATEMENT SET
  const dmlEntries = generateDml(currentTree, nodeIndex, pluginSql)
  for (const entry of dmlEntries) {
    const details = buildDmlDetails(entry, nodeIndex)
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

// ── Comment block builder ────────────────────────────────────────────

const RULER = `-- ${"=".repeat(68)}`
const SEP = `-- ${"-".repeat(68)}`

function buildCommentBlock(
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

// ── Per-node detail builders ─────────────────────────────────────────

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

function buildCatalogDetails(
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

function buildSourceDetails(
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

function buildSourceSchema(
  node: ConstructNode,
): Array<{ name: string; type: string }> | undefined {
  const schemaDef = node.props.schema as
    | import("@/core/schema.js").SchemaDefinition
    | undefined
  if (!schemaDef) return undefined
  return Object.entries(schemaDef.fields).map(([name, type]) => ({
    name,
    type: String(type),
  }))
}

function buildSinkDetails(
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
    default:
      break
  }

  return details
}

function buildDmlDetails(
  entry: DmlEntry,
  nodeIndex: Map<string, ConstructNode>,
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
      const n = nodeIndex.get(c.origin.nodeId)
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

function getTransformDetail(
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

  const version = options.flinkVersion ?? "2.0"
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
    generatedAt: new Date().toISOString(),
    taps,
  }

  return { manifest, diagnostics }
}

// ── Identifier quoting ──────────────────────────────────────────────

function q(identifier: string): string {
  return `\`${identifier}\``
}

/** Strip changelog-encoding formats to their base serialization format.
 *  upsert-kafka handles changelog via key semantics — the value format
 *  must be insert-only (e.g. json, avro). */
function toInsertOnlyFormat(format: string): string {
  switch (format) {
    case "debezium-json":
    case "canal-json":
      return "json"
    case "debezium-avro":
      return "avro"
    default:
      return format
  }
}

// ── Duration parsing ────────────────────────────────────────────────

function toInterval(duration: string): string {
  const lower = duration.trim().toLowerCase()

  if (lower.startsWith("interval")) return duration

  if (/^\d+$/.test(lower)) {
    const ms = parseInt(lower, 10)
    if (ms % 86400000 === 0) return `INTERVAL '${ms / 86400000}' DAY`
    if (ms % 3600000 === 0) return `INTERVAL '${ms / 3600000}' HOUR`
    if (ms % 60000 === 0) return `INTERVAL '${ms / 60000}' MINUTE`
    return `INTERVAL '${ms / 1000}' SECOND`
  }

  const match = lower.match(
    /^(\d+)\s*(s|sec|second|seconds|m|min|minute|minutes|h|hr|hour|hours|d|day|days)$/,
  )
  if (match) {
    const value = match[1]
    const unit = match[2]
    if (unit.startsWith("s")) return `INTERVAL '${value}' SECOND`
    if (unit.startsWith("m")) return `INTERVAL '${value}' MINUTE`
    if (unit.startsWith("h")) return `INTERVAL '${value}' HOUR`
    if (unit.startsWith("d")) return `INTERVAL '${value}' DAY`
  }

  return `INTERVAL '${duration}'`
}

function toMilliseconds(duration: string): number {
  const lower = duration.trim().toLowerCase()

  if (/^\d+$/.test(lower)) return parseInt(lower, 10)

  const match = lower.match(
    /^(\d+)\s*(ms|s|sec|second|seconds|m|min|minute|minutes|h|hr|hour|hours)$/,
  )
  if (match) {
    const value = parseInt(match[1], 10)
    const unit = match[2]
    if (unit === "ms") return value
    if (unit.startsWith("s")) return value * 1000
    if (unit.startsWith("m")) return value * 60000
    if (unit.startsWith("h")) return value * 3600000
  }

  return parseInt(lower, 10)
}

// ── SET statements ──────────────────────────────────────────────────

function generateSetStatements(
  pipeline: ConstructNode,
  version: FlinkMajorVersion,
): string[] {
  const config: Record<string, string> = {}
  const props = pipeline.props

  // Always set pipeline.name so Flink job name matches the pipeline name.
  // This enables the dashboard to match running jobs to tap manifests by name.
  const pipelineName = props.name as string | undefined
  if (pipelineName) {
    config["pipeline.name"] = pipelineName
  }

  if (props.mode) {
    config["execution.runtime-mode"] = props.mode as string
  }

  if (props.parallelism !== undefined) {
    config["parallelism.default"] = String(props.parallelism)
  }

  const checkpoint = props.checkpoint as
    | { interval: string; mode?: string }
    | undefined
  if (checkpoint) {
    config["execution.checkpointing.interval"] = String(
      toMilliseconds(checkpoint.interval),
    )
    if (checkpoint.mode) {
      config["execution.checkpointing.mode"] = checkpoint.mode
    }
  }

  if (props.stateBackend) {
    config["state.backend.type"] = props.stateBackend as string
  }

  if (props.stateTtl) {
    config["table.exec.state.ttl"] = String(
      toMilliseconds(props.stateTtl as string),
    )
  }

  if (props.restartStrategy) {
    const rs = props.restartStrategy as {
      type: string
      attempts?: number
      delay?: string
    }
    config["restart-strategy.type"] = rs.type
    if (rs.attempts !== undefined) {
      config["restart-strategy.fixed-delay.attempts"] = String(rs.attempts)
    }
    if (rs.delay) {
      config["restart-strategy.fixed-delay.delay"] = rs.delay
    }
  }

  const userConfig = props.flinkConfig as Record<string, string> | undefined
  if (userConfig) {
    Object.assign(config, userConfig)
  }

  const normalized = FlinkVersionCompat.normalizeConfig(config, version)

  return Object.entries(normalized).map(
    ([key, value]) => `SET '${key}' = '${value}';`,
  )
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
      break
    case "IcebergCatalog":
      withProps.type = "iceberg"
      withProps["catalog-type"] = props.catalogType as string
      withProps.uri = props.uri as string
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
      if (props.bootstrapServers) {
        withProps["properties.bootstrap.servers"] =
          props.bootstrapServers as string
      }
      if (!hasPk) {
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
      withProps.connector = props.connector as string
      if (props.format) withProps.format = props.format as string
      if (props.options) {
        Object.assign(withProps, props.options as Record<string, string>)
      }
      break
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
    return `-- ${node.component} ${q(String(props.catalogName))}.${q(String(props.database))}.${q(String(props.table))} (catalog-managed)`
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
        type: "INT",
        expr: `HOUR(CAST(${q(col)} AS TIMESTAMP))`,
      }
    case "YEAR":
      return {
        name: "yr",
        type: "INT",
        expr: `YEAR(CAST(${q(col)} AS TIMESTAMP))`,
      }
    case "MONTH":
      return {
        name: "mo",
        type: "INT",
        expr: `MONTH(CAST(${q(col)} AS TIMESTAMP))`,
      }
    case "DAY":
      return {
        name: "dy",
        type: "INT",
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
      if (needsUpsert) {
        withProps.connector = "upsert-kafka"
        withProps.topic = props.topic as string
        withProps["key.format"] = "json"
        // upsert-kafka requires an insert-only value format — strip changelog
        // formats (debezium-json, canal-json) to their base serialization
        const rawFormat = (props.format as string) ?? "json"
        withProps["value.format"] = toInsertOnlyFormat(rawFormat)
      } else {
        withProps.connector = "kafka"
        withProps.topic = props.topic as string
        withProps.format = (props.format as string) ?? "json"
      }
      // Explicit prop takes priority; fall back to pipeline-level bootstrap
      const bootstrap = (props.bootstrapServers as string) ?? pipelineBootstrap
      if (bootstrap) {
        withProps["properties.bootstrap.servers"] = bootstrap
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
  pipelineNode: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): Map<string, SinkMetadata> {
  const result = new Map<string, SinkMetadata>()

  function resolveSourceChangelogMode(source: ConstructNode): ChangelogMode {
    const mode = source.props.changelogMode as string | undefined
    return mode === "retract" ? "retract" : "append-only"
  }

  function resolveSourcePrimaryKey(
    source: ConstructNode,
  ): readonly string[] | undefined {
    const schema = source.props.schema as
      | { primaryKey?: { columns: readonly string[] } }
      | undefined
    return schema?.primaryKey?.columns
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
        const schema = resolveNodeSchema(upstream, nodeIndex)
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
          const startSchema = resolveNodeSchema(sibling, nodeIndex)
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
            // Propagate PK from the left (primary) source
            const leftSource = findDeepestSource(sibling)
            if (leftSource) {
              primaryKey = resolveSourcePrimaryKey(leftSource)
            }
          }
          // Propagate through intermediate transforms
          const transforms: ConstructNode[] = []
          for (let j = i + 1; j < sinkIndex; j++) {
            const transform = parent.children[j]
            if (transform.kind === "Transform" || transform.kind === "Window") {
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
          const startSchema = resolveNodeSchema(sibling, nodeIndex)
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
            if (transform.kind === "Transform" || transform.kind === "Window") {
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
  pipelineNode: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): DmlEntry[] {
  const entries: DmlEntry[] = []

  // Collect all sinks from the pipeline's direct children
  collectSinkDml(pipelineNode, nodeIndex, entries, pluginSql)

  return entries
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
  parent: ConstructNode,
  sinkIndex: number,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  // Collect preceding siblings that form the data chain
  const chain: ConstructNode[] = []
  for (let i = 0; i < sinkIndex; i++) {
    const sibling = parent.children[i]
    if (
      sibling.kind === "Source" ||
      sibling.kind === "Transform" ||
      sibling.kind === "Window" ||
      sibling.kind === "Join" ||
      sibling.kind === "CEP"
    ) {
      chain.push(sibling)
    }
  }

  if (chain.length === 0) return "SELECT * FROM unknown"

  // If only a source, just reference it
  if (chain.length === 1 && chain[0].kind === "Source") {
    return buildQuery(chain[0], nodeIndex, pluginSql)
  }

  // Chain: first element is the source, rest are transforms
  let currentUpstream: { sql: string; sourceRef: string; isSimple: boolean } = {
    sql: buildQuery(chain[0], nodeIndex, pluginSql),
    sourceRef: chain[0].kind === "Source" ? q(chain[0].id) : chain[0].id,
    isSimple: chain[0].kind === "Source",
  }

  // Track schema through the chain for schema-aware transforms (Rename, Drop, Cast, etc.)
  let chainSchema: ResolvedColumn[] | null =
    chain[0].kind === "Source" ? resolveNodeSchema(chain[0], nodeIndex) : null

  const chainFragStart = _fragments?.length ?? 0

  for (let i = 1; i < chain.length; i++) {
    const transform = chain[i]
    const prevSql = currentUpstream.sql
    const fragBeforeIter = _fragments?.length ?? 0

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
        sql: buildQuery(transform, nodeIndex, pluginSql),
        sourceRef: transform.id,
        isSimple: false,
      }
      chainSchema = resolveNodeSchema(transform, nodeIndex)
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
    const sql = buildQuery(transform, nodeIndex, pluginSql)
    ;(injectionTarget as { children: ConstructNode[] }).children = savedChildren

    // Shift fragments from previous iterations to account for embedding.
    // When the previous SQL is wrapped inside the new query (e.g. as a subquery),
    // earlier fragments need their offsets adjusted to where prevSql appears in sql.
    if (
      !currentUpstream.isSimple &&
      _fragments &&
      fragBeforeIter > chainFragStart
    ) {
      const embedIdx = sql.indexOf(prevSql)
      if (embedIdx >= 0) {
        for (let f = chainFragStart; f < fragBeforeIter; f++) {
          _fragments[f] = {
            ..._fragments[f],
            offset: _fragments[f].offset + embedIdx,
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
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  entries: DmlEntry[],
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
  parent?: ConstructNode,
): void {
  if (node.kind === "Sink") {
    const sinkRef = resolveSinkRef(node)

    // Collect fragments during query building
    const fragments = beginFragmentCollection()
    let upstream: string
    if (node.children.length > 0) {
      // Pattern 1: reverse-nesting — sink wraps its upstream as children
      upstream = buildQuery(node.children[0], nodeIndex, pluginSql)
    } else if (parent) {
      // Pattern 2: forward-reading JSX — sink is a sibling of its upstream
      const sinkIndex = parent.children.indexOf(node)
      upstream = buildSiblingChainQuery(parent, sinkIndex, nodeIndex, pluginSql)
    } else {
      upstream = "SELECT * FROM unknown"
    }
    endFragmentCollection()

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
      collectSideDml(child, nodeIndex, entries, pluginSql)
    }
    return
  }

  // Route generates multiple INSERT statements (one per branch)
  if (node.component === "Route") {
    collectRouteDml(node, nodeIndex, entries, pluginSql, parent)
    return
  }

  // SideOutput generates two INSERT statements (main + side)
  if (node.component === "SideOutput") {
    collectSideOutputDml(node, nodeIndex, entries, pluginSql)
    return
  }

  // Validate generates two INSERT statements (valid + rejected)
  if (node.component === "Validate") {
    collectValidateDml(node, nodeIndex, entries, pluginSql)
    return
  }

  // Recurse into children to find sinks
  for (const child of node.children) {
    collectSinkDml(child, nodeIndex, entries, pluginSql, node)
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
  routeNode: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
  parent?: ConstructNode,
): { sql: string; sourceRef: string; isSimple: boolean } {
  // Pattern 1: upstream is a direct non-Branch child of Route
  const upstreamChildren = routeNode.children.filter(
    (c) => c.component !== "Route.Branch" && c.component !== "Route.Default",
  )
  if (upstreamChildren.length > 0) {
    const up = getUpstream(
      { children: upstreamChildren } as ConstructNode,
      nodeIndex,
      pluginSql,
    )
    return up
  }

  // Pattern 2: upstream is a preceding sibling chain in parent (JSX forward-reading)
  // Build the full chain (e.g. Source → Map) rather than just the nearest sibling.
  if (parent) {
    const routeIndex = parent.children.indexOf(routeNode)
    const chainSql = buildSiblingChainQuery(
      parent,
      routeIndex,
      nodeIndex,
      pluginSql,
    )
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
  transforms: readonly ConstructNode[],
  upstream: { sql: string; sourceRef: string; isSimple: boolean },
  branchCondition: string | undefined,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
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
    const upstreamNode = nodeIndex.get(rawRef)
    if (upstreamNode) chainSchema = resolveNodeSchema(upstreamNode, nodeIndex)
  }

  // Chain remaining transforms using VirtualRef for temporary child injection
  const branchFragStart = _fragments?.length ?? 0

  for (let i = startIndex; i < transforms.length; i++) {
    const transform = transforms[i]
    const prevSql = currentUpstream.sql
    const fragBeforeIter = _fragments?.length ?? 0
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
    const sql = buildQuery(transform, nodeIndex, pluginSql)
    ;(transform as { children: ConstructNode[] }).children = savedChildren

    // Shift fragments from previous iterations to account for embedding
    if (
      !currentUpstream.isSimple &&
      _fragments &&
      fragBeforeIter > branchFragStart
    ) {
      const embedIdx = sql.indexOf(prevSql)
      if (embedIdx >= 0) {
        for (let f = branchFragStart; f < fragBeforeIter; f++) {
          _fragments[f] = {
            ..._fragments[f],
            offset: _fragments[f].offset + embedIdx,
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
  routeNode: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  entries: DmlEntry[],
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
  parent?: ConstructNode,
): void {
  const branches = routeNode.children.filter(
    (c) => c.component === "Route.Branch" || c.component === "Route.Default",
  )

  const upstream = resolveRouteUpstream(routeNode, nodeIndex, pluginSql, parent)

  for (const branch of branches) {
    // Normalize: treat condition "true" as no condition (it selects all rows)
    const rawCondition = branch.props.condition as string | undefined
    const condition =
      rawCondition?.trim().toLowerCase() === "true" ? undefined : rawCondition

    // Separate transforms and sinks within the branch
    const transforms = branch.children.filter((c) => c.kind !== "Sink")
    const sinks = branch.children.filter((c) => c.kind === "Sink")

    // Collect fragments during branch query building
    const fragments = beginFragmentCollection()
    const query = buildBranchQuery(
      transforms,
      upstream,
      condition,
      nodeIndex,
      pluginSql,
    )
    endFragmentCollection()

    for (const sink of sinks) {
      const sinkRef = resolveSinkRef(sink)
      let sinkQuery = query

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

function buildQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  // Check plugin SQL generators first (allows overriding built-in components)
  const pluginGen = pluginSql?.get(node.component)
  if (pluginGen) {
    return pluginGen(node, nodeIndex)
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
          result = buildQuery(child, nodeIndex, pluginSql)
          ;(child as { children: ConstructNode[] }).children = saved
        }
        return result
      }
      return `SELECT * FROM ${q(node.id)}`
    case "CatalogSource":
      return `SELECT * FROM ${q(String(node.props.catalogName))}.${q(String(node.props.database))}.${q(String(node.props.table))}`

    // Transforms
    case "Filter":
      return buildFilterQuery(node, nodeIndex, pluginSql)
    case "Map":
      return buildMapQuery(node, nodeIndex, pluginSql)
    case "FlatMap":
      return buildFlatMapQuery(node, nodeIndex, pluginSql)
    case "Aggregate":
      return buildAggregateQuery(node, nodeIndex, pluginSql)
    case "Union":
      return buildUnionQuery(node, nodeIndex, pluginSql)
    case "Deduplicate":
      return buildDeduplicateQuery(node, nodeIndex, pluginSql)
    case "TopN":
      return buildTopNQuery(node, nodeIndex, pluginSql)

    // Field transforms
    case "Rename":
      return buildRenameQuery(node, nodeIndex, pluginSql)
    case "Drop":
      return buildDropQuery(node, nodeIndex, pluginSql)
    case "Cast":
      return buildCastQuery(node, nodeIndex, pluginSql)
    case "Coalesce":
      return buildCoalesceQuery(node, nodeIndex, pluginSql)
    case "AddField":
      return buildAddFieldQuery(node, nodeIndex, pluginSql)

    // Joins
    case "Join":
      return buildJoinQuery(node, nodeIndex)
    case "TemporalJoin":
      return buildTemporalJoinQuery(node, nodeIndex)
    case "LookupJoin":
      return buildLookupJoinQuery(node, nodeIndex)
    case "IntervalJoin":
      return buildIntervalJoinQuery(node, nodeIndex)

    // Windows
    case "TumbleWindow":
    case "SlideWindow":
    case "SessionWindow":
      return buildWindowQuery(node, nodeIndex, pluginSql)

    // Escape hatches
    case "Query":
      return buildQueryComponentQuery(node, nodeIndex, pluginSql)
    case "RawSQL":
      return buildRawSqlQuery(node)
    case "Qualify":
      return buildQualifyQuery(node, nodeIndex, pluginSql)

    // CEP
    case "MatchRecognize":
      return buildMatchRecognizeQuery(node, nodeIndex, pluginSql)

    // Validate — main path (valid records)
    case "Validate":
      return buildValidateQuery(node, nodeIndex, pluginSql)

    // View — reference by name
    case "View":
      return `SELECT * FROM ${q(node.props.name as string)}`

    // SideOutput — main path (non-matching records)
    case "SideOutput":
      return buildSideOutputQuery(node, nodeIndex, pluginSql)

    // LateralJoin — LATERAL TABLE TVF join
    case "LateralJoin":
      return buildLateralJoinQuery(node, nodeIndex)

    default:
      // Unknown — try first child
      if (node.children.length > 0) {
        return buildQuery(node.children[0], nodeIndex, pluginSql)
      }
      return `SELECT * FROM ${q(node.id)}`
  }
}

/**
 * Get the upstream SQL and source table reference from a node's children.
 */
function getUpstream(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
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
  if (child.kind === "Source" && !pluginSql?.has(child.component)) {
    const ref =
      child.component === "CatalogSource"
        ? `${q(String(child.props.catalogName))}.${q(String(child.props.database))}.${q(String(child.props.table))}`
        : q(child.id)
    return { sql: `SELECT * FROM ${ref}`, sourceRef: ref, isSimple: true }
  }

  // Otherwise build the child query
  const childSql = buildQuery(child, nodeIndex, pluginSql)
  return { sql: childSql, sourceRef: child.id, isSimple: false }
}

// ── Filter ──────────────────────────────────────────────────────────

function buildFilterQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const condition = node.props.condition as string
  const fragStart = _fragments?.length ?? 0
  const upstream = getUpstream(node, nodeIndex, pluginSql)

  const wherePart = ` WHERE ${condition}`

  if (upstream.isSimple) {
    const result = `SELECT * FROM ${upstream.sourceRef}${wherePart}`
    pushFragment(result.length - wherePart.length, wherePart.length, node)
    return result
  }
  const prefix = `SELECT * FROM (\n`
  const result = `${prefix}${upstream.sql}\n)${wherePart}`
  shiftFragmentsSince(fragStart, prefix.length)
  pushFragment(result.length - wherePart.length, wherePart.length, node)
  return result
}

// ── Map ─────────────────────────────────────────────────────────────

function buildMapQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const select = node.props.select as Record<string, string>
  const projections = Object.entries(select)
    .map(([alias, expr]) => `${expr} AS ${q(alias)}`)
    .join(", ")

  const fragStart = _fragments?.length ?? 0
  const upstream = getUpstream(node, nodeIndex, pluginSql)

  const selectPart = `SELECT ${projections}`

  if (upstream.isSimple) {
    const result = `${selectPart} FROM ${upstream.sourceRef}`
    pushFragment(0, selectPart.length, node)
    return result
  }
  const prefix = `${selectPart} FROM (\n`
  const result = `${prefix}${upstream.sql}\n)`
  shiftFragmentsSince(fragStart, prefix.length)
  pushFragment(0, selectPart.length, node)
  return result
}

// ── FlatMap ─────────────────────────────────────────────────────────

function buildFlatMapQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const unnestField = node.props.unnest as string
  const asFields = node.props.as as Record<string, string>
  const aliases = Object.keys(asFields).map(q).join(", ")

  const fragStart = _fragments?.length ?? 0
  const upstream = getUpstream(node, nodeIndex, pluginSql)
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
    shiftFragmentsSince(fragStart, prefix.length)
  }

  // Track: the added aliases in SELECT and the CROSS JOIN UNNEST are FlatMap's contribution
  pushFragment(0, selectPart.length, node)
  pushFragment(result.length - crossJoinPart.length, crossJoinPart.length, node)

  return result
}

// ── Aggregate ───────────────────────────────────────────────────────

function buildAggregateQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const groupBy = node.props.groupBy as readonly string[]
  const select = node.props.select as Record<string, string>
  const groupBySet = new Set(groupBy)

  const groupCols = groupBy.map(q).join(", ")
  const projections = [
    ...groupBy.map(q),
    // Skip select entries that duplicate a groupBy column
    ...Object.entries(select)
      .filter(([alias]) => !groupBySet.has(alias))
      .map(([alias, expr]) => `${expr} AS ${q(alias)}`),
  ].join(", ")

  const fragStart = _fragments?.length ?? 0
  const upstream = getUpstream(node, nodeIndex, pluginSql)

  const selectPart = `SELECT ${projections}`
  const groupByPart = ` GROUP BY ${groupCols}`

  if (upstream.isSimple) {
    const result = `${selectPart} FROM ${upstream.sourceRef}${groupByPart}`
    pushFragment(0, selectPart.length, node)
    pushFragment(result.length - groupByPart.length, groupByPart.length, node)
    return result
  }
  const prefix = `${selectPart} FROM (\n`
  const result = `${prefix}${upstream.sql}\n)${groupByPart}`
  shiftFragmentsSince(fragStart, prefix.length)
  pushFragment(0, selectPart.length, node)
  pushFragment(result.length - groupByPart.length, groupByPart.length, node)
  return result
}

// ── Union ───────────────────────────────────────────────────────────

function buildUnionQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const parts: string[] = []

  for (const child of node.children) {
    parts.push(buildQuery(child, nodeIndex, pluginSql))
  }

  if (parts.length === 0) return "SELECT * FROM unknown"
  const result = parts.join("\nUNION ALL\n")

  // Track each "UNION ALL" keyword as a Union fragment
  if (parts.length > 1) {
    let offset = parts[0].length + 1 // +1 for the \n before "UNION ALL"
    for (let i = 1; i < parts.length; i++) {
      pushFragment(offset, "UNION ALL".length, node)
      offset += "UNION ALL".length + 1 + parts[i].length + 1 // +1 for \n after, +1 for \n before next
    }
  }

  return result
}

// ── Deduplicate ─────────────────────────────────────────────────────

function buildDeduplicateQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const key = node.props.key as readonly string[]
  const order = node.props.order as string
  const keep = node.props.keep as "first" | "last"

  const partitionBy = key.map(q).join(", ")
  const orderDir = keep === "first" ? "ASC" : "DESC"

  const fragStart = _fragments?.length ?? 0
  const upstream = getUpstream(node, nodeIndex, pluginSql)
  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  if (FlinkVersionCompat.isVersionAtLeast(_synthVersion, "2.0")) {
    const selectPart = `SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${q(order)} ${orderDir}) AS rownum`
    const qualifyPart = "QUALIFY rownum = 1"
    const result = `${selectPart}\nFROM ${fromClause}\n${qualifyPart}`
    if (!upstream.isSimple)
      shiftFragmentsSince(fragStart, selectPart.length + 1 + "FROM (\n".length)
    pushFragment(0, selectPart.length, node)
    pushFragment(result.length - qualifyPart.length, qualifyPart.length, node)
    return result
  }

  const innerSelect = `  SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${q(order)} ${orderDir}) AS rownum`
  const wherePart = ") WHERE rownum = 1"
  const result = `SELECT * FROM (\n${innerSelect}\n  FROM ${fromClause}\n${wherePart}`
  if (!upstream.isSimple)
    shiftFragmentsSince(
      fragStart,
      "SELECT * FROM (\n".length + innerSelect.length + 1 + "  FROM (\n".length,
    )
  pushFragment(0, "SELECT * FROM (".length, node)
  pushFragment("SELECT * FROM (\n".length, innerSelect.length, node)
  pushFragment(result.length - wherePart.length, wherePart.length, node)
  return result
}

// ── TopN ────────────────────────────────────────────────────────────

function buildTopNQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const partitionBy = (node.props.partitionBy as readonly string[])
    .map(q)
    .join(", ")
  const orderBy = node.props.orderBy as Record<string, "ASC" | "DESC">
  const n = node.props.n as number

  const orderClause = Object.entries(orderBy)
    .map(([field, dir]) => `${q(field)} ${dir}`)
    .join(", ")

  const fragStart = _fragments?.length ?? 0
  const upstream = getUpstream(node, nodeIndex, pluginSql)
  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  if (FlinkVersionCompat.isVersionAtLeast(_synthVersion, "2.0")) {
    const selectPart = `SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${orderClause}) AS rownum`
    const qualifyPart = `QUALIFY rownum <= ${n}`
    const result = `${selectPart}\nFROM ${fromClause}\n${qualifyPart}`
    if (!upstream.isSimple)
      shiftFragmentsSince(fragStart, selectPart.length + 1 + "FROM (\n".length)
    pushFragment(0, selectPart.length, node)
    pushFragment(result.length - qualifyPart.length, qualifyPart.length, node)
    return result
  }

  const innerSelect = `  SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${orderClause}) AS rownum`
  const wherePart = `) WHERE rownum <= ${n}`
  const result = `SELECT * FROM (\n${innerSelect}\n  FROM ${fromClause}\n${wherePart}`
  if (!upstream.isSimple)
    shiftFragmentsSince(
      fragStart,
      "SELECT * FROM (\n".length + innerSelect.length + 1 + "  FROM (\n".length,
    )
  pushFragment(0, "SELECT * FROM (".length, node)
  pushFragment("SELECT * FROM (\n".length, innerSelect.length, node)
  pushFragment(result.length - wherePart.length, wherePart.length, node)
  return result
}

// ── Qualify (escape hatch) ───────────────────────────────────────────

function buildQualifyQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const condition = node.props.condition as string
  const windowExpr = node.props.window as string | undefined

  const upstream = getUpstream(node, nodeIndex, pluginSql)

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

function buildRenameQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const columns = node.props.columns as Record<string, string>
  const fragStart = _fragments?.length ?? 0
  const upstream = getUpstream(node, nodeIndex, pluginSql)

  const schema = resolveNodeSchema(node.children[0], nodeIndex)
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
        pushFragment(offset, colParts[i].text.length, node)
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
  shiftFragmentsSince(fragStart, prefix.length)
  pushRenameFragments(0)
  return result
}

// ── Drop ────────────────────────────────────────────────────────────

function buildDropQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const dropCols = new Set(node.props.columns as readonly string[])
  const fragStart = _fragments?.length ?? 0
  const upstream = getUpstream(node, nodeIndex, pluginSql)

  const schema = resolveNodeSchema(node.children[0], nodeIndex)
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
    pushFragment(0, selectPart.length, node)
    return result
  }
  const prefix = `${selectPart} FROM (\n`
  const result = `${prefix}${upstream.sql}\n)`
  shiftFragmentsSince(fragStart, prefix.length)
  pushFragment(0, selectPart.length, node)
  return result
}

// ── Cast ────────────────────────────────────────────────────────────

function buildCastQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const castCols = node.props.columns as Record<string, string>
  const safe = node.props.safe as boolean | undefined
  const castFn = safe ? "TRY_CAST" : "CAST"
  const fragStart = _fragments?.length ?? 0
  const upstream = getUpstream(node, nodeIndex, pluginSql)

  const schema = resolveNodeSchema(node.children[0], nodeIndex)
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
        pushFragment(offset, colParts[i].text.length, node)
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
  shiftFragmentsSince(fragStart, prefix.length)
  pushCastFragments(0)
  return result
}

// ── Coalesce ────────────────────────────────────────────────────────

function buildCoalesceQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const coalesceCols = node.props.columns as Record<string, string>
  const fragStart = _fragments?.length ?? 0
  const upstream = getUpstream(node, nodeIndex, pluginSql)

  const schema = resolveNodeSchema(node.children[0], nodeIndex)
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
        pushFragment(offset, colParts[i].text.length, node)
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
  shiftFragmentsSince(fragStart, prefix.length)
  pushCoalesceFragments(0)
  return result
}

// ── AddField ────────────────────────────────────────────────────────

function buildAddFieldQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const addCols = node.props.columns as Record<string, string>
  const fragStart = _fragments?.length ?? 0
  const upstream = getUpstream(node, nodeIndex, pluginSql)

  // Check for name collisions with upstream schema
  const schema = resolveNodeSchema(node.children[0], nodeIndex)
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
    pushFragment(addedOffset, addedLength, node)
    return result
  }
  const prefix = `${selectPart} FROM (\n`
  const result = `${prefix}${upstream.sql}\n)`
  shiftFragmentsSince(fragStart, prefix.length)
  pushFragment(addedOffset, addedLength, node)
  return result
}

// ── Join ────────────────────────────────────────────────────────────

function buildJoinQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): string {
  const joinType = (node.props.type as string) ?? "inner"
  const onCondition = node.props.on as string
  const hints = node.props.hints as { broadcast?: "left" | "right" } | undefined
  const leftId = node.props.left as string
  const rightId = node.props.right as string

  const left = resolveJoinOperand(leftId, nodeIndex)
  const right = resolveJoinOperand(rightId, nodeIndex)
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
    ? `/*+ BROADCAST(${resolveRef(hints.broadcast === "left" ? leftId : rightId, nodeIndex)}) */ `
    : ""

  return `${ctePrefix}SELECT ${hintClause}* FROM ${left.ref} ${sqlJoinType} JOIN ${right.ref} ON ${onCondition}`
}

// ── Temporal Join ───────────────────────────────────────────────────

function buildTemporalJoinQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): string {
  const onCondition = node.props.on as string
  const asOf = node.props.asOf as string
  const streamId = node.props.stream as string
  const temporalId = node.props.temporal as string

  const stream = resolveJoinOperand(streamId, nodeIndex)
  const temporal = resolveJoinOperand(temporalId, nodeIndex)
  const ctes = [stream.cte, temporal.cte].filter(Boolean)
  const ctePrefix = ctes.length > 0 ? `WITH ${ctes.join(",\n")}\n` : ""

  return `${ctePrefix}SELECT * FROM ${stream.ref} LEFT JOIN ${temporal.ref} FOR SYSTEM_TIME AS OF ${stream.ref}.${q(asOf)} ON ${onCondition}`
}

// ── Lookup Join ─────────────────────────────────────────────────────

function buildLookupJoinQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): string {
  const onCondition = node.props.on as string
  const inputId = node.props.input as string
  const table = node.props.table as string
  const select = node.props.select as Record<string, string> | undefined

  const input = resolveJoinOperand(inputId, nodeIndex)
  const ctePrefix = input.cte ? `WITH ${input.cte}\n` : ""

  const projections = select
    ? Object.entries(select)
        .map(([alias, expr]) => `${expr} AS ${q(alias)}`)
        .join(", ")
    : "*"

  return `${ctePrefix}SELECT ${projections} FROM ${input.ref} LEFT JOIN ${q(table)} FOR SYSTEM_TIME AS OF ${input.ref}.proc_time ON ${onCondition}`
}

// ── Interval Join ───────────────────────────────────────────────────

function buildIntervalJoinQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): string {
  const joinType = (node.props.type as string) ?? "inner"
  const onCondition = node.props.on as string
  const interval = node.props.interval as { from: string; to: string }
  const leftId = node.props.left as string
  const rightId = node.props.right as string

  const left = resolveJoinOperand(leftId, nodeIndex)
  const right = resolveJoinOperand(rightId, nodeIndex)
  const ctes = [left.cte, right.cte].filter(Boolean)
  const ctePrefix = ctes.length > 0 ? `WITH ${ctes.join(",\n")}\n` : ""

  const sqlJoinType = joinType === "inner" ? "" : `${joinType.toUpperCase()} `

  // Interval join: right.watermarkCol BETWEEN left.from AND left.to
  const rightNode = nodeIndex.get(rightId)
  const rightSchema = rightNode?.props.schema as
    | { watermark?: { column: string } }
    | undefined
  const rightTimeCol = rightSchema?.watermark?.column
  const betweenCol = rightTimeCol
    ? `${right.ref}.${q(rightTimeCol)}`
    : `${right.ref}.${interval.from}`

  return `${ctePrefix}SELECT * FROM ${left.ref} ${sqlJoinType}JOIN ${right.ref} ON ${onCondition} AND ${betweenCol} BETWEEN ${left.ref}.${interval.from} AND ${left.ref}.${interval.to}`
}

// ── Window ──────────────────────────────────────────────────────────

function buildWindowQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const windowCol = node.props.on as string

  // Find the Aggregate child (if windowed aggregation)
  const aggChild = node.children.find((c) => c.component === "Aggregate")

  // Find the source feeding the window (non-Aggregate child)
  const sourceChild = node.children.find((c) => c.component !== "Aggregate")
  const upstream = sourceChild
    ? getUpstream(
        { children: [sourceChild] } as ConstructNode,
        nodeIndex,
        pluginSql,
      )
    : getUpstream(node, nodeIndex, pluginSql)
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
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
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
      ? getUpstream(
          { children: upstreamChildren } as ConstructNode,
          nodeIndex,
          pluginSql,
        )
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
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const pattern = node.props.pattern as string
  const define = node.props.define as Record<string, string>
  const measures = node.props.measures as Record<string, string>
  const after = node.props.after as string | undefined
  const partitionBy = node.props.partitionBy as readonly string[] | undefined
  const orderBy = node.props.orderBy as string | undefined

  const upstream = getUpstream(node, nodeIndex, pluginSql)
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
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  entries: DmlEntry[],
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): void {
  if (node.component === "SideOutput") {
    collectSideOutputDml(node, nodeIndex, entries, pluginSql)
  } else if (node.component === "Validate") {
    collectValidateDml(node, nodeIndex, entries, pluginSql)
  }

  // Continue recursing to find nested SideOutput/Validate
  for (const child of node.children) {
    collectSideDml(child, nodeIndex, entries, pluginSql)
  }
}

// ── SideOutput ──────────────────────────────────────────────────────

function buildSideOutputQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const condition = node.props.condition as string

  // Upstream is found from non-SideOutput.Sink children
  const upstreamChildren = node.children.filter(
    (c) => c.component !== "SideOutput.Sink",
  )
  const upstream =
    upstreamChildren.length > 0
      ? getUpstream(
          { children: upstreamChildren } as ConstructNode,
          nodeIndex,
          pluginSql,
        )
      : { sql: "SELECT * FROM unknown", sourceRef: "unknown", isSimple: false }

  if (upstream.isSimple) {
    return `SELECT * FROM ${upstream.sourceRef} WHERE NOT (${condition})`
  }
  return `SELECT * FROM (\n${upstream.sql}\n) WHERE NOT (${condition})`
}

function collectSideOutputDml(
  sideOutputNode: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  entries: DmlEntry[],
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
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
      ? getUpstream(
          { children: upstreamChildren } as ConstructNode,
          nodeIndex,
          pluginSql,
        )
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

function buildValidateQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
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
      ? getUpstream(
          { children: upstreamChildren } as ConstructNode,
          nodeIndex,
          pluginSql,
        )
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
  validateNode: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  entries: DmlEntry[],
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
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
      ? getUpstream(
          { children: upstreamChildren } as ConstructNode,
          nodeIndex,
          pluginSql,
        )
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

function generateViewDdl(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const name = node.props.name as string

  // The view's children define the upstream query
  const upstream =
    node.children.length > 0
      ? buildQuery(node.children[0], nodeIndex, pluginSql)
      : "SELECT * FROM unknown"

  return `CREATE VIEW ${q(name)} AS\n${upstream};`
}

// ── MaterializedTable DDL ───────────────────────────────────────────

function generateMaterializedTableDdl(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql: ReadonlyMap<string, PluginSqlGenerator> | undefined,
  version: FlinkMajorVersion,
): string {
  const props = node.props
  const name = props.name as string
  const catalogName = props.catalogName as string
  const database = props.database as string | undefined

  // Version gate: require Flink >= 2.0
  const versionCheck = FlinkVersionCompat.checkFeature(
    "MATERIALIZED_TABLE",
    version,
  )
  if (versionCheck) {
    throw new Error(versionCheck.message)
  }

  // Freshness is required for Flink < 2.2
  if (
    !props.freshness &&
    !FlinkVersionCompat.isVersionAtLeast(version, "2.2")
  ) {
    throw new Error("MaterializedTable freshness is required for Flink < 2.2")
  }

  // Bucketing requires Flink >= 2.2
  if (props.bucketing) {
    const bucketCheck = FlinkVersionCompat.checkFeature(
      "MATERIALIZED_TABLE_BUCKETING",
      version,
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
      ? buildQuery(node.children[0], nodeIndex, pluginSql)
      : "SELECT * FROM unknown"

  parts.push(`AS\n${upstream};`)

  return parts.join("\n")
}

// ── LateralJoin ─────────────────────────────────────────────────────

function buildLateralJoinQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): string {
  const funcName = node.props.function as string
  const args = node.props.args as readonly (string | number)[]
  const asFields = node.props.as as Record<string, string>
  const joinType = (node.props.type as string) ?? "cross"
  const inputId = node.props.input as string

  const inputRef = resolveRef(inputId, nodeIndex)
  const aliases = Object.keys(asFields).map(q).join(", ")
  const argList = args
    .map((a) => (typeof a === "string" ? a : String(a)))
    .join(", ")

  const sqlJoinType = joinType === "left" ? "LEFT JOIN" : "JOIN"

  return `SELECT ${inputRef}.*, T.${Object.keys(asFields).map(q).join(`, T.`)} FROM ${inputRef} ${sqlJoinType} LATERAL TABLE(${funcName}(${argList})) AS T(${aliases})`
}

// ── Ref resolution ──────────────────────────────────────────────────

function resolveRef(
  nodeId: string,
  nodeIndex: Map<string, ConstructNode>,
): string {
  const node = nodeIndex.get(nodeId)
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
  nodeId: string,
  nodeIndex: Map<string, ConstructNode>,
): { ref: string; cte: string | null } {
  const node = nodeIndex.get(nodeId)
  if (!node || node.kind === "Source") {
    return { ref: resolveRef(nodeId, nodeIndex), cte: null }
  }
  // Non-source operand — build its query and wrap as a CTE
  const sql = buildQuery(node, nodeIndex)
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

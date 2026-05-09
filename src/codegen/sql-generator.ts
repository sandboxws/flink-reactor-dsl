import { Either } from "effect"
import { SqlGenerationError } from "@/core/errors.js"
import { FlinkVersionCompat } from "@/core/flink-compat.js"
import type { PluginDdlGenerator, PluginSqlGenerator } from "@/core/plugin.js"
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
  type SqlFragment,
  type StatementOrigin,
} from "./sql-build-context.js"
import { generateCatalogDdl, generateUdfDdl } from "./sql-ddl-catalog.js"
import { generateSinkDdl, resolvePartitionExpression } from "./sql-ddl-sink.js"
import { generateSourceDdl } from "./sql-ddl-source.js"
import type { DmlEntry } from "./sql-dml-types.js"
// `q` is the historical alias used by the ~96 inline call sites in this
// file. The shared helper additionally escapes embedded backticks, which
// the previous local copy did not.
import { quoteIdentifier as q } from "./sql-identifiers.js"
import { buildQuery } from "./sql-query-dispatcher.js"
import { getUpstream } from "./sql-query-helpers.js"
import {
  collectSideDml,
  collectSideOutputDml,
  collectValidateDml,
} from "./sql-query-side-paths.js"
import { resolveSinkRef } from "./sql-refs.js"
import { generateSetStatements } from "./sql-set-statements.js"
import { resolveSinkMetadata, type SinkMetadata } from "./sql-sink-metadata.js"
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
    buildQuery,
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

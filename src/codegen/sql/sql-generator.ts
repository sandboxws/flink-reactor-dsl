/**
 * SQL synthesis orchestrator. This file owns the public API surface
 * (`generateSql`, `generateSqlEither`, `generateTapManifest`,
 * `hasPipelineConnectorSource`) and the private driver
 * (`generateSqlImpl`) that walks the construct tree and emits SQL in
 * deterministic order: SET → CATALOG → UDF → SOURCE → SINK → VIEW →
 * MATERIALIZED TABLE → INSERT.
 *
 * Every concrete builder lives in a focused sibling module:
 *
 *   - sql-build-context.ts      — BuildContext + lifecycle helpers
 *   - sql-set-statements.ts     — leading SET 'k' = 'v' block
 *   - sql-ddl-{catalog,source,sink,views}.ts — CREATE TABLE/CATALOG/VIEW DDL
 *   - sql-sink-metadata.ts      — schema/changelog/PK propagation
 *   - sql-query-{transforms,field-ops,aggregate-window,joins,escape,
 *                side-paths}.ts — per-component SELECT builders
 *   - sql-query-dispatcher.ts   — Map-based registry that routes a node
 *                                  to its builder
 *   - sql-dml-collection.ts     — INSERT statement collection (the engine
 *                                  that walks sinks → sources)
 *   - sql-statement-meta.ts     — dashboard hover-tooltip metadata
 *
 * Synthesis state lives on a per-call `BuildContext` threaded through
 * every internal helper as the first argument. The reentrancy tripwire
 * (module-level `_activeSynthesisCount` in sql-build-context.ts) fires
 * when a plugin re-enters `generateSql` synchronously — that path is
 * unsupported because fragment accumulators would interleave.
 */

import { Either } from "effect"
import { SqlGenerationError } from "@/core/errors.js"
import type { PluginDdlGenerator, PluginSqlGenerator } from "@/core/plugin.js"
import type { ValidationDiagnostic } from "@/core/synth-context.js"
import { SynthContext } from "@/core/synth-context.js"
import { generateTapMetadata } from "@/core/tap.js"
import type {
  ConstructNode,
  FlinkMajorVersion,
  TapManifest,
} from "@/core/types.js"
import type { OptimizeOptions } from "../pipeline-optimizer.js"
import { optimizePipeline } from "../pipeline-optimizer.js"
import { indexTree } from "../schema-introspect.js"
import {
  type BuildContext,
  createBuildContext,
  enterSynthesis,
  exitSynthesis,
  type SqlFragment,
  type StatementOrigin,
} from "./sql-build-context.js"
import { generateCatalogDdl, generateUdfDdl } from "./sql-ddl-catalog.js"
import { generateSinkDdl } from "./sql-ddl-sink.js"
import { generateSourceDdl } from "./sql-ddl-source.js"
import {
  generateMaterializedTableDdl,
  generateViewDdl,
} from "./sql-ddl-views.js"
import { generateDml } from "./sql-dml-collection.js"
import { buildQuery } from "./sql-query-dispatcher.js"
import { generateSetStatements } from "./sql-set-statements.js"
import { resolveSinkMetadata } from "./sql-sink-metadata.js"
import {
  buildCatalogDetails,
  buildCommentBlock,
  buildDmlDetails,
  buildSinkDetails,
  buildSourceDetails,
  buildSourceSchema,
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

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
import {
  collectTransformChain,
  findDeepestSource,
  findRouteUpstreamNode,
  indexTree,
  type ResolvedColumn,
  resolveNodeSchema,
  resolveTransformSchema,
} from "./schema-introspect.js"

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
}

export interface GenerateSqlResult {
  readonly statements: readonly string[]
  readonly sql: string
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
  const pluginSql = options.pluginSqlGenerators
  const pluginDdl = options.pluginDdlGenerators

  // Build a node index for lookups by ID
  const nodeIndex = new Map<string, ConstructNode>()
  indexTree(pipelineNode, nodeIndex)

  const ctx = new SynthContext()
  ctx.buildFromTree(pipelineNode)

  const statements: string[] = []

  // 1. SET statements from pipeline props
  statements.push(...generateSetStatements(pipelineNode, version))

  // 2. CREATE CATALOG
  const catalogs = ctx.getNodesByKind("Catalog")
  for (const cat of catalogs) {
    statements.push(generateCatalogDdl(cat))
  }

  // 2.5. CREATE FUNCTION (UDFs)
  const udfs = ctx.getNodesByKind("UDF")
  for (const udf of udfs) {
    statements.push(generateUdfDdl(udf))
  }

  // 3. CREATE TABLE (sources) — check plugin DDL generators first
  const sources = ctx.getNodesByKind("Source")
  for (const src of sources) {
    const pluginGen = pluginDdl?.get(src.component)
    if (pluginGen) {
      const ddl = pluginGen(src)
      if (ddl) statements.push(ddl)
    } else {
      const ddl = generateSourceDdl(src)
      if (ddl) statements.push(ddl)
    }
  }

  // 4. CREATE TABLE (sinks) — resolve schemas then generate DDL
  const sinks = ctx.getNodesByKind("Sink")
  const sinkMeta = resolveSinkMetadata(pipelineNode, nodeIndex)
  for (const sink of sinks) {
    const pluginGen = pluginDdl?.get(sink.component)
    if (pluginGen) {
      const ddl = pluginGen(sink)
      if (ddl) statements.push(ddl)
    } else {
      statements.push(generateSinkDdl(sink, sinkMeta.get(sink.id)))
    }
  }

  // 4.5. CREATE VIEW (only for View nodes that define a query, not references)
  const views = ctx.getNodesByKind("View")
  for (const view of views) {
    if (view.children.length > 0) {
      statements.push(generateViewDdl(view, nodeIndex, pluginSql))
    }
  }

  // 5. DML: INSERT INTO / STATEMENT SET
  const dmlStatements = generateDml(pipelineNode, nodeIndex, pluginSql)
  if (dmlStatements.length === 1) {
    statements.push(dmlStatements[0])
  } else if (dmlStatements.length > 1) {
    statements.push(
      `EXECUTE STATEMENT SET BEGIN\n${dmlStatements.join("\n")}\nEND;`,
    )
  }

  return {
    statements,
    sql: `${statements.join("\n\n")}\n`,
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
    case "KafkaSource":
      withProps.connector = "kafka"
      withProps.topic = props.topic as string
      withProps.format = (props.format as string) ?? "json"
      if (props.bootstrapServers) {
        withProps["properties.bootstrap.servers"] =
          props.bootstrapServers as string
      }
      if (props.startupMode) {
        withProps["scan.startup.mode"] = props.startupMode as string
      }
      if (props.consumerGroup) {
        withProps["properties.group.id"] = props.consumerGroup as string
      }
      break
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

function generateSinkDdl(node: ConstructNode, metadata?: SinkMetadata): string {
  const props = node.props
  const tableName = node.id

  if (props.catalogName) {
    return `-- ${node.component} ${q(String(props.catalogName))}.${q(String(props.database))}.${q(String(props.table))} (catalog-managed)`
  }

  const withClause = generateSinkWithClause(node, metadata)
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

  if (node.component === "JdbcSink" && props.upsertMode && props.keyFields) {
    const cols = (props.keyFields as readonly string[]).map(q).join(", ")
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
    const partCols = (props.partitionBy as readonly string[]).join(", ")
    if (columnDefs) {
      parts.push(
        `CREATE TABLE ${q(tableName)} (`,
        columnDefs,
        `) PARTITIONED BY (${partCols}) WITH (\n${withClause}\n);`,
      )
    } else {
      parts.push(
        `CREATE TABLE ${q(tableName)} PARTITIONED BY (${partCols}) WITH (\n${withClause}\n);`,
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

function generateSinkWithClause(
  node: ConstructNode,
  metadata?: SinkMetadata,
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
        withProps["value.format"] = (props.format as string) ?? "json"
      } else {
        withProps.connector = "kafka"
        withProps.topic = props.topic as string
        withProps.format = (props.format as string) ?? "json"
      }
      if (props.bootstrapServers) {
        withProps["properties.bootstrap.servers"] =
          props.bootstrapServers as string
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
    }
    return mode
  }

  /**
   * Propagate primary key through a transform chain.
   * - Filter/Map/Deduplicate/TopN: PK preserved from upstream
   * - Aggregate: PK becomes the groupBy columns
   */
  function propagatePrimaryKey(
    transforms: readonly ConstructNode[],
    upstreamPk: readonly string[] | undefined,
  ): readonly string[] | undefined {
    let pk = upstreamPk
    for (const t of transforms) {
      if (t.component === "Aggregate") {
        pk = t.props.groupBy as readonly string[]
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
          if (sibling.kind === "Source") {
            schema = resolveNodeSchema(sibling, nodeIndex)
            changelogMode = resolveSourceChangelogMode(sibling)
            primaryKey = resolveSourcePrimaryKey(sibling)
            // Propagate through intermediate transforms
            const transforms: ConstructNode[] = []
            for (let j = i + 1; j < sinkIndex; j++) {
              const transform = parent.children[j]
              if (transform.kind === "Transform") {
                transforms.push(transform)
                if (schema) schema = resolveTransformSchema(transform, schema)
              }
            }
            changelogMode = propagateChangelogMode(transforms, changelogMode)
            primaryKey = propagatePrimaryKey(transforms, primaryKey)
            break
          }
        }
        if (schema) result.set(node.id, { schema, changelogMode, primaryKey })
      }
      return
    }

    if (node.component === "Route") {
      // Find upstream source for the Route
      const routeUpstreamNode = findRouteUpstreamNode(node, parent)
      const routeSchema = routeUpstreamNode
        ? resolveNodeSchema(routeUpstreamNode, nodeIndex)
        : null
      const upstreamSource =
        routeUpstreamNode?.kind === "Source"
          ? routeUpstreamNode
          : routeUpstreamNode
            ? findDeepestSource(routeUpstreamNode)
            : null
      const baseChangelogMode = upstreamSource
        ? resolveSourceChangelogMode(upstreamSource)
        : ("append-only" as ChangelogMode)
      const basePk = upstreamSource
        ? resolveSourcePrimaryKey(upstreamSource)
        : undefined

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

function generateDml(
  pipelineNode: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string[] {
  const statements: string[] = []

  // Collect all sinks from the pipeline's direct children
  collectSinkDml(pipelineNode, nodeIndex, statements, pluginSql)

  return statements
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
    if (sibling.kind === "Source" || sibling.kind === "Transform") {
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

  for (let i = 1; i < chain.length; i++) {
    const transform = chain[i]

    // Only inject VirtualRef if the transform has no children (forward-reading pattern)
    if (transform.children.length > 0) {
      // Transform already has children (reverse-nesting) — just build it normally
      currentUpstream = {
        sql: buildQuery(transform, nodeIndex, pluginSql),
        sourceRef: transform.id,
        isSimple: false,
      }
      chainSchema = resolveNodeSchema(transform, nodeIndex)
      continue
    }

    const savedChildren = transform.children
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

    ;(transform as { children: ConstructNode[] }).children = [virtualSource]
    const sql = buildQuery(transform, nodeIndex, pluginSql)
    ;(transform as { children: ConstructNode[] }).children = savedChildren

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
  statements: string[],
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
  parent?: ConstructNode,
): void {
  if (node.kind === "Sink") {
    const sinkRef = resolveSinkRef(node)
    let upstream: string
    if (node.children.length > 0) {
      // Pattern 1: reverse-nesting — sink wraps its upstream as children
      upstream = buildQuery(node.children[0], nodeIndex, pluginSql)
    } else if (parent) {
      // Pattern 2: forward-reading JSX — sink is a sibling of its upstream
      // Collect preceding siblings (sources + transforms) in reading order and chain them
      const sinkIndex = parent.children.indexOf(node)
      upstream = buildSiblingChainQuery(parent, sinkIndex, nodeIndex, pluginSql)
    } else {
      upstream = "SELECT * FROM unknown"
    }
    statements.push(`INSERT INTO ${sinkRef}\n${upstream};`)
    // Recurse into upstream to find SideOutput/Validate side-path DML
    for (const child of node.children) {
      collectSideDml(child, nodeIndex, statements, pluginSql)
    }
    return
  }

  // Route generates multiple INSERT statements (one per branch)
  if (node.component === "Route") {
    collectRouteDml(node, nodeIndex, statements, pluginSql, parent)
    return
  }

  // SideOutput generates two INSERT statements (main + side)
  if (node.component === "SideOutput") {
    collectSideOutputDml(node, nodeIndex, statements, pluginSql)
    return
  }

  // Validate generates two INSERT statements (valid + rejected)
  if (node.component === "Validate") {
    collectValidateDml(node, nodeIndex, statements, pluginSql)
    return
  }

  // Recurse into children to find sinks
  for (const child of node.children) {
    collectSinkDml(child, nodeIndex, statements, pluginSql, node)
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

  // Pattern 2: upstream is a preceding sibling in parent (JSX forward-reading)
  if (parent) {
    const routeIndex = parent.children.indexOf(routeNode)
    for (let i = routeIndex - 1; i >= 0; i--) {
      const sibling = parent.children[i]
      if (sibling.kind === "Source" || sibling.kind === "Transform") {
        const up = getUpstream(
          { children: [sibling] } as ConstructNode,
          nodeIndex,
          pluginSql,
        )
        return up
      }
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
  for (let i = startIndex; i < transforms.length; i++) {
    const transform = transforms[i]
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

    ;(transform as { children: ConstructNode[] }).children = [virtualSource]
    const sql = buildQuery(transform, nodeIndex, pluginSql)
    ;(transform as { children: ConstructNode[] }).children = savedChildren

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
  statements: string[],
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

    // Build query chain: source → transforms → (branch condition)
    const query = buildBranchQuery(
      transforms,
      upstream,
      condition,
      nodeIndex,
      pluginSql,
    )

    for (const sink of sinks) {
      const sinkRef = resolveSinkRef(sink)
      statements.push(`INSERT INTO ${sinkRef}\n${query};`)
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

    // Sources — terminal nodes
    case "KafkaSource":
    case "JdbcSource":
    case "GenericSource":
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
  const upstream = getUpstream(node, nodeIndex, pluginSql)

  if (upstream.isSimple) {
    return `SELECT * FROM ${upstream.sourceRef} WHERE ${condition}`
  }
  return `SELECT * FROM (\n${upstream.sql}\n) WHERE ${condition}`
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

  const upstream = getUpstream(node, nodeIndex, pluginSql)

  if (upstream.isSimple) {
    return `SELECT ${projections} FROM ${upstream.sourceRef}`
  }
  return `SELECT ${projections} FROM (\n${upstream.sql}\n)`
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

  const upstream = getUpstream(node, nodeIndex, pluginSql)
  const ref = upstream.sourceRef

  return `SELECT ${q(ref)}.*, ${aliases} FROM ${upstream.isSimple ? ref : `(\n${upstream.sql}\n) AS ${q(ref)}`} CROSS JOIN UNNEST(${q(ref)}.${q(unnestField)}) AS T(${aliases})`
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

  const upstream = getUpstream(node, nodeIndex, pluginSql)

  if (upstream.isSimple) {
    return `SELECT ${projections} FROM ${upstream.sourceRef} GROUP BY ${groupCols}`
  }
  return `SELECT ${projections} FROM (\n${upstream.sql}\n) GROUP BY ${groupCols}`
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
  return parts.join("\nUNION ALL\n")
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

  const upstream = getUpstream(node, nodeIndex, pluginSql)
  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  return [
    "SELECT * FROM (",
    `  SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${q(order)} ${orderDir}) AS rownum`,
    `  FROM ${fromClause}`,
    ") WHERE rownum = 1",
  ].join("\n")
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

  const upstream = getUpstream(node, nodeIndex, pluginSql)
  const fromClause = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  return [
    "SELECT * FROM (",
    `  SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${orderClause}) AS rownum`,
    `  FROM ${fromClause}`,
    `) WHERE rownum <= ${n}`,
  ].join("\n")
}

// ── Rename ──────────────────────────────────────────────────────────

function buildRenameQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const columns = node.props.columns as Record<string, string>
  const upstream = getUpstream(node, nodeIndex, pluginSql)

  const schema = resolveNodeSchema(node.children[0], nodeIndex)
  if (!schema) {
    // Unresolvable upstream — fall back to SELECT *
    if (upstream.isSimple) return `SELECT * FROM ${upstream.sourceRef}`
    return `SELECT * FROM (\n${upstream.sql}\n)`
  }

  const projections = schema
    .map((c) => {
      const newName = columns[c.name]
      return newName ? `${q(c.name)} AS ${q(newName)}` : q(c.name)
    })
    .join(", ")

  if (upstream.isSimple) {
    return `SELECT ${projections} FROM ${upstream.sourceRef}`
  }
  return `SELECT ${projections} FROM (\n${upstream.sql}\n)`
}

// ── Drop ────────────────────────────────────────────────────────────

function buildDropQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const dropCols = new Set(node.props.columns as readonly string[])
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

  if (upstream.isSimple) {
    return `SELECT ${projections} FROM ${upstream.sourceRef}`
  }
  return `SELECT ${projections} FROM (\n${upstream.sql}\n)`
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
  const upstream = getUpstream(node, nodeIndex, pluginSql)

  const schema = resolveNodeSchema(node.children[0], nodeIndex)
  if (!schema) {
    if (upstream.isSimple) return `SELECT * FROM ${upstream.sourceRef}`
    return `SELECT * FROM (\n${upstream.sql}\n)`
  }

  const projections = schema
    .map((c) => {
      const targetType = castCols[c.name]
      return targetType
        ? `${castFn}(${q(c.name)} AS ${targetType}) AS ${q(c.name)}`
        : q(c.name)
    })
    .join(", ")

  if (upstream.isSimple) {
    return `SELECT ${projections} FROM ${upstream.sourceRef}`
  }
  return `SELECT ${projections} FROM (\n${upstream.sql}\n)`
}

// ── Coalesce ────────────────────────────────────────────────────────

function buildCoalesceQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const coalesceCols = node.props.columns as Record<string, string>
  const upstream = getUpstream(node, nodeIndex, pluginSql)

  const schema = resolveNodeSchema(node.children[0], nodeIndex)
  if (!schema) {
    if (upstream.isSimple) return `SELECT * FROM ${upstream.sourceRef}`
    return `SELECT * FROM (\n${upstream.sql}\n)`
  }

  const projections = schema
    .map((c) => {
      const defaultExpr = coalesceCols[c.name]
      return defaultExpr
        ? `COALESCE(${q(c.name)}, ${defaultExpr}) AS ${q(c.name)}`
        : q(c.name)
    })
    .join(", ")

  if (upstream.isSimple) {
    return `SELECT ${projections} FROM ${upstream.sourceRef}`
  }
  return `SELECT ${projections} FROM (\n${upstream.sql}\n)`
}

// ── AddField ────────────────────────────────────────────────────────

function buildAddFieldQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): string {
  const addCols = node.props.columns as Record<string, string>
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

  if (upstream.isSimple) {
    return `SELECT *, ${addedProjections} FROM ${upstream.sourceRef}`
  }
  return `SELECT *, ${addedProjections} FROM (\n${upstream.sql}\n)`
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

  const leftRef = resolveRef(leftId, nodeIndex)
  const rightRef = resolveRef(rightId, nodeIndex)

  if (joinType === "anti") {
    return `SELECT ${leftRef}.* FROM ${leftRef} WHERE NOT EXISTS (\n  SELECT 1 FROM ${rightRef} WHERE ${onCondition}\n)`
  }

  if (joinType === "semi") {
    return `SELECT ${leftRef}.* FROM ${leftRef} WHERE EXISTS (\n  SELECT 1 FROM ${rightRef} WHERE ${onCondition}\n)`
  }

  const sqlJoinType =
    joinType === "full" ? "FULL OUTER" : joinType.toUpperCase()

  const hintClause = hints?.broadcast
    ? `/*+ BROADCAST(${resolveRef(hints.broadcast === "left" ? leftId : rightId, nodeIndex)}) */ `
    : ""

  return `SELECT ${hintClause}* FROM ${leftRef} ${sqlJoinType} JOIN ${rightRef} ON ${onCondition}`
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

  const streamRef = resolveRef(streamId, nodeIndex)
  const temporalRef = resolveRef(temporalId, nodeIndex)

  return `SELECT * FROM ${streamRef} LEFT JOIN ${temporalRef} FOR SYSTEM_TIME AS OF ${streamRef}.${q(asOf)} ON ${onCondition}`
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

  const inputRef = resolveRef(inputId, nodeIndex)

  const projections = select
    ? Object.entries(select)
        .map(([alias, expr]) => `${expr} AS ${q(alias)}`)
        .join(", ")
    : "*"

  return `SELECT ${projections} FROM ${inputRef} LEFT JOIN ${q(table)} FOR SYSTEM_TIME AS OF ${inputRef}.proc_time ON ${onCondition}`
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

  const leftRef = resolveRef(leftId, nodeIndex)
  const rightRef = resolveRef(rightId, nodeIndex)

  const sqlJoinType = joinType === "inner" ? "" : `${joinType.toUpperCase()} `

  return `SELECT * FROM ${leftRef} ${sqlJoinType}JOIN ${rightRef} ON ${onCondition} AND ${leftRef}.${interval.from} BETWEEN ${rightRef}.${interval.from} AND ${rightRef}.${interval.to}`
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
  const sourceRef = upstream.isSimple
    ? upstream.sourceRef
    : `(\n${upstream.sql}\n)`

  const tvf = buildWindowTvf(node, sourceRef, windowCol)

  if (!aggChild) {
    return `SELECT * FROM TABLE(\n  ${tvf}\n)`
  }

  const groupBy = aggChild.props.groupBy as readonly string[]
  const select = aggChild.props.select as Record<string, string>

  const groupCols = [...groupBy.map(q), "window_start", "window_end"].join(", ")

  const projections = [
    ...groupBy.map(q),
    ...Object.entries(select).map(([alias, expr]) => `${expr} AS ${q(alias)}`),
    "window_start",
    "window_end",
  ].join(", ")

  return `SELECT ${projections} FROM TABLE(\n  ${tvf}\n) GROUP BY ${groupCols}`
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
  statements: string[],
  pluginSql?: ReadonlyMap<string, PluginSqlGenerator>,
): void {
  if (node.component === "SideOutput") {
    collectSideOutputDml(node, nodeIndex, statements, pluginSql)
  } else if (node.component === "Validate") {
    collectValidateDml(node, nodeIndex, statements, pluginSql)
  }

  // Continue recursing to find nested SideOutput/Validate
  for (const child of node.children) {
    collectSideDml(child, nodeIndex, statements, pluginSql)
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
  statements: string[],
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
        statements.push(
          `INSERT INTO ${sinkRef}\nSELECT ${selectList} FROM ${fromClause} WHERE (${condition});`,
        )
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
  statements: string[],
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
        statements.push(
          `INSERT INTO ${sinkRef}\nSELECT *,\n    CASE\n${errorCase}\n    END AS \`_validation_error\`,\n    CURRENT_TIMESTAMP AS \`_validated_at\`\nFROM ${fromClause}\nWHERE NOT (\n    ${validCondition}\n);`,
        )
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

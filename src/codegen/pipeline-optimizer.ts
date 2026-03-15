import { FlinkVersionCompat } from "@/core/flink-compat.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"
import { indexTree } from "./schema-introspect.js"

// ── Public types ─────────────────────────────────────────────────────

export interface OptimizeOptions {
  /** Enable/disable SET statement auto-injection (default: true) */
  readonly setInjection?: boolean
  /** Enable/disable SQL hint emission (default: true) */
  readonly hintEmission?: boolean
  /** Enable/disable source schema narrowing (default: true) */
  readonly schemaNarrowing?: boolean
  /** Enable/disable dead branch elimination (default: true) */
  readonly deadBranchElimination?: boolean
}

/** Result of running optimization passes on a construct tree */
export interface OptimizationResult {
  /** The (potentially modified) construct tree */
  readonly tree: ConstructNode
  /** Additional SET statements to prepend to SQL output */
  readonly additionalSets: readonly string[]
  /** Hint annotations keyed by node ID → hint SQL fragment */
  readonly hintAnnotations: ReadonlyMap<string, string>
}

// ── Version-aware SET key registry ───────────────────────────────────

/**
 * Registry of performance-related SET keys with version-aware mappings.
 * All keys use canonical 2.0+ names; FlinkVersionCompat.normalizeConfig
 * handles backward mapping to 1.20.
 */
const PERF_SET_KEYS = {
  // Mini-batch aggregation
  miniBatchEnabled: "table.exec.mini-batch.enabled",
  miniBatchInterval: "table.exec.mini-batch.allow-latency",
  miniBatchSize: "table.exec.mini-batch.size",
  // Two-phase aggregation
  twoPhaseAggEnabled: "table.optimizer.agg-phase-strategy",
  // Distinct-agg splitting
  distinctAggSplitEnabled: "table.optimizer.distinct-agg.split.enabled",
  distinctAggSplitBucketNum: "table.optimizer.distinct-agg.split.bucket-num",
} as const

// ── Pass: SET statement injection ────────────────────────────────────

function hasNodeOfComponent(node: ConstructNode, component: string): boolean {
  if (node.component === component) return true
  return node.children.some((c) => hasNodeOfComponent(c, component))
}

function collectNodesOfComponent(
  node: ConstructNode,
  component: string,
): ConstructNode[] {
  const result: ConstructNode[] = []
  if (node.component === component) result.push(node)
  for (const child of node.children) {
    result.push(...collectNodesOfComponent(child, component))
  }
  return result
}

function hasDistinctAggregate(node: ConstructNode): boolean {
  const aggregates = collectNodesOfComponent(node, "Aggregate")
  for (const agg of aggregates) {
    const select = agg.props.select as Record<string, string> | undefined
    if (!select) continue
    for (const expr of Object.values(select)) {
      if (/\bDISTINCT\b/i.test(expr)) return true
    }
  }
  return false
}

function hasHighCardinalityGroupBy(node: ConstructNode): boolean {
  const aggregates = collectNodesOfComponent(node, "Aggregate")
  for (const agg of aggregates) {
    const groupBy = agg.props.groupBy as readonly string[] | undefined
    // Heuristic: 3+ group-by columns suggests high cardinality
    if (groupBy && groupBy.length >= 3) return true
  }
  return false
}

/**
 * Analyze the construct tree and inject performance SET statements
 * based on detected pipeline patterns.
 */
function runSetInjectionPass(
  tree: ConstructNode,
  version: FlinkMajorVersion,
  userSets: ReadonlySet<string>,
): string[] {
  const config: Record<string, string> = {}

  // Mini-batch: enable when Aggregate nodes are present
  if (hasNodeOfComponent(tree, "Aggregate")) {
    if (!userSets.has(PERF_SET_KEYS.miniBatchEnabled)) {
      config[PERF_SET_KEYS.miniBatchEnabled] = "true"
    }
    if (!userSets.has(PERF_SET_KEYS.miniBatchInterval)) {
      config[PERF_SET_KEYS.miniBatchInterval] = "5 s"
    }
    if (!userSets.has(PERF_SET_KEYS.miniBatchSize)) {
      config[PERF_SET_KEYS.miniBatchSize] = "1000"
    }
  }

  // TWO_PHASE aggregation for high-cardinality GROUP BY
  if (hasHighCardinalityGroupBy(tree)) {
    if (!userSets.has(PERF_SET_KEYS.twoPhaseAggEnabled)) {
      config[PERF_SET_KEYS.twoPhaseAggEnabled] = "TWO_PHASE"
    }
  }

  // Distinct-agg splitting for DISTINCT aggregates
  if (hasDistinctAggregate(tree)) {
    if (!userSets.has(PERF_SET_KEYS.distinctAggSplitEnabled)) {
      config[PERF_SET_KEYS.distinctAggSplitEnabled] = "true"
    }
    if (!userSets.has(PERF_SET_KEYS.distinctAggSplitBucketNum)) {
      config[PERF_SET_KEYS.distinctAggSplitBucketNum] = "1024"
    }
  }

  if (Object.keys(config).length === 0) return []

  // Normalize for Flink version
  const normalized = FlinkVersionCompat.normalizeConfig(config, version)

  return Object.entries(normalized).map(
    ([key, value]) => `SET '${key}' = '${value}';`,
  )
}

// ── Pass: SQL hint emission ──────────────────────────────────────────

/**
 * Collect hint annotations from Join nodes in the construct tree.
 * Returns a map of node ID → SQL hint clause to be injected in SELECT.
 */
function runHintEmissionPass(tree: ConstructNode): Map<string, string> {
  const annotations = new Map<string, string>()

  function walk(node: ConstructNode): void {
    if (node.component === "Join") {
      const hints: string[] = []

      // BROADCAST hint
      const joinHints = node.props.hints as
        | { broadcast?: "left" | "right" }
        | undefined
      if (joinHints?.broadcast) {
        const side =
          joinHints.broadcast === "left"
            ? (node.props.left as string)
            : (node.props.right as string)
        hints.push(`BROADCAST(${side})`)
      }

      // STATE_TTL hint
      const stateTtl = node.props.stateTtl as string | undefined
      if (stateTtl) {
        const leftId = node.props.left as string
        const rightId = node.props.right as string
        hints.push(
          `STATE_TTL(${leftId} = '${stateTtl}', ${rightId} = '${stateTtl}')`,
        )
      }

      if (hints.length > 0) {
        annotations.set(node.id, `/*+ ${hints.join(", ")} */`)
      }
    }

    for (const child of node.children) {
      walk(child)
    }
  }

  walk(tree)
  return annotations
}

// ── Pass: Source schema narrowing ────────────────────────────────────

/**
 * Trace which columns are actually referenced downstream from each source.
 * Returns a map of source node ID → set of referenced column names.
 * Returns null for a source if any dynamic reference is detected (conservative).
 */
function traceColumnUsage(
  tree: ConstructNode,
): Map<string, Set<string> | null> {
  const usage = new Map<string, Set<string> | null>()
  const nodeIndex = new Map<string, ConstructNode>()
  indexTree(tree, nodeIndex)

  // Collect all source node IDs
  function collectSources(node: ConstructNode): void {
    if (node.kind === "Source") {
      usage.set(node.id, new Set())
    }
    for (const child of node.children) {
      collectSources(child)
    }
  }
  collectSources(tree)

  // Walk the tree and trace column references from each transform back to sources
  function traceNode(node: ConstructNode): void {
    // For each transform, determine which source it reads from and which columns it references
    if (node.kind === "Transform" || node.kind === "Sink") {
      const referencedCols = extractReferencedColumns(node)

      if (referencedCols === null) {
        // Dynamic reference detected — mark all upstream sources as null (keep all)
        markUpstreamSourcesConservative(node)
      } else {
        // Add referenced columns to upstream source
        addColumnsToUpstreamSource(node, referencedCols)
      }
    }

    for (const child of node.children) {
      traceNode(child)
    }
  }

  function markUpstreamSourcesConservative(node: ConstructNode): void {
    if (node.kind === "Source") {
      usage.set(node.id, null)
      return
    }
    for (const child of node.children) {
      markUpstreamSourcesConservative(child)
    }
  }

  function addColumnsToUpstreamSource(
    node: ConstructNode,
    columns: Set<string>,
  ): void {
    // Walk down to find the source and trace columns through transforms
    if (node.kind === "Source") {
      const current = usage.get(node.id)
      if (current === null) return // already marked conservative
      const merged = current ?? new Set<string>()
      for (const col of columns) {
        merged.add(col)
      }
      usage.set(node.id, merged)
      return
    }

    // For transforms that rename columns, map back to original names
    const mappedCols = mapColumnsBackThroughTransform(node, columns)
    if (mappedCols === null) {
      // Can't map — go conservative on upstream
      markUpstreamSourcesConservative(node)
      return
    }

    for (const child of node.children) {
      addColumnsToUpstreamSource(child, mappedCols)
    }
  }

  traceNode(tree)
  return usage
}

/**
 * Extract the set of column names referenced by a node.
 * Returns null if dynamic references are detected (e.g., SELECT *).
 */
function extractReferencedColumns(node: ConstructNode): Set<string> | null {
  const cols = new Set<string>()

  switch (node.component) {
    case "Filter": {
      const condition = node.props.condition as string | undefined
      if (!condition) return null
      extractColumnsFromExpression(condition, cols)
      return cols
    }

    case "Map": {
      const select = node.props.select as Record<string, string> | undefined
      if (!select) return null
      for (const expr of Object.values(select)) {
        extractColumnsFromExpression(expr, cols)
      }
      return cols
    }

    case "Aggregate": {
      const groupBy = node.props.groupBy as readonly string[] | undefined
      const select = node.props.select as Record<string, string> | undefined
      if (groupBy) {
        for (const col of groupBy) cols.add(col)
      }
      if (select) {
        for (const expr of Object.values(select)) {
          extractColumnsFromExpression(expr, cols)
        }
      }
      return cols
    }

    case "Drop": {
      // Drop needs all columns (it passes through everything except dropped ones)
      return null
    }

    case "Rename": {
      // Rename references specific columns
      const columns = node.props.columns as Record<string, string> | undefined
      if (!columns) return null
      for (const oldName of Object.keys(columns)) {
        cols.add(oldName)
      }
      // But it also passes through all other columns
      return null
    }

    case "AddField": {
      const columns = node.props.columns as Record<string, string> | undefined
      if (!columns) return null
      // AddField passes through all upstream columns plus new computed ones
      // The computed ones reference upstream columns
      for (const expr of Object.values(columns)) {
        extractColumnsFromExpression(expr, cols)
      }
      // But still passes through all upstream columns
      return null
    }

    case "Cast":
    case "Coalesce":
    case "Deduplicate":
    case "TopN":
      // These pass through all columns
      return null

    default:
      // Unknown — be conservative
      return null
  }
}

/**
 * Extract column name references from a SQL expression.
 * Handles backtick-quoted identifiers and simple column names.
 */
function extractColumnsFromExpression(
  expr: string,
  columns: Set<string>,
): void {
  // Match backtick-quoted identifiers
  const backtickPattern = /`(\w+)`/g
  let match: RegExpExecArray | null
  match = backtickPattern.exec(expr)
  while (match !== null) {
    columns.add(match[1])
    match = backtickPattern.exec(expr)
  }

  // Match bare identifiers used in aggregate functions or standalone
  // e.g., SUM(amount), COUNT(user_id), field_name
  const bareIdentPattern =
    /\b(?:SUM|COUNT|AVG|MIN|MAX|FIRST_VALUE|LAST_VALUE)\s*\(\s*(?:DISTINCT\s+)?(\w+)\s*\)/gi
  match = bareIdentPattern.exec(expr)
  while (match !== null) {
    if (match[1] !== "*") columns.add(match[1])
    match = bareIdentPattern.exec(expr)
  }

  // Simple bare column reference (single word, not a keyword or function)
  const SQL_KEYWORDS = new Set([
    "AND",
    "OR",
    "NOT",
    "IS",
    "NULL",
    "TRUE",
    "FALSE",
    "IN",
    "BETWEEN",
    "LIKE",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "AS",
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "BY",
    "HAVING",
    "ORDER",
    "LIMIT",
    "DISTINCT",
    "SUM",
    "COUNT",
    "AVG",
    "MIN",
    "MAX",
    "FIRST_VALUE",
    "LAST_VALUE",
    "INTERVAL",
  ])
  const simpleRef = /\b([a-zA-Z_]\w*)\b/g
  match = simpleRef.exec(expr)
  while (match !== null) {
    if (!SQL_KEYWORDS.has(match[1].toUpperCase()) && !/^\d/.test(match[1])) {
      columns.add(match[1])
    }
    match = simpleRef.exec(expr)
  }
}

/**
 * Map column names back through a transform to their upstream equivalents.
 * Returns null if the mapping is ambiguous or the transform references all columns.
 */
function mapColumnsBackThroughTransform(
  node: ConstructNode,
  columns: Set<string>,
): Set<string> | null {
  switch (node.component) {
    case "Rename": {
      const renames = node.props.columns as Record<string, string> | undefined
      if (!renames) return null
      // Invert: newName → oldName
      const inverseMap = new Map<string, string>()
      for (const [oldName, newName] of Object.entries(renames)) {
        inverseMap.set(newName, oldName)
      }
      const mapped = new Set<string>()
      for (const col of columns) {
        mapped.add(inverseMap.get(col) ?? col)
      }
      return mapped
    }

    case "Filter":
    case "Coalesce":
    case "Deduplicate":
    case "TopN":
      // Passthrough — column names don't change
      return columns

    case "Map": {
      // Map creates new column names from expressions — need to trace expressions
      const select = node.props.select as Record<string, string> | undefined
      if (!select) return null
      const mapped = new Set<string>()
      for (const col of columns) {
        const expr = select[col]
        if (expr) {
          extractColumnsFromExpression(expr, mapped)
        }
      }
      return mapped
    }

    case "Aggregate": {
      // Aggregate output is groupBy + select — trace back to input columns
      const groupBy = node.props.groupBy as readonly string[] | undefined
      const select = node.props.select as Record<string, string> | undefined
      const mapped = new Set<string>()
      if (groupBy) {
        for (const col of columns) {
          if (groupBy.includes(col)) mapped.add(col)
        }
      }
      if (select) {
        for (const col of columns) {
          const expr = select[col]
          if (expr) {
            extractColumnsFromExpression(expr, mapped)
          }
        }
      }
      return mapped
    }

    default:
      // Unknown transform — can't map, go conservative
      return null
  }
}

/**
 * Narrow source DDL by replacing the source node's schema with only referenced columns.
 * Returns a new tree with narrowed source schemas.
 */
function runSchemaNarrowingPass(tree: ConstructNode): ConstructNode {
  const columnUsage = traceColumnUsage(tree)

  function narrowNode(node: ConstructNode): ConstructNode {
    if (node.kind === "Source") {
      const usedCols = columnUsage.get(node.id)

      // null means conservative — keep all columns
      // empty set means no downstream references found — keep all (safety)
      if (usedCols === null || usedCols === undefined || usedCols.size === 0) {
        return node
      }

      const schema = node.props.schema as
        | {
            fields: Record<string, string>
            watermark?: { column: string; expression: string }
            primaryKey?: { columns: readonly string[] }
            metadataColumns: readonly {
              column: string
              type: string
              key?: string
              isVirtual?: boolean
            }[]
          }
        | undefined
      if (!schema) return node

      // Always keep watermark column, primary key columns, and metadata columns
      const mustKeep = new Set<string>()
      if (schema.watermark) mustKeep.add(schema.watermark.column)
      if (schema.primaryKey) {
        for (const col of schema.primaryKey.columns) mustKeep.add(col)
      }
      for (const meta of schema.metadataColumns) {
        mustKeep.add(meta.column)
      }

      // Merge must-keep with used columns
      const keepCols = new Set([...usedCols, ...mustKeep])

      // Check if all columns are used — no narrowing needed
      const allFields = Object.keys(schema.fields)
      if (allFields.every((f) => keepCols.has(f))) return node

      // Narrow the fields
      const narrowedFields: Record<string, string> = {}
      for (const [name, type] of Object.entries(schema.fields)) {
        if (keepCols.has(name)) {
          narrowedFields[name] = type
        }
      }

      const narrowedSchema = { ...schema, fields: narrowedFields }
      return {
        ...node,
        props: { ...node.props, schema: narrowedSchema },
      }
    }

    // Recurse for non-source nodes
    const newChildren = node.children.map(narrowNode)
    if (newChildren.every((c, i) => c === node.children[i])) return node
    return { ...node, children: newChildren }
  }

  return narrowNode(tree)
}

// ── Pass: Dead branch elimination ────────────────────────────────────

/**
 * Evaluate whether a SQL condition is statically false.
 * Only handles trivially false cases — no expression evaluation.
 */
function isStaticallyFalse(condition: string): boolean {
  const trimmed = condition.trim().toLowerCase()

  // Direct boolean false
  if (trimmed === "false") return true

  // Numeric equality contradictions: 1 = 0, 0 = 1
  const eqMatch = trimmed.match(/^(\d+)\s*=\s*(\d+)$/)
  if (eqMatch && eqMatch[1] !== eqMatch[2]) return true

  // String equality contradictions: 'a' = 'b'
  const strMatch = trimmed.match(/^'([^']*)'\s*=\s*'([^']*)'$/)
  if (strMatch && strMatch[1] !== strMatch[2]) return true

  return false
}

/**
 * Remove Route branches whose conditions are statically false.
 * Returns a new tree with dead branches eliminated.
 */
function runDeadBranchEliminationPass(tree: ConstructNode): ConstructNode {
  function processNode(node: ConstructNode): ConstructNode {
    if (node.component === "Route") {
      const liveBranches = node.children.filter((child) => {
        if (child.component === "Route.Branch") {
          const condition = child.props.condition as string | undefined
          if (condition && isStaticallyFalse(condition)) {
            return false // eliminate this branch
          }
        }
        return true // keep Route.Default and live branches
      })

      if (liveBranches.length === node.children.length) return node

      return {
        ...node,
        children: liveBranches.map(processNode),
      }
    }

    // Recurse for all other nodes
    const newChildren = node.children.map(processNode)
    if (newChildren.every((c, i) => c === node.children[i])) return node
    return { ...node, children: newChildren }
  }

  return processNode(tree)
}

// ── Extract user-defined SET keys ────────────────────────────────────

/**
 * Extract the set of config keys that the user has explicitly set
 * via pipeline props (flinkConfig, checkpoint, etc).
 */
function extractUserSetKeys(pipeline: ConstructNode): Set<string> {
  const keys = new Set<string>()
  const props = pipeline.props

  if (props.name) keys.add("pipeline.name")
  if (props.mode) keys.add("execution.runtime-mode")
  if (props.parallelism !== undefined) keys.add("parallelism.default")
  if (props.stateBackend) keys.add("state.backend.type")
  if (props.stateTtl) keys.add("table.exec.state.ttl")

  const checkpoint = props.checkpoint as
    | { interval: string; mode?: string }
    | undefined
  if (checkpoint) {
    keys.add("execution.checkpointing.interval")
    if (checkpoint.mode) keys.add("execution.checkpointing.mode")
  }

  const rs = props.restartStrategy as { type: string } | undefined
  if (rs) {
    keys.add("restart-strategy.type")
  }

  // User-provided flinkConfig overrides everything
  const userConfig = props.flinkConfig as Record<string, string> | undefined
  if (userConfig) {
    for (const key of Object.keys(userConfig)) {
      keys.add(key)
    }
  }

  return keys
}

// ── Main optimizer entry point ───────────────────────────────────────

/**
 * Run optimization passes on a pipeline construct tree.
 *
 * Each pass is independent and individually togglable via OptimizeOptions.
 * Passes operate on the construct tree (pre-SQL) and produce:
 * - A potentially modified construct tree
 * - Additional SET statements to prepend
 * - Hint annotations for SQL generation
 *
 * User-defined SET statements always take precedence over auto-injected ones.
 */
export function optimizePipeline(
  pipelineNode: ConstructNode,
  version: FlinkMajorVersion,
  options: OptimizeOptions = {},
): OptimizationResult {
  const {
    setInjection = true,
    hintEmission = true,
    schemaNarrowing = true,
    deadBranchElimination = true,
  } = options

  let tree = pipelineNode
  let additionalSets: string[] = []
  let hintAnnotations = new Map<string, string>()

  // Extract user SET keys for precedence checking
  const userSets = extractUserSetKeys(pipelineNode)

  // Pass 1: SET statement injection
  if (setInjection) {
    additionalSets = runSetInjectionPass(tree, version, userSets)
  }

  // Pass 2: SQL hint emission
  if (hintEmission) {
    hintAnnotations = runHintEmissionPass(tree)
  }

  // Pass 3: Source schema narrowing
  if (schemaNarrowing) {
    tree = runSchemaNarrowingPass(tree)
  }

  // Pass 4: Dead branch elimination
  if (deadBranchElimination) {
    tree = runDeadBranchEliminationPass(tree)
  }

  return {
    tree,
    additionalSets,
    hintAnnotations,
  }
}

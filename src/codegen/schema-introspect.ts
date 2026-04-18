import type { SchemaDefinition } from "@/core/schema.js"
import { SynthContext } from "@/core/synth-context.js"
import type { ConstructNode } from "@/core/types.js"

// ── Shared types ────────────────────────────────────────────────────

export interface ResolvedColumn {
  readonly name: string
  readonly type: string
}

// ── Expression type inference ───────────────────────────────────────

/**
 * Infer the Flink SQL output type of an expression given upstream field types.
 *
 * Handles common aggregate functions (SUM, COUNT, AVG, MIN, MAX) and
 * simple column references. Falls back to STRING for complex expressions.
 */
export function inferExpressionType(
  expr: string,
  upstreamFields: Map<string, string>,
): string {
  const trimmed = expr.trim()

  // Simple column reference (possibly backtick-quoted)
  const colName = trimmed.replace(/^`|`$/g, "")
  if (upstreamFields.has(colName)) {
    return upstreamFields.get(colName)!
  }

  // COUNT(*) / COUNT(col)
  if (/^COUNT\s*\(/i.test(trimmed)) {
    return "BIGINT"
  }

  // SUM(col) — promote INT to BIGINT, keep DOUBLE/DECIMAL
  const sumMatch = trimmed.match(/^SUM\s*\(\s*`?(\w+)`?\s*\)/i)
  if (sumMatch) {
    const fieldType = upstreamFields.get(sumMatch[1]) ?? "BIGINT"
    if (
      fieldType === "INT" ||
      fieldType === "SMALLINT" ||
      fieldType === "TINYINT"
    )
      return "BIGINT"
    return fieldType
  }
  // SUM with complex expression (e.g. SUM(CASE WHEN ...)) — always numeric
  if (/^SUM\s*\(/i.test(trimmed)) {
    return "BIGINT"
  }

  // AVG(col) — always DOUBLE for integer types, keep DECIMAL
  const avgMatch = trimmed.match(/^AVG\s*\(\s*`?(\w+)`?\s*\)/i)
  if (avgMatch) {
    const fieldType = upstreamFields.get(avgMatch[1]) ?? "DOUBLE"
    if (fieldType.startsWith("DECIMAL")) return fieldType
    return "DOUBLE"
  }

  // STDDEV_POP/STDDEV_SAMP/VAR_POP/VAR_SAMP(col) — DOUBLE, preserve DECIMAL
  const varianceMatch = trimmed.match(
    /^(?:STDDEV_POP|STDDEV_SAMP|VAR_POP|VAR_SAMP)\s*\(\s*`?(\w+)`?\s*\)/i,
  )
  if (varianceMatch) {
    const fieldType = upstreamFields.get(varianceMatch[1]) ?? "DOUBLE"
    if (fieldType.startsWith("DECIMAL")) return fieldType
    return "DOUBLE"
  }

  // MATCH_RECOGNIZE FIRST/LAST(col [, N]) — same type as source. The
  // pattern-variable prefix is already stripped upstream by the
  // MatchRecognize case of resolveNodeSchema.
  const matchRecognizeNavMatch = trimmed.match(
    /^(?:FIRST|LAST)\s*\(\s*`?(\w+)`?\s*(?:,\s*\d+)?\s*\)/i,
  )
  if (matchRecognizeNavMatch) {
    return upstreamFields.get(matchRecognizeNavMatch[1]) ?? "STRING"
  }

  // MIN/MAX/FIRST_VALUE/LAST_VALUE(col) — same type as source
  const preserveTypeMatch = trimmed.match(
    /^(?:MIN|MAX|FIRST_VALUE|LAST_VALUE)\s*\(\s*`?(\w+)`?\s*\)/i,
  )
  if (preserveTypeMatch) {
    return upstreamFields.get(preserveTypeMatch[1]) ?? "STRING"
  }

  // TIMESTAMPDIFF — always returns INT
  if (/^TIMESTAMPDIFF\s*\(/i.test(trimmed)) {
    return "INT"
  }

  // Comparison operators — result is BOOLEAN
  if (
    /[><=!]/.test(trimmed) ||
    /\b(?:AND|OR|NOT|IN|BETWEEN|LIKE|IS)\b/i.test(trimmed)
  ) {
    return "BOOLEAN"
  }

  // Arithmetic expression (contains +, -, *, /) — infer from operand types
  if (/[+\-*/]/.test(trimmed)) {
    // Extract column references from the expression
    const colRefs =
      trimmed.match(/`?(\w+)`?/g)?.map((c) => c.replace(/`/g, "")) ?? []
    const operandTypes = colRefs
      .map((c) => upstreamFields.get(c))
      .filter(Boolean) as string[]
    if (operandTypes.length > 0) {
      // If any operand is DECIMAL, result is DECIMAL; if DOUBLE, result is DOUBLE; else BIGINT
      if (operandTypes.some((t) => t.startsWith("DECIMAL"))) {
        return operandTypes.find((t) => t.startsWith("DECIMAL")) ?? "DECIMAL"
      }
      if (operandTypes.some((t) => t === "DOUBLE")) return "DOUBLE"
      return "BIGINT"
    }
  }

  // Fallback
  return "STRING"
}

// ── Node schema resolution ──────────────────────────────────────────

/**
 * Resolve the output schema of a node by tracing upstream through transforms.
 * Returns column definitions or null if schema cannot be inferred.
 */
export function resolveNodeSchema(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): ResolvedColumn[] | null {
  switch (node.component) {
    case "KafkaSource":
    case "JdbcSource":
    case "GenericSource":
    case "DataGenSource": {
      const schema = node.props.schema as SchemaDefinition | undefined
      if (!schema) return null
      return Object.entries(schema.fields).map(([name, type]) => ({
        name,
        type: type as string,
      }))
    }

    // Passthrough transforms — same schema as upstream
    case "Filter":
      return node.children.length > 0
        ? resolveNodeSchema(node.children[0], nodeIndex)
        : null

    // Deduplicate/TopN add a rownum column via ROW_NUMBER()
    case "Deduplicate":
    case "TopN": {
      const upstreamSchema =
        node.children.length > 0
          ? resolveNodeSchema(node.children[0], nodeIndex)
          : null
      if (!upstreamSchema) return null
      return [...upstreamSchema, { name: "rownum", type: "BIGINT" }]
    }

    // Window nodes: resolve child schema + append window_start/window_end
    case "TumbleWindow":
    case "SlideWindow":
    case "SessionWindow": {
      // Window typically wraps an Aggregate child
      const aggChild = node.children.find((c) => c.component === "Aggregate")
      if (aggChild) {
        // Resolve the aggregate against its own upstream (the source inside the window)
        const aggSchema = resolveNodeSchema(aggChild, nodeIndex)
        if (!aggSchema) return null
        return [
          ...aggSchema,
          { name: "window_start", type: "TIMESTAMP(3)" },
          { name: "window_end", type: "TIMESTAMP(3)" },
        ]
      }
      // No aggregate child — passthrough with window columns
      const childSchema =
        node.children.length > 0
          ? resolveNodeSchema(node.children[0], nodeIndex)
          : null
      if (!childSchema) return null
      return [
        ...childSchema,
        { name: "window_start", type: "TIMESTAMP(3)" },
        { name: "window_end", type: "TIMESTAMP(3)" },
      ]
    }

    // Rename — same fields with some names changed
    case "Rename": {
      const columns = node.props.columns as Record<string, string>
      const upstreamSchema =
        node.children.length > 0
          ? resolveNodeSchema(node.children[0], nodeIndex)
          : null
      if (!upstreamSchema) return null
      return upstreamSchema.map((c) => ({
        name: columns[c.name] ?? c.name,
        type: c.type,
      }))
    }

    // Drop — exclude listed fields
    case "Drop": {
      const dropCols = new Set(node.props.columns as readonly string[])
      const upstreamSchema =
        node.children.length > 0
          ? resolveNodeSchema(node.children[0], nodeIndex)
          : null
      if (!upstreamSchema) return null
      return upstreamSchema.filter((c) => !dropCols.has(c.name))
    }

    // Cast — same fields with some types changed
    case "Cast": {
      const castCols = node.props.columns as Record<string, string>
      const upstreamSchema =
        node.children.length > 0
          ? resolveNodeSchema(node.children[0], nodeIndex)
          : null
      if (!upstreamSchema) return null
      return upstreamSchema.map((c) => ({
        name: c.name,
        type: castCols[c.name] ?? c.type,
      }))
    }

    // Coalesce — passthrough (same schema)
    case "Coalesce":
      return node.children.length > 0
        ? resolveNodeSchema(node.children[0], nodeIndex)
        : null

    // AddField — upstream fields + new computed fields
    case "AddField": {
      const addCols = node.props.columns as Record<string, string>
      const types = node.props.types as Record<string, string> | undefined
      const upstreamSchema =
        node.children.length > 0
          ? resolveNodeSchema(node.children[0], nodeIndex)
          : null
      const upstreamFields = new Map(
        upstreamSchema?.map((c) => [c.name, c.type]) ?? [],
      )
      const base = upstreamSchema ?? []
      const added = Object.entries(addCols).map(([name, expr]) => ({
        name,
        type: types?.[name] ?? inferExpressionType(expr, upstreamFields),
      }))
      return [...base, ...added]
    }

    // FlatMap — upstream fields + unnested fields
    case "FlatMap": {
      const asFields = node.props.as as Record<string, string>
      const upstreamSchema =
        node.children.length > 0
          ? resolveNodeSchema(node.children[0], nodeIndex)
          : null
      const base = upstreamSchema ?? []
      const unnested = Object.entries(asFields).map(([name, type]) => ({
        name,
        type,
      }))
      return [...base, ...unnested]
    }

    // Map — output columns from `select` keys, types inferred from upstream
    case "Map": {
      const select = node.props.select as Record<string, string>
      const upstreamSchema =
        node.children.length > 0
          ? resolveNodeSchema(node.children[0], nodeIndex)
          : null
      const upstreamFields = new Map(
        upstreamSchema?.map((c) => [c.name, c.type]) ?? [],
      )
      return Object.entries(select).map(([alias, expr]) => ({
        name: alias,
        type: inferExpressionType(expr, upstreamFields),
      }))
    }

    // Aggregate — groupBy fields + select aggregates
    case "Aggregate": {
      const groupBy = node.props.groupBy as readonly string[]
      const select = node.props.select as Record<string, string>
      const groupBySet = new Set(groupBy)
      const upstreamSchema =
        node.children.length > 0
          ? resolveNodeSchema(node.children[0], nodeIndex)
          : null
      const upstreamFields = new Map(
        upstreamSchema?.map((c) => [c.name, c.type]) ?? [],
      )

      const columns: ResolvedColumn[] = []
      // Add groupBy columns with original types
      for (const col of groupBy) {
        columns.push({
          name: col,
          type: upstreamFields.get(col) ?? "STRING",
        })
      }
      // Add aggregate select columns, skipping duplicates of groupBy columns
      for (const [alias, expr] of Object.entries(select)) {
        if (!groupBySet.has(alias)) {
          columns.push({
            name: alias,
            type: inferExpressionType(expr, upstreamFields),
          })
        }
      }
      // Forward-nesting (Aggregate { children: Window }): the codegen
      // emits `SELECT …, window_start, window_end FROM TABLE(TVF(…))`,
      // so these window metadata columns flow into the output. Reflect
      // them in the resolved schema so the auto-inferred sink DDL lines
      // up with the query result (BUG-030).
      const firstChild = node.children[0]
      if (
        firstChild &&
        (firstChild.component === "TumbleWindow" ||
          firstChild.component === "SlideWindow" ||
          firstChild.component === "SessionWindow")
      ) {
        const existing = new Set(columns.map((c) => c.name))
        for (const col of ["window_start", "window_end"]) {
          if (!existing.has(col)) {
            columns.push({ name: col, type: "TIMESTAMP(3)" })
          }
        }
      }
      return columns
    }

    // Joins — merge both sides' schemas, deduplicating columns that
    // appear on both sides (e.g. join key columns).
    case "Join":
    case "IntervalJoin": {
      const joinType = node.props.type as string | undefined
      const leftSchema = node.children[0]
        ? resolveNodeSchema(node.children[0], nodeIndex)
        : null
      // Anti/semi joins only produce left-side columns
      if (joinType === "anti" || joinType === "semi") return leftSchema
      const rightSchema = node.children[1]
        ? resolveNodeSchema(node.children[1], nodeIndex)
        : null
      if (!leftSchema) return rightSchema
      if (!rightSchema) return leftSchema
      const leftNames = new Set(leftSchema.map((c) => c.name))
      return [
        ...leftSchema,
        ...rightSchema.filter((c) => !leftNames.has(c.name)),
      ]
    }
    case "TemporalJoin": {
      const streamSchema = node.children[0]
        ? resolveNodeSchema(node.children[0], nodeIndex)
        : null
      const temporalSchema = node.children[1]
        ? resolveNodeSchema(node.children[1], nodeIndex)
        : null
      if (!streamSchema) return temporalSchema
      if (!temporalSchema) return streamSchema
      const streamNames = new Set(streamSchema.map((c) => c.name))
      return [
        ...streamSchema,
        ...temporalSchema.filter((c) => !streamNames.has(c.name)),
      ]
    }
    // LookupJoin — only input schema (lookup table is external)
    case "LookupJoin":
      return node.children[0]
        ? resolveNodeSchema(node.children[0], nodeIndex)
        : null

    // MatchRecognize — output schema from PARTITION BY + MEASURES
    case "MatchRecognize": {
      const measures = node.props.measures as Record<string, string> | undefined
      const partitionBy = node.props.partitionBy as
        | readonly string[]
        | undefined
      if (!measures) return null
      // Resolve the input source schema for type inference
      const inputSchema = node.children[0]
        ? resolveNodeSchema(node.children[0], nodeIndex)
        : null
      const sourceFields = new Map(
        inputSchema?.map((c) => [c.name, c.type]) ?? [],
      )
      const columns: ResolvedColumn[] = []
      // PARTITION BY columns are automatically included in output
      if (partitionBy) {
        for (const col of partitionBy) {
          columns.push({ name: col, type: sourceFields.get(col) ?? "STRING" })
        }
      }
      // MEASURES define the remaining output columns
      for (const [name, expr] of Object.entries(measures)) {
        // Strip pattern variable prefixes (e.g. `A.`, `pickup.`) so
        // inferExpressionType can look up base column names in the source
        // schema. Safe inside MatchRecognize because it operates on a
        // single input relation — any dotted prefix is a pattern variable.
        const stripped = expr.replace(/\b[A-Za-z_]\w*\./g, "")
        columns.push({
          name,
          type: inferExpressionType(stripped, sourceFields),
        })
      }
      return columns
    }

    // VirtualRef — internal node used by sibling/branch chaining
    case "VirtualRef": {
      // Schema carried from the previous transform in a sibling chain
      if (node.props._schema) return node.props._schema as ResolvedColumn[]
      // Simple reference — look up the actual node
      const actual = nodeIndex.get(node.id)
      if (actual && actual.component !== "VirtualRef") {
        return resolveNodeSchema(actual, nodeIndex)
      }
      return null
    }

    default:
      // Try first child
      return node.children.length > 0
        ? resolveNodeSchema(node.children[0], nodeIndex)
        : null
  }
}

// ── Transform schema resolution ─────────────────────────────────────

/**
 * Apply a transform to a schema, returning the output schema.
 */
export function resolveTransformSchema(
  transform: ConstructNode,
  inputSchema: ResolvedColumn[],
): ResolvedColumn[] | null {
  const upstreamFields = new Map(inputSchema.map((c) => [c.name, c.type]))

  switch (transform.component) {
    case "Filter":
      return inputSchema // passthrough

    // Deduplicate/TopN add a rownum column via ROW_NUMBER()
    case "Deduplicate":
    case "TopN":
      return [...inputSchema, { name: "rownum", type: "BIGINT" }]

    // Window nodes: append window_start/window_end
    case "TumbleWindow":
    case "SlideWindow":
    case "SessionWindow": {
      // Window typically wraps an Aggregate child
      const aggChild = transform.children?.find(
        (c) => c.component === "Aggregate",
      )
      if (aggChild) {
        // Resolve aggregate output from inputSchema
        const aggSchema = resolveTransformSchema(aggChild, inputSchema)
        if (!aggSchema) return null
        return [
          ...aggSchema,
          { name: "window_start", type: "TIMESTAMP(3)" },
          { name: "window_end", type: "TIMESTAMP(3)" },
        ]
      }
      // No aggregate — passthrough with window columns
      return [
        ...inputSchema,
        { name: "window_start", type: "TIMESTAMP(3)" },
        { name: "window_end", type: "TIMESTAMP(3)" },
      ]
    }

    // FlatMap — upstream fields + unnested fields
    case "FlatMap": {
      const asFields = transform.props.as as Record<string, string>
      const unnested = Object.entries(asFields).map(([name, type]) => ({
        name,
        type,
      }))
      return [...inputSchema, ...unnested]
    }

    case "Rename": {
      const columns = transform.props.columns as Record<string, string>
      return inputSchema.map((c) => ({
        name: columns[c.name] ?? c.name,
        type: c.type,
      }))
    }

    case "Drop": {
      const dropCols = new Set(transform.props.columns as readonly string[])
      return inputSchema.filter((c) => !dropCols.has(c.name))
    }

    case "Cast": {
      const castCols = transform.props.columns as Record<string, string>
      return inputSchema.map((c) => ({
        name: c.name,
        type: castCols[c.name] ?? c.type,
      }))
    }

    case "Coalesce":
      return inputSchema // passthrough

    case "AddField": {
      const addCols = transform.props.columns as Record<string, string>
      const types = transform.props.types as Record<string, string> | undefined
      const added = Object.entries(addCols).map(([name, expr]) => ({
        name,
        type: types?.[name] ?? inferExpressionType(expr, upstreamFields),
      }))
      return [...inputSchema, ...added]
    }

    case "Map": {
      const select = transform.props.select as Record<string, string>
      return Object.entries(select).map(([alias, expr]) => ({
        name: alias,
        type: inferExpressionType(expr, upstreamFields),
      }))
    }

    case "Aggregate": {
      const groupBy = transform.props.groupBy as readonly string[]
      const select = transform.props.select as Record<string, string>
      const groupBySet = new Set(groupBy)
      const columns: ResolvedColumn[] = []
      for (const col of groupBy) {
        columns.push({ name: col, type: upstreamFields.get(col) ?? "STRING" })
      }
      // Skip select entries that duplicate a groupBy column
      for (const [alias, expr] of Object.entries(select)) {
        if (!groupBySet.has(alias)) {
          columns.push({
            name: alias,
            type: inferExpressionType(expr, upstreamFields),
          })
        }
      }
      return columns
    }

    default:
      return inputSchema // assume passthrough for unknown transforms
  }
}

// ── Tree traversal helpers ──────────────────────────────────────────

/**
 * Find the deepest source node by walking down children.
 */
export function findDeepestSource(node: ConstructNode): ConstructNode | null {
  if (node.kind === "Source") return node
  for (const child of node.children) {
    const found = findDeepestSource(child)
    if (found) return found
  }
  return null
}

/**
 * Collect the transform/window chain from a node down to (but not including) the source.
 * Includes both Transform and Window nodes so changelog mode propagation can
 * distinguish windowed vs unbounded aggregations.
 */
export function collectTransformChain(node: ConstructNode): ConstructNode[] {
  const chain: ConstructNode[] = []
  let current: ConstructNode | undefined = node
  while (current && current.kind !== "Source") {
    if (current.kind === "Transform" || current.kind === "Window")
      chain.push(current)
    current = current.children[0]
  }
  return chain
}

/**
 * Find the upstream source/transform node for a Route by looking at preceding siblings.
 */
export function findRouteUpstreamNode(
  routeNode: ConstructNode,
  parent?: ConstructNode,
): ConstructNode | null {
  // Check non-Branch children first (programmatic pattern)
  const upstreamChildren = routeNode.children.filter(
    (c) => c.component !== "Route.Branch" && c.component !== "Route.Default",
  )
  if (upstreamChildren.length > 0) return upstreamChildren[0]

  // Look at preceding siblings in parent (JSX pattern)
  if (parent) {
    const routeIndex = parent.children.indexOf(routeNode)
    for (let i = routeIndex - 1; i >= 0; i--) {
      const sibling = parent.children[i]
      if (
        sibling.kind === "Source" ||
        sibling.kind === "Transform" ||
        sibling.kind === "Join" ||
        sibling.kind === "CEP"
      ) {
        return sibling
      }
    }
  }
  return null
}

// ── Tree indexing ───────────────────────────────────────────────────

export function indexTree(
  node: ConstructNode,
  index: Map<string, ConstructNode>,
): void {
  index.set(node.id, node)
  for (const child of node.children) {
    indexTree(child, index)
  }
}

// ── Pipeline schema introspection ───────────────────────────────────

export type ConstraintKind = "PK" | "WM" | "META" | "META VIRTUAL"

export interface IntrospectedColumn {
  readonly name: string
  readonly type: string
  readonly constraints: readonly ConstraintKind[]
}

export interface IntrospectedSchema {
  readonly nodeId: string
  readonly component: string
  readonly kind: "source" | "sink"
  readonly nameHint: string
  readonly columns: readonly IntrospectedColumn[]
}

/**
 * Introspect all source and sink schemas in a pipeline construct tree.
 *
 * Sources: reads SchemaDefinition directly from node props, including
 * constraint annotations (PK, watermark, metadata).
 *
 * Sinks: resolves schemas by tracing upstream through the construct tree,
 * handling both reverse-nesting and forward-reading JSX patterns.
 */
export function introspectPipelineSchemas(
  pipelineNode: ConstructNode,
): IntrospectedSchema[] {
  const nodeIndex = new Map<string, ConstructNode>()
  indexTree(pipelineNode, nodeIndex)

  const ctx = new SynthContext()
  ctx.buildFromTree(pipelineNode)

  const schemas: IntrospectedSchema[] = []

  // ── Sources ──────────────────────────────────────────────────
  const sources = ctx.getNodesByKind("Source")
  for (const src of sources) {
    if (src.component === "CatalogSource") continue

    const schemaDef = src.props.schema as SchemaDefinition | undefined
    if (!schemaDef) continue

    const pkSet = new Set(schemaDef.primaryKey?.columns ?? [])
    const propsKeySet = new Set(
      (src.props.primaryKey as readonly string[] | undefined) ?? [],
    )
    const wmCol = schemaDef.watermark?.column
    const _metaCols = new Map(
      schemaDef.metadataColumns.map((m) => [m.column, m.isVirtual]),
    )

    const columns: IntrospectedColumn[] = []

    // Regular fields
    for (const [name, type] of Object.entries(schemaDef.fields)) {
      const constraints: ConstraintKind[] = []
      if (pkSet.has(name) || propsKeySet.has(name)) constraints.push("PK")
      if (name === wmCol) constraints.push("WM")
      columns.push({ name, type: type as string, constraints })
    }

    // Metadata columns
    for (const meta of schemaDef.metadataColumns) {
      const constraints: ConstraintKind[] = [
        meta.isVirtual ? "META VIRTUAL" : "META",
      ]
      columns.push({
        name: meta.column,
        type: meta.type as string,
        constraints,
      })
    }

    schemas.push({
      nodeId: src.id,
      component: src.component,
      kind: "source",
      nameHint: (src.props._nameHint as string) ?? src.id,
      columns,
    })
  }

  // ── Sinks ────────────────────────────────────────────────────
  const sinkMetadata = resolveSinkSchemas(pipelineNode, nodeIndex)

  const sinks = ctx.getNodesByKind("Sink")
  for (const sink of sinks) {
    const meta = sinkMetadata.get(sink.id)
    if (!meta) {
      // Unresolvable sink schema
      schemas.push({
        nodeId: sink.id,
        component: sink.component,
        kind: "sink",
        nameHint: (sink.props._nameHint as string) ?? sink.id,
        columns: [],
      })
      continue
    }

    schemas.push({
      nodeId: sink.id,
      component: sink.component,
      kind: "sink",
      nameHint: (sink.props._nameHint as string) ?? sink.id,
      columns: meta.map((c) => ({
        name: c.name,
        type: c.type,
        constraints: [],
      })),
    })
  }

  return schemas
}

// ── Sink schema resolution (for introspection) ─────────────────────

/**
 * Walk the pipeline tree to resolve output schemas for all sinks.
 * Returns columns only (no changelog mode tracking — that's sql-generator's concern).
 */
function resolveSinkSchemas(
  pipelineNode: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): Map<string, ResolvedColumn[]> {
  const result = new Map<string, ResolvedColumn[]>()

  function walk(node: ConstructNode, parent?: ConstructNode): void {
    if (node.kind === "Sink") {
      // Pattern 1: sink has children (reverse-nesting)
      if (node.children.length > 0) {
        const schema = resolveNodeSchema(node.children[0], nodeIndex)
        if (schema) result.set(node.id, schema)
        return
      }
      // Pattern 2: forward-reading JSX — resolve from preceding siblings
      if (parent) {
        const sinkIndex = parent.children.indexOf(node)
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
          // Propagate through intermediate transforms after the starting node
          let schema: ResolvedColumn[] | null = startSchema
          for (let j = i + 1; j < sinkIndex; j++) {
            const transform = parent.children[j]
            if (
              (transform.kind === "Transform" || transform.kind === "Window") &&
              schema
            ) {
              schema = resolveTransformSchema(transform, schema)
            }
          }
          if (schema) result.set(node.id, schema)
          break
        }
      }
      return
    }

    if (node.component === "Route") {
      // Find upstream source for the Route
      const routeUpstream = findRouteUpstreamNode(node, parent)
      const routeSchema = routeUpstream
        ? resolveNodeSchema(routeUpstream, nodeIndex)
        : null

      // Resolve schema for each sink in each branch
      const branches = node.children.filter(
        (c) =>
          c.component === "Route.Branch" || c.component === "Route.Default",
      )
      for (const branch of branches) {
        const transforms = branch.children.filter((c) => c.kind !== "Sink")
        const sinks = branch.children.filter((c) => c.kind === "Sink")

        let schema = routeSchema
        for (const transform of transforms) {
          if (!schema) break
          schema = resolveTransformSchema(transform, schema)
        }

        if (schema) {
          for (const sink of sinks) {
            result.set(sink.id, schema)
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

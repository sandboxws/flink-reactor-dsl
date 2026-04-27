import {
  indexTree,
  type ResolvedColumn,
  resolveNodeSchema,
  resolveTransformSchema,
} from "@/codegen/schema-introspect.js"
import { validateSqlExpression } from "./sql-expr-validator.js"
import type {
  ValidationCategory,
  ValidationDiagnostic,
  ValidationDiagnosticDetails,
} from "./synth-context.js"
import type { ConstructNode } from "./types.js"

// ── SQL keywords to exclude from bare identifier matching ──────────

const SQL_KEYWORDS: ReadonlySet<string> = new Set([
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
  "FROM",
  "WHERE",
  "SELECT",
  "GROUP",
  "BY",
  "ORDER",
  "ASC",
  "DESC",
  "HAVING",
  "LIMIT",
  "OFFSET",
  "JOIN",
  "ON",
  "LEFT",
  "RIGHT",
  "INNER",
  "OUTER",
  "FULL",
  "CROSS",
  "UNION",
  "ALL",
  "DISTINCT",
  "EXISTS",
  "CAST",
  "TIMESTAMP",
  "DATE",
  "TIME",
  "INTERVAL",
  "ROW",
  "ARRAY",
  "MAP",
  "OVER",
  "PARTITION",
  "ROWS",
  "RANGE",
  "UNBOUNDED",
  "PRECEDING",
  "FOLLOWING",
  "CURRENT",
  "COUNT",
  "SUM",
  "AVG",
  "MIN",
  "MAX",
  "FIRST_VALUE",
  "LAST_VALUE",
  "LAG",
  "LEAD",
  "ROW_NUMBER",
  "RANK",
  "DENSE_RANK",
  "COALESCE",
  "IF",
  "NULLIF",
  "IFNULL",
  "CONCAT",
  "SUBSTRING",
  "TRIM",
  "UPPER",
  "LOWER",
  "LENGTH",
  "REPLACE",
  "REGEXP",
  "FLOOR",
  "CEIL",
  "ROUND",
  "ABS",
  "MOD",
  "POWER",
  "SQRT",
  "LOG",
  "LN",
  "EXP",
  "CURRENT_TIMESTAMP",
  "CURRENT_DATE",
  "CURRENT_TIME",
  "LOCALTIME",
  "LOCALTIMESTAMP",
  "NOW",
  "TO_TIMESTAMP",
  "TO_DATE",
  "DATE_FORMAT",
  "TIMESTAMPDIFF",
  "TIMESTAMPADD",
  "EXTRACT",
  "YEAR",
  "MONTH",
  "DAY",
  "HOUR",
  "MINUTE",
  "SECOND",
])

// ── Column reference extraction ─────────────────────────────────────

/**
 * Extract column references from a SQL expression.
 *
 * Strategy:
 * 1. Extract backtick-quoted identifiers (definitive column references)
 * 2. Match bare identifiers against the known column set (best-effort)
 * 3. Exclude SQL keywords and numeric/string literals
 *
 * Returns deduplicated list of referenced column names.
 */
export function extractColumnReferences(
  expr: string,
  knownColumns: string[],
): string[] {
  const refs = new Set<string>()

  // 1. Backtick-quoted identifiers: `column_name`
  const backtickPattern = /`([^`]+)`/g
  let match: RegExpExecArray | null
  while ((match = backtickPattern.exec(expr)) !== null) {
    refs.add(match[1])
  }

  // 2. Bare identifiers matched against known columns
  const knownSet = new Set(knownColumns)
  // Remove string literals and backtick-quoted sections to avoid false matches
  const cleaned = expr
    .replace(/`[^`]+`/g, "") // remove backtick-quoted
    .replace(/'[^']*'/g, "") // remove string literals
  const barePattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g
  while ((match = barePattern.exec(cleaned)) !== null) {
    const ident = match[1]
    // Skip SQL keywords (case-insensitive)
    if (SQL_KEYWORDS.has(ident.toUpperCase())) continue
    // Only include if it's a known column
    if (knownSet.has(ident)) {
      refs.add(ident)
    }
  }

  return [...refs]
}

// ── Schema validation ───────────────────────────────────────────────

/**
 * Validate column references in transform props against resolved upstream schemas.
 *
 * Handles two construct tree patterns:
 *
 * 1. **Nesting pattern**: `<Source><Filter><Sink/></Filter></Source>`
 *    Parent is upstream of children. Schema propagates from parent → child.
 *
 * 2. **Sibling pattern**: `<Pipeline><Source/><Filter/><Sink/></Pipeline>`
 *    Preceding siblings are upstream. Schema propagates left → right.
 */
export function validateSchemaReferences(
  pipelineNode: ConstructNode,
): ValidationDiagnostic[] {
  const nodeIndex = new Map<string, ConstructNode>()
  indexTree(pipelineNode, nodeIndex)

  const diagnostics: ValidationDiagnostic[] = []

  /**
   * Walk the tree with a propagated schema context.
   * `inheritedSchema` is the schema from the upstream context (parent or preceding sibling).
   */
  function walk(
    node: ConstructNode,
    inheritedSchema: ResolvedColumn[] | null,
  ): void {
    let currentSchema = inheritedSchema

    // If this node is a Source, resolve its own schema
    if (node.kind === "Source") {
      currentSchema = resolveNodeSchema(node, nodeIndex)
    }

    // If this node is a Transform or Join, validate its column references
    if (node.kind === "Transform" || node.kind === "Join") {
      // Explicit children are an authoritative upstream declaration — prefer
      // them over an inherited schema from an enclosing context. Example:
      // `Aggregate({ children: windowed })` nested inside a LookupJoin's
      // subtree must validate `groupBy`/`select` against `windowed`'s output,
      // not the Join's inherited schema (which would be the *other* join
      // input). We resolve child[0]'s OUTPUT, which is this node's INPUT —
      // resolving the node itself would yield the transform's output and
      // miss columns the node's expressions reference upstream.
      if (node.children.length > 0) {
        const upstreamSchema = resolveNodeSchema(node.children[0], nodeIndex)
        if (upstreamSchema !== null) currentSchema = upstreamSchema
      }
      // Fall back to resolveNodeSchema when nothing is inherited (e.g. the
      // Sink → Filter → Source reverse-nesting pattern with no parent).
      if (currentSchema === null) {
        currentSchema = resolveNodeSchema(node, nodeIndex)
      }
      // For Joins, also try resolving from children (join inputs)
      if (
        node.kind === "Join" &&
        currentSchema === null &&
        node.children.length > 0
      ) {
        const childSchemas = node.children
          .map((c) => resolveNodeSchema(c, nodeIndex))
          .filter((s): s is ResolvedColumn[] => s !== null)
        if (childSchemas.length > 0) {
          currentSchema = childSchemas.flat()
        }
      }

      if (currentSchema === null) {
        diagnostics.push({
          severity: "warning",
          message: `Cannot resolve upstream schema for '${node.component}' (${node.id}) — skipping column reference validation`,
          nodeId: node.id,
          component: node.component,
          category: "schema",
        })
      } else {
        const columnNames = currentSchema.map((c) => c.name)
        diagnostics.push(...validateNodeColumnReferences(node, columnNames))
      }
      // Propagate schema through this transform for downstream children
      if (currentSchema) {
        currentSchema = resolveTransformSchema(node, currentSchema)
      }
    }

    // Process children — handle sibling chain propagation
    // Children are processed left-to-right; each sibling inherits the schema
    // from the previous sibling (sibling pattern) or from the parent (nesting pattern).
    let siblingSchema = currentSchema
    for (const child of node.children) {
      walk(child, siblingSchema)
      // After walking a Source or RawSQL child, update sibling schema for
      // next siblings — both contribute their own declared output schema.
      if (child.kind === "Source" || child.kind === "RawSQL") {
        siblingSchema = resolveNodeSchema(child, nodeIndex)
      } else if (child.kind === "Transform" && siblingSchema) {
        siblingSchema = resolveTransformSchema(child, siblingSchema)
      }
    }
  }

  walk(pipelineNode, null)
  return diagnostics
}

// ── Per-component column validation ─────────────────────────────────

function validateNodeColumnReferences(
  node: ConstructNode,
  availableColumns: string[],
): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = []
  const colSet = new Set(availableColumns)

  const makeDiag = (
    severity: "error" | "warning",
    referencedColumn: string,
    context: string,
  ): ValidationDiagnostic => ({
    severity,
    message: `${node.component} '${node.id}' references unknown column '${referencedColumn}' in ${context}. Available columns: [${availableColumns.join(", ")}]`,
    nodeId: node.id,
    component: node.component,
    category: "schema" as ValidationCategory,
    details: {
      availableColumns,
      referencedColumn,
    },
  })

  switch (node.component) {
    // ── Array-lookup components ──────────────────────────────────
    case "Deduplicate": {
      const key = node.props.key as readonly string[] | undefined
      const order = node.props.order as string | undefined
      if (key) {
        for (const col of key) {
          if (!colSet.has(col)) diagnostics.push(makeDiag("error", col, "key"))
        }
      }
      if (order) {
        // order is a single column name, optionally with ASC/DESC suffix
        const colName = order.replace(/\s+(ASC|DESC)$/i, "").trim()
        if (!colSet.has(colName))
          diagnostics.push(makeDiag("error", colName, "order"))
      }
      break
    }

    case "TopN": {
      const partitionBy = node.props.partitionBy as
        | readonly string[]
        | undefined
      const orderBy = node.props.orderBy as
        | Record<string, "ASC" | "DESC">
        | undefined
      if (partitionBy) {
        for (const col of partitionBy) {
          if (!colSet.has(col))
            diagnostics.push(makeDiag("error", col, "partitionBy"))
        }
      }
      if (orderBy) {
        for (const col of Object.keys(orderBy)) {
          if (!colSet.has(col))
            diagnostics.push(makeDiag("error", col, "orderBy"))
        }
      }
      break
    }

    case "Drop": {
      const columns = node.props.columns as readonly string[] | undefined
      if (columns) {
        for (const col of columns) {
          if (!colSet.has(col))
            diagnostics.push(makeDiag("error", col, "columns"))
        }
      }
      break
    }

    case "Rename": {
      const columns = node.props.columns as Record<string, string> | undefined
      if (columns) {
        for (const oldName of Object.keys(columns)) {
          if (!colSet.has(oldName))
            diagnostics.push(makeDiag("error", oldName, "columns"))
        }
      }
      break
    }

    case "Cast": {
      const columns = node.props.columns as Record<string, string> | undefined
      if (columns) {
        for (const col of Object.keys(columns)) {
          if (!colSet.has(col))
            diagnostics.push(makeDiag("error", col, "columns"))
        }
      }
      break
    }

    case "Coalesce": {
      const columns = node.props.columns as Record<string, string> | undefined
      if (columns) {
        for (const col of Object.keys(columns)) {
          if (!colSet.has(col))
            diagnostics.push(makeDiag("error", col, "columns"))
        }
      }
      break
    }

    // ── Expression-extraction components ─────────────────────────
    case "Filter": {
      const condition = node.props.condition as string | undefined
      if (condition) {
        const refs = extractColumnReferences(condition, availableColumns)
        for (const ref of refs) {
          if (!colSet.has(ref))
            diagnostics.push(makeDiag("error", ref, "condition"))
        }
      }
      break
    }

    case "Map": {
      const select = node.props.select as Record<string, string> | undefined
      if (select) {
        for (const [alias, expr] of Object.entries(select)) {
          const refs = extractColumnReferences(expr, availableColumns)
          for (const ref of refs) {
            if (!colSet.has(ref))
              diagnostics.push(makeDiag("error", ref, `select.${alias}`))
          }
        }
      }
      break
    }

    case "Aggregate": {
      const groupBy = node.props.groupBy as readonly string[] | undefined
      const select = node.props.select as Record<string, string> | undefined
      if (groupBy) {
        for (const col of groupBy) {
          if (!colSet.has(col))
            diagnostics.push(makeDiag("error", col, "groupBy"))
        }
      }
      if (select) {
        for (const [alias, expr] of Object.entries(select)) {
          const refs = extractColumnReferences(expr, availableColumns)
          for (const ref of refs) {
            if (!colSet.has(ref))
              diagnostics.push(makeDiag("error", ref, `select.${alias}`))
          }
        }
      }
      break
    }

    case "Join": {
      const on = node.props.on as string | undefined
      if (on) {
        const refs = extractColumnReferences(on, availableColumns)
        for (const ref of refs) {
          if (!colSet.has(ref)) diagnostics.push(makeDiag("error", ref, "on"))
        }
      }
      break
    }

    case "Validate": {
      const rules = node.props.rules as
        | { notNull?: readonly string[]; range?: Record<string, unknown> }
        | undefined
      if (rules?.notNull) {
        for (const col of rules.notNull) {
          if (!colSet.has(col))
            diagnostics.push(makeDiag("error", col, "rules.notNull"))
        }
      }
      if (rules?.range) {
        for (const col of Object.keys(rules.range)) {
          if (!colSet.has(col))
            diagnostics.push(makeDiag("error", col, "rules.range"))
        }
      }
      break
    }
  }

  return diagnostics
}

// ── Expression syntax validation ─────────────────────────────────────

/**
 * Expression-type props by component.
 * Maps component name → array of prop paths that contain SQL expressions.
 */
const EXPRESSION_PROPS: Record<string, string[]> = {
  Filter: ["condition"],
  Map: ["select.*"],
  Join: ["on"],
  Validate: ["rules.*"],
  Qualify: ["condition"],
  "Query.Where": ["condition"],
  "Query.Having": ["condition"],
}

/**
 * Extract expression strings from a node based on EXPRESSION_PROPS config.
 * Returns array of `{ propPath, expr }`.
 */
function extractExpressions(
  node: ConstructNode,
): Array<{ propPath: string; expr: string }> {
  const propPaths = EXPRESSION_PROPS[node.component]
  if (!propPaths) return []

  const result: Array<{ propPath: string; expr: string }> = []

  for (const path of propPaths) {
    if (path.endsWith(".*")) {
      const baseProp = path.slice(0, -2)
      const value = node.props[baseProp]
      if (value && typeof value === "object") {
        for (const [key, expr] of Object.entries(
          value as Record<string, unknown>,
        )) {
          if (typeof expr === "string") {
            result.push({ propPath: `${baseProp}.${key}`, expr })
          }
        }
      }
    } else {
      const value = node.props[path]
      if (typeof value === "string") {
        result.push({ propPath: path, expr: value })
      }
    }
  }

  return result
}

/**
 * Validate SQL expression syntax for all expression-type props in a pipeline.
 *
 * Walks the construct tree, finds components with expression props
 * (Filter.condition, Map.select values, Join.on, etc.), and validates
 * each expression using dt-sql-parser's FlinkSQL parser.
 *
 * Returns diagnostics with category "expression" and details.expressionErrors.
 */
export async function validateExpressionSyntax(
  pipelineNode: ConstructNode,
): Promise<ValidationDiagnostic[]> {
  const diagnostics: ValidationDiagnostic[] = []
  const nodes: ConstructNode[] = []

  // Collect all nodes
  function collect(node: ConstructNode): void {
    nodes.push(node)
    for (const child of node.children) {
      collect(child)
    }
  }
  collect(pipelineNode)

  // Validate expressions in each node
  for (const node of nodes) {
    const expressions = extractExpressions(node)
    for (const { propPath, expr } of expressions) {
      const result = await validateSqlExpression(expr)
      if (!result.valid) {
        const expressionErrors = result.errors.map(
          (e) => `col ${e.startColumn}: ${e.message}`,
        )
        diagnostics.push({
          severity: "warning",
          message: `${node.component} '${node.id}' has invalid SQL syntax in ${propPath}: ${expressionErrors.join("; ")}`,
          nodeId: node.id,
          component: node.component,
          category: "expression" as ValidationCategory,
          details: {
            expressionErrors,
          } as ValidationDiagnosticDetails,
        })
      }
    }
  }

  return diagnostics
}

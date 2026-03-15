// ── SQL Expression Validator ─────────────────────────────────────────
//
// Validates SQL expression syntax offline using dt-sql-parser's FlinkSQL
// parser. Wraps bare expressions in a dummy SELECT statement before
// parsing, then adjusts error offsets back to the original expression.

// ── Types ────────────────────────────────────────────────────────────

export interface ExprSyntaxError {
  readonly message: string
  readonly startColumn: number
  readonly endColumn: number
  readonly startLine: number
  readonly endLine: number
}

export interface ExprValidationResult {
  readonly valid: boolean
  readonly errors: readonly ExprSyntaxError[]
}

// ── Wrapping constants ──────────────────────────────────────────────

const WRAPPER_PREFIX = "SELECT * FROM __dummy WHERE "
const WRAPPER_PREFIX_LEN = WRAPPER_PREFIX.length

// ── Lazy-loaded parser singleton ────────────────────────────────────

type FlinkSQLParser = {
  validate(sql: string): Array<{
    startLine: number
    endLine: number
    startColumn: number
    endColumn: number
    message: string
  }>
}

let cachedParser: FlinkSQLParser | null = null

async function getParser(): Promise<FlinkSQLParser> {
  if (cachedParser) return cachedParser

  const mod = await import("dt-sql-parser")
  // dt-sql-parser exports via CJS-style default when dynamically imported
  const FlinkSQL =
    (mod as Record<string, unknown>).FlinkSQL ??
    ((mod as { default?: Record<string, unknown> }).default?.FlinkSQL as
      | (new () => FlinkSQLParser)
      | undefined)

  if (!FlinkSQL) {
    throw new Error(
      "Failed to import FlinkSQL from dt-sql-parser — unexpected module shape",
    )
  }

  cachedParser = new (FlinkSQL as new () => FlinkSQLParser)()
  return cachedParser
}

// ── Public API ──────────────────────────────────────────────────────

/**
 * Validate a bare SQL expression (e.g. `amount > 100`, `CONCAT(a, b)`)
 * by wrapping it in a dummy SELECT statement and parsing via FlinkSQL.
 *
 * Returns adjusted error offsets that map back to the original expression.
 */
export async function validateSqlExpression(
  expr: string,
): Promise<ExprValidationResult> {
  const trimmed = expr.trim()

  if (trimmed === "") {
    return {
      valid: false,
      errors: [
        {
          message: "Expression is empty",
          startColumn: 0,
          endColumn: 0,
          startLine: 1,
          endLine: 1,
        },
      ],
    }
  }

  const parser = await getParser()
  const wrappedSql = `${WRAPPER_PREFIX}${trimmed}`
  const rawErrors = parser.validate(wrappedSql)

  if (rawErrors.length === 0) {
    return { valid: true, errors: [] }
  }

  const adjustedErrors: ExprSyntaxError[] = rawErrors.map((err) => ({
    message: err.message,
    startLine: err.startLine,
    endLine: err.endLine,
    // Adjust column offsets for line 1 (where the prefix lives)
    startColumn:
      err.startLine === 1
        ? Math.max(0, err.startColumn - WRAPPER_PREFIX_LEN)
        : err.startColumn,
    endColumn:
      err.endLine === 1
        ? Math.max(0, err.endColumn - WRAPPER_PREFIX_LEN)
        : err.endColumn,
  }))

  return { valid: false, errors: adjustedErrors }
}

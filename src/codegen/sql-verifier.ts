// ── Post-Generation SQL Verifier ────────────────────────────────────
//
// Tier 1: Always-on structural checks (string scanning, no parsing).
// Tier 2: Opt-in deep checks via dt-sql-parser and SQL Gateway EXPLAIN.

import type { ValidationDiagnostic } from "@/core/synth-context.js"

// ── Tier 1: Static structural checks ───────────────────────────────

/**
 * Verify generated SQL statements for structural correctness.
 *
 * Lightweight string-based checks that catch common codegen bugs:
 * - INSERT INTO targets without a matching CREATE TABLE
 * - Unbalanced parentheses (respecting string literal boundaries)
 * - Unterminated backtick-quoted identifiers
 * - Empty statements
 */
export function verifySql(
  statements: readonly string[],
): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = []

  diagnostics.push(...checkInsertCreatePairing(statements))
  diagnostics.push(...checkBalancedParentheses(statements))
  diagnostics.push(...checkBacktickTermination(statements))
  diagnostics.push(...checkEmptyStatements(statements))

  return diagnostics
}

// ── INSERT INTO / CREATE TABLE pairing ─────────────────────────────

const CREATE_TABLE_RE =
  /CREATE\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`([^`]+)`/gi
const INSERT_INTO_RE = /INSERT\s+INTO\s+`([^`]+)`/gi

function extractTableNames(
  statements: readonly string[],
  regex: RegExp,
): Set<string> {
  const names = new Set<string>()
  for (const stmt of statements) {
    regex.lastIndex = 0
    for (
      let match = regex.exec(stmt);
      match !== null;
      match = regex.exec(stmt)
    ) {
      names.add(match[1])
    }
  }
  return names
}

function checkInsertCreatePairing(
  statements: readonly string[],
): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = []
  const createdTables = extractTableNames(statements, CREATE_TABLE_RE)
  const insertTargets = extractTableNames(statements, INSERT_INTO_RE)

  for (const target of insertTargets) {
    if (!createdTables.has(target)) {
      diagnostics.push({
        severity: "error",
        message: `INSERT INTO target \`${target}\` has no matching CREATE TABLE`,
        category: "sql",
      })
    }
  }

  return diagnostics
}

// ── Balanced parentheses ───────────────────────────────────────────

function checkBalancedParentheses(
  statements: readonly string[],
): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = []

  for (const stmt of statements) {
    let depth = 0
    let inSingleQuote = false
    let i = 0

    while (i < stmt.length) {
      const ch = stmt[i]

      if (inSingleQuote) {
        if (ch === "'" && stmt[i + 1] === "'") {
          // Escaped single quote inside string literal
          i += 2
          continue
        }
        if (ch === "'") {
          inSingleQuote = false
        }
        i++
        continue
      }

      if (ch === "'") {
        inSingleQuote = true
      } else if (ch === "(") {
        depth++
      } else if (ch === ")") {
        depth--
        if (depth < 0) break
      }
      i++
    }

    if (depth !== 0) {
      diagnostics.push({
        severity: "error",
        message: `Unbalanced parentheses in statement: ${stmt.slice(0, 60)}...`,
        category: "sql",
      })
    }
  }

  return diagnostics
}

// ── Backtick-quoted identifier termination ──────────────────────────

function checkBacktickTermination(
  statements: readonly string[],
): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = []

  for (const stmt of statements) {
    let inSingleQuote = false
    let inBacktick = false
    let i = 0

    while (i < stmt.length) {
      const ch = stmt[i]

      if (inSingleQuote) {
        if (ch === "'" && stmt[i + 1] === "'") {
          i += 2
          continue
        }
        if (ch === "'") {
          inSingleQuote = false
        }
        i++
        continue
      }

      if (inBacktick) {
        if (ch === "`") {
          inBacktick = false
        }
        i++
        continue
      }

      if (ch === "'") {
        inSingleQuote = true
      } else if (ch === "`") {
        inBacktick = true
      }
      i++
    }

    if (inBacktick) {
      diagnostics.push({
        severity: "error",
        message: `Unterminated backtick-quoted identifier in statement: ${stmt.slice(0, 60)}...`,
        category: "sql",
      })
    }
  }

  return diagnostics
}

// ── Empty statement detection ──────────────────────────────────────

function checkEmptyStatements(
  statements: readonly string[],
): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = []

  for (const stmt of statements) {
    if (stmt.trim() === "" || stmt.trim() === ";") {
      diagnostics.push({
        severity: "error",
        message: "Empty SQL statement detected",
        category: "sql",
      })
    }
  }

  return diagnostics
}

// ── Tier 2: Deep checks ────────────────────────────────────────────

/**
 * Deep-verify generated SQL using dt-sql-parser for full parse
 * verification. Gracefully degrades if dt-sql-parser is not available.
 */
export async function deepVerifySql(
  statements: readonly string[],
): Promise<ValidationDiagnostic[]> {
  const diagnostics: ValidationDiagnostic[] = []

  // dt-sql-parser full statement parsing
  diagnostics.push(...(await deepParseCheck(statements)))

  return diagnostics
}

// ── dt-sql-parser statement parsing ────────────────────────────────

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

async function getParser(): Promise<FlinkSQLParser | null> {
  if (cachedParser) return cachedParser

  try {
    const mod = await import("dt-sql-parser")
    const FlinkSQL =
      (mod as Record<string, unknown>).FlinkSQL ??
      ((mod as { default?: Record<string, unknown> }).default?.FlinkSQL as
        | (new () => FlinkSQLParser)
        | undefined)

    if (!FlinkSQL) return null

    cachedParser = new (FlinkSQL as new () => FlinkSQLParser)()
    return cachedParser
  } catch {
    return null
  }
}

async function deepParseCheck(
  statements: readonly string[],
): Promise<ValidationDiagnostic[]> {
  const parser = await getParser()
  if (!parser) {
    return [
      {
        severity: "warning",
        message: "Deep SQL validation skipped: dt-sql-parser not available",
        category: "sql",
      },
    ]
  }

  const diagnostics: ValidationDiagnostic[] = []

  for (const stmt of statements) {
    // Skip SET statements and EXECUTE STATEMENT SET wrappers
    const trimmed = stmt.trim()
    if (
      trimmed.startsWith("SET ") ||
      trimmed === "EXECUTE STATEMENT SET BEGIN" ||
      trimmed === "END;"
    ) {
      continue
    }

    const errors = parser.validate(trimmed)
    if (errors.length > 0) {
      diagnostics.push({
        severity: "error",
        message: `SQL parse error: ${errors[0].message} in: ${trimmed.slice(0, 60)}...`,
        category: "sql",
      })
    }
  }

  return diagnostics
}

// ── SQL Gateway EXPLAIN validation ─────────────────────────────────

/**
 * Submit EXPLAIN for each statement to a running SQL Gateway for
 * semantic validation. Gracefully degrades if the gateway is not reachable.
 */
export async function deepVerifySqlViaGateway(
  statements: readonly string[],
  gatewayUrl = "http://localhost:8083",
): Promise<ValidationDiagnostic[]> {
  const diagnostics: ValidationDiagnostic[] = []

  let SqlGatewayClient: typeof import("@/lib/sql-gateway/client.js").SqlGatewayClient
  let SqlGatewayClientError: typeof import("@/lib/sql-gateway/client.js").SqlGatewayClientError

  try {
    const mod = await import("@/lib/sql-gateway/client.js")
    SqlGatewayClient = mod.SqlGatewayClient
    SqlGatewayClientError = mod.SqlGatewayClientError
  } catch {
    diagnostics.push({
      severity: "warning",
      message: "Deep SQL validation skipped: SQL Gateway client not available",
      category: "sql",
    })
    return diagnostics
  }

  const client = new SqlGatewayClient(gatewayUrl)

  let sessionHandle: string
  try {
    sessionHandle = await client.openSession()
  } catch (err) {
    if (err instanceof SqlGatewayClientError) {
      diagnostics.push({
        severity: "warning",
        message: `Deep SQL validation skipped: SQL Gateway unavailable at ${gatewayUrl} — ${err.message}`,
        category: "sql",
      })
      return diagnostics
    }
    throw err
  }

  try {
    for (const stmt of statements) {
      const trimmed = stmt.trim()
      if (
        trimmed.startsWith("SET ") ||
        trimmed === "EXECUTE STATEMENT SET BEGIN" ||
        trimmed === "END;"
      ) {
        continue
      }

      try {
        const opHandle = await client.submitStatement(
          sessionHandle,
          `EXPLAIN ${trimmed}`,
        )
        let status = await client.getOperationStatus(sessionHandle, opHandle)
        let attempts = 0
        while (status === "RUNNING" && attempts < 30) {
          await new Promise((r) => setTimeout(r, 500))
          status = await client.getOperationStatus(sessionHandle, opHandle)
          attempts++
        }

        if (status === "ERROR") {
          diagnostics.push({
            severity: "error",
            message: `SQL Gateway EXPLAIN rejected: ${trimmed.slice(0, 60)}...`,
            category: "sql",
          })
        }
      } catch (err) {
        if (err instanceof SqlGatewayClientError) {
          diagnostics.push({
            severity: "error",
            message: `SQL Gateway error: ${err.message} for: ${trimmed.slice(0, 60)}...`,
            category: "sql",
          })
        } else {
          throw err
        }
      }
    }
  } finally {
    try {
      await client.closeSession(sessionHandle)
    } catch {
      // Best-effort cleanup
    }
  }

  return diagnostics
}

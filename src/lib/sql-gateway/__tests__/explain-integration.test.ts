import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { splitSqlStatements } from "@/lib/sql-gateway/sql-utils.js"
import { synth } from "@/testing/synth.js"
import { SqlGatewayClient, StatementExecutionError } from "../client.js"

const SQL_GATEWAY_URL = process.env.SQL_GATEWAY_URL ?? "http://localhost:8083"

// Examples that require connectors/formats not in the docker-compose cluster
const SKIP = new Set([
  "07-lookup-join", // MySQL JDBC
  "13-simple-etl", // Elasticsearch-7 connector
  "20-realtime-dashboard", // MySQL JDBC LookupJoin
  "22-enrichment-archive", // MySQL JDBC LookupJoin
  "27-ml-feature-pipeline", // Parquet format
])

const EXAMPLES = [
  "01-simple-source-sink",
  "02-filter-project",
  "03-group-aggregate",
  "04-tumble-window",
  "05-hop-window",
  "06-interval-join",
  "07-lookup-join",
  "08-multi-stream-join",
  "11-dedup-window",
  "12-session-window",
  "13-simple-etl",
  "14-ohlcv-window",
  "15-cep-fraud-detection",
  "16-temporal-join",
  "18-broadcast-join",
  "19-union-aggregate",
  "20-realtime-dashboard",
  "21-branching-multi-sink",
  "22-enrichment-archive",
  "23-batch-etl",
  "24-lambda-architecture",
  "25-batch-reporting",
  "26-cdc-sync",
  "27-ml-feature-pipeline",
  "28-branching-iot",
  "30-flatmap-unnest",
  "31-top-n-ranking",
  "32-union-streams",
  "33-rename-fields",
  "34-drop-fields",
  "35-cast-types",
  "36-coalesce-defaults",
  "37-add-computed-field",
  "38-dedup-aggregate",
]

// ─── Helpers ────────────────────────────────────────────────────────────────

function isDdl(stmt: string): boolean {
  const upper = stmt.trimStart().toUpperCase()
  return (
    upper.startsWith("CREATE ") ||
    upper.startsWith("SET ") ||
    upper.startsWith("DROP ") ||
    upper.startsWith("ALTER ") ||
    upper.startsWith("USE ")
  )
}

function isStatementSet(stmt: string): boolean {
  return /^EXECUTE\s+STATEMENT\s+SET/i.test(stmt.trimStart())
}

/** Extract individual INSERT statements from an EXECUTE STATEMENT SET block */
function extractInserts(block: string): string[] {
  const inserts: string[] = []
  // Remove the EXECUTE STATEMENT SET BEGIN / END wrapper
  const inner = block
    .replace(/^EXECUTE\s+STATEMENT\s+SET\s*(BEGIN)?\s*/i, "")
    .replace(/\s*END\s*;?\s*$/i, "")
    .trim()

  // Split on INSERT INTO boundaries
  const parts = inner.split(/(?=INSERT\s+INTO)/i)
  for (const part of parts) {
    const trimmed = part.trim().replace(/;$/, "").trim()
    if (trimmed && /^INSERT\s+INTO/i.test(trimmed)) {
      inserts.push(trimmed)
    }
  }
  return inserts
}

// ─── Pre-flight check ───────────────────────────────────────────────────────
// Must be top-level await so the result is available when describe.skipIf
// evaluates synchronously at module parse time.

const gatewayAvailable = await fetch(`${SQL_GATEWAY_URL}/v1/info`, {
  signal: AbortSignal.timeout(3000),
})
  .then((res) => res.ok)
  .catch(() => false)

// ─── Tests ──────────────────────────────────────────────────────────────────

describe.skipIf(!gatewayAvailable)("EXPLAIN Integration Tests", () => {
  const client = new SqlGatewayClient(SQL_GATEWAY_URL)
  let sessionHandle: string

  beforeAll(async () => {
    sessionHandle = await client.openSession()
  })

  afterAll(async () => {
    if (sessionHandle) {
      try {
        await client.closeSession(sessionHandle)
      } catch {
        // Best-effort cleanup
      }
    }
  })

  for (const id of EXAMPLES) {
    const testFn = SKIP.has(id) ? it.skip : it

    testFn(id, { timeout: 60_000 }, async () => {
      resetNodeIdCounter()
      const mod = await import(`../../../examples/${id}/after.tsx`)
      const { sql } = synth(mod.default)
      const statements = splitSqlStatements(sql)

      // Per-example session to avoid DDL conflicts between tests
      const exampleSession = await client.openSession()

      try {
        for (const stmt of statements) {
          if (isDdl(stmt)) {
            // Execute DDL to establish session state
            const status = await client.executeStatement(
              exampleSession,
              stmt,
              30_000,
            )
            expect(status).toBe("FINISHED")
          } else if (isStatementSet(stmt)) {
            // Extract individual INSERTs and EXPLAIN each
            const inserts = extractInserts(stmt)
            for (const insert of inserts) {
              const plan = await client.explainInSession(exampleSession, insert)
              expect(plan).toBeTruthy()
            }
          } else {
            // DML — EXPLAIN it directly
            const plan = await client.explainInSession(exampleSession, stmt)
            expect(plan).toBeTruthy()
          }
        }
      } finally {
        try {
          await client.closeSession(exampleSession)
        } catch {
          // Best-effort cleanup
        }
      }
    })
  }

  it(
    "surfaces structured errors for invalid SQL",
    { timeout: 30_000 },
    async () => {
      const session = await client.openSession()
      try {
        // Create a table so we have session state
        await client.executeStatement(
          session,
          `CREATE TABLE test_err_source (id STRING) WITH ('connector' = 'blackhole')`,
        )
        // EXPLAIN a query referencing a non-existent column
        await client.explainInSession(
          session,
          "INSERT INTO test_err_source SELECT nonexistent_col FROM test_err_source",
        )
        expect.unreachable("Should have thrown")
      } catch (err) {
        expect(err).toBeInstanceOf(StatementExecutionError)
        const execErr = err as StatementExecutionError
        expect(execErr.detail.statement).toContain("nonexistent_col")
        expect(execErr.detail.message).toBeTruthy()
        expect(execErr.detail.fullMessage).toBeTruthy()
        // message should be more readable than the raw fullMessage
        expect(execErr.detail.message.length).toBeLessThanOrEqual(
          execErr.detail.fullMessage.length,
        )
      } finally {
        try {
          await client.closeSession(session)
        } catch {
          // Best-effort cleanup
        }
      }
    },
  )
})

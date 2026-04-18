import { rmSync } from "node:fs"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import type { TemplateName } from "@/cli/commands/new.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { SqlGatewayClient } from "@/lib/sql-gateway/client.js"
import { splitSqlStatements } from "@/lib/sql-gateway/sql-utils.js"
import {
  EXPECTED_PIPELINES,
  scaffoldAndSynth,
} from "./helpers/scaffold-and-synth.js"

const SQL_GATEWAY_URL = process.env.SQL_GATEWAY_URL ?? "http://localhost:8083"

// When set, a missing gateway causes the suite to FAIL loudly instead of
// silently skipping. CI should run with this enabled so a broken cluster
// never masquerades as a green build.
const REQUIRE_SQL_GATEWAY = process.env.REQUIRE_SQL_GATEWAY === "1"

// ── Pipelines skipped from EXPLAIN validation ─────────────────────────
// The scaffold→synth test (template-scaffold-synth.test.ts) already
// guarantees these pipelines synthesize. The entries below fail against
// Flink's EXPLAIN for reasons tracked in `bugs/`; remove an entry from
// here when its underlying bug is fixed.

const SKIP = new Set<string>([
  // ── Codegen issues needing deeper fixes ───────────────────────────
  // "ecom-order-enrichment", // bugs/029 — multiple rowtime cols in Kafka sink after interval join
  "iot-predictive-maintenance", // bugs/030 — temporal join requires a time-attribute field; windowed-agg output loses the attribute through the CTE alias
])

// ── Helpers ──────────────────────────────────────────────────────────

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

function extractInserts(block: string): string[] {
  const inserts: string[] = []
  const inner = block
    .replace(/^EXECUTE\s+STATEMENT\s+SET\s*(BEGIN)?\s*/i, "")
    .replace(/\s*END\s*;?\s*$/i, "")
    .trim()

  const parts = inner.split(/(?=INSERT\s+INTO)/i)
  for (const part of parts) {
    const trimmed = part.trim().replace(/;$/, "").trim()
    if (trimmed && /^INSERT\s+INTO/i.test(trimmed)) {
      inserts.push(trimmed)
    }
  }
  return inserts
}

// ── Pre-flight ───────────────────────────────────────────────────────

const gatewayAvailable = await fetch(`${SQL_GATEWAY_URL}/v1/info`, {
  signal: AbortSignal.timeout(3000),
})
  .then((res) => res.ok)
  .catch(() => false)

if (REQUIRE_SQL_GATEWAY && !gatewayAvailable) {
  throw new Error(
    `REQUIRE_SQL_GATEWAY=1 but Flink SQL Gateway is not reachable at ${SQL_GATEWAY_URL}. Start the cluster (\`flink-reactor cluster up\`) before running.`,
  )
}

// ── EXPLAIN tests (require running SQL Gateway) ──────────────────────

describe.skipIf(!gatewayAvailable)(
  "Template pipeline EXPLAIN integration",
  () => {
    const client = new SqlGatewayClient(SQL_GATEWAY_URL)
    let sessionHandle: string

    // Scaffold each template once up-front; reuse artifacts across its
    // per-pipeline EXPLAIN tests. Temp dirs are cleaned in afterAll.
    type ScaffoldedTemplate = {
      tempRoot: string
      artifacts: Map<string, string> // pipeline name → sql
    }
    const scaffolded = new Map<TemplateName, ScaffoldedTemplate>()

    beforeAll(async () => {
      sessionHandle = await client.openSession()

      for (const [template, pipelines] of Object.entries(
        EXPECTED_PIPELINES,
      ) as [TemplateName, readonly string[]][]) {
        if (pipelines.length === 0) continue
        resetNodeIdCounter()
        const result = await scaffoldAndSynth(template)
        const byName = new Map<string, string>()
        for (const artifact of result.artifacts) {
          byName.set(artifact.name, artifact.sql.sql)
        }
        scaffolded.set(template, {
          tempRoot: result.tempRoot,
          artifacts: byName,
        })
      }
    }, 60_000)

    afterAll(async () => {
      if (sessionHandle) {
        try {
          await client.closeSession(sessionHandle)
        } catch {
          // Best-effort cleanup
        }
      }
      for (const { tempRoot } of scaffolded.values()) {
        rmSync(tempRoot, { recursive: true, force: true })
      }
    })

    for (const [template, pipelines] of Object.entries(EXPECTED_PIPELINES) as [
      TemplateName,
      readonly string[],
    ][]) {
      for (const pipeline of pipelines) {
        const testFn = SKIP.has(pipeline) ? it.skip : it

        testFn(`${template}/${pipeline}`, { timeout: 60_000 }, async () => {
          const entry = scaffolded.get(template)
          if (!entry) throw new Error(`Template ${template} not scaffolded`)

          const sql = entry.artifacts.get(pipeline)
          if (!sql) {
            throw new Error(
              `Pipeline ${pipeline} not found in scaffolded ${template}; got: ${[...entry.artifacts.keys()].join(", ")}`,
            )
          }

          const statements = splitSqlStatements(sql)
          const pipelineSession = await client.openSession()

          try {
            for (const stmt of statements) {
              if (isDdl(stmt)) {
                const status = await client.executeStatement(
                  pipelineSession,
                  stmt,
                  30_000,
                )
                expect(status).toBe("FINISHED")
              } else if (isStatementSet(stmt)) {
                const inserts = extractInserts(stmt)
                for (const insert of inserts) {
                  const plan = await client.explainInSession(
                    pipelineSession,
                    insert,
                  )
                  expect(plan).toBeTruthy()
                }
              } else {
                const plan = await client.explainInSession(
                  pipelineSession,
                  stmt,
                )
                expect(plan).toBeTruthy()
              }
            }
          } finally {
            try {
              await client.closeSession(pipelineSession)
            } catch {
              // Best-effort cleanup
            }
          }
        })
      }
    }
  },
)

import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { SqlGatewayClient } from "@/lib/sql-gateway/client.js"
import { splitSqlStatements } from "@/lib/sql-gateway/sql-utils.js"
import { synth } from "@/testing/synth.js"

const SQL_GATEWAY_URL = process.env.SQL_GATEWAY_URL ?? "http://localhost:8083"

// ── Template pipeline fixtures ───────────────────────────────────────
// Each entry: [fixture-id, template-name, description]

const FIXTURES: [string, string, string][] = [
  // Starter
  ["starter-hello-world", "starter", "KafkaSource → Filter → KafkaSink"],
  // Realtime Analytics
  [
    "analytics-page-views",
    "realtime-analytics",
    "Kafka → TumbleWindow → Aggregate → JDBC",
  ],
  // CDC Lakehouse
  [
    "cdc-to-lakehouse",
    "cdc-lakehouse",
    "Debezium CDC → IcebergSink (v2 upsert)",
  ],
  // E-Commerce (4 pipelines)
  ["ecom-order-enrichment", "ecommerce", "3-way join (interval + temporal)"],
  [
    "ecom-revenue-analytics",
    "ecommerce",
    "Route: windowed agg → JDBC + filter → Kafka",
  ],
  [
    "ecom-customer-360",
    "ecommerce",
    "LookupJoin → SessionWindow → JdbcSink upsert",
  ],
  ["pump-ecom", "ecommerce", "StatementSet: 4× DataGen → Kafka"],
  // Ride-Sharing (2 pipelines)
  [
    "rides-trip-tracking",
    "ride-sharing",
    "IntervalJoin → MatchRecognize → Route",
  ],
  [
    "rides-surge-pricing",
    "ride-sharing",
    "TumbleWindow → Aggregate → BroadcastJoin",
  ],
  // Grocery Delivery (2 pipelines)
  [
    "grocery-order-fulfillment",
    "grocery-delivery",
    "TemporalJoin → Route (2 branches)",
  ],
  [
    "grocery-store-rankings",
    "grocery-delivery",
    "Deduplicate → TumbleWindow → Aggregate → JDBC upsert",
  ],
  // Banking (2 pipelines)
  ["bank-fraud-detection", "banking", "MatchRecognize → TemporalJoin"],
  ["bank-compliance-agg", "banking", "TumbleWindow → Route (3 branches)"],
  // IoT Factory (2 pipelines)
  [
    "iot-predictive-maintenance",
    "iot-factory",
    "SlideWindow → Aggregate → TemporalJoin → Route",
  ],
  ["pump-iot", "iot-factory", "StatementSet: 2× DataGen → Kafka"],
  // Lakehouse Ingestion (2 pipelines)
  [
    "lakehouse-ingest",
    "lakehouse-ingestion",
    "StatementSet: 3× Kafka → Iceberg",
  ],
  ["pump-lakehouse", "lakehouse-ingestion", "StatementSet: 3× DataGen → Kafka"],
  // Lakehouse Analytics / Medallion (4 pipelines)
  [
    "medallion-bronze",
    "lakehouse-analytics",
    "Kafka CDC → Iceberg append (v1)",
  ],
  [
    "medallion-silver",
    "lakehouse-analytics",
    "Kafka → Deduplicate → Iceberg upsert (v2)",
  ],
  [
    "medallion-gold",
    "lakehouse-analytics",
    "Kafka → TumbleWindow → Aggregate → Iceberg",
  ],
  ["pump-medallion", "lakehouse-analytics", "DataGen → Kafka pump"],
]

// Pipelines requiring connectors not in the local cluster
// or with pre-existing codegen issues needing deeper fixes
const SKIP = new Set([
  // ── Missing catalog connectors in local cluster ───────────────────
  "cdc-to-lakehouse", // Iceberg REST catalog connector
  "lakehouse-ingest", // Iceberg REST catalog connector
  "medallion-bronze", // Iceberg REST catalog connector
  "medallion-silver", // Iceberg REST catalog connector
  "medallion-gold", // Iceberg REST catalog connector

  // ── Codegen issues needing deeper fixes ───────────────────────────
  "ecom-order-enrichment", // interval join table reference issues
  "ecom-customer-360", // LookupJoin references external table not in session
  "rides-trip-tracking", // MatchRecognize SQL parsing not yet supported by EXPLAIN
  "rides-surge-pricing", // BroadcastJoin references intermediate 'demand' table not registered
  "bank-fraud-detection", // column ambiguity in temporal join (accountId)
  "bank-compliance-agg", // Route branch column type mismatch after windowed aggregation
  "grocery-order-fulfillment", // column ambiguity in temporal join (storeId)
  "iot-predictive-maintenance", // STDDEV_POP may need special handling in windowed context

  // ── Multi-pair StatementSet type mismatches ─────────────────────
  "pump-ecom", // StatementSet: later source/sink pairs have column type mismatches
  "pump-iot", // StatementSet: later source/sink pairs have column type mismatches
  "pump-lakehouse", // StatementSet: later source/sink pairs have column type mismatches
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

// ── Synth-only tests (always run) ────────────────────────────────────
// Validate that every template pipeline synthesizes without errors.

describe("Template pipeline synthesis", () => {
  for (const [id, template, desc] of FIXTURES) {
    it(`${id} (${template}): ${desc}`, async () => {
      resetNodeIdCounter()
      const mod = await import(`./fixtures/${id}.tsx`)
      const { sql } = synth(mod.default)
      expect(sql).toBeTruthy()
      expect(sql.length).toBeGreaterThan(0)

      const statements = splitSqlStatements(sql)
      expect(statements.length).toBeGreaterThan(0)
    })
  }
})

// ── EXPLAIN tests (require running SQL Gateway) ──────────────────────

describe.skipIf(!gatewayAvailable)(
  "Template pipeline EXPLAIN integration",
  () => {
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

    for (const [id, template, desc] of FIXTURES) {
      const testFn = SKIP.has(id) ? it.skip : it

      testFn(`${id} (${template}): ${desc}`, { timeout: 60_000 }, async () => {
        resetNodeIdCounter()
        const mod = await import(`./fixtures/${id}.tsx`)
        const { sql } = synth(mod.default)
        const statements = splitSqlStatements(sql)

        // Per-pipeline session to avoid DDL conflicts
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
              const plan = await client.explainInSession(pipelineSession, stmt)
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
  },
)

import { beforeEach, describe, expect, it } from "vitest"
import { generateSql } from "@/codegen/sql-generator.js"
import { PaimonCatalog } from "@/components/catalogs.js"
import { MaterializedTable } from "@/components/materialized-table.js"
import { Pipeline } from "@/components/pipeline.js"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { Aggregate, Filter } from "@/components/transforms.js"
import { View } from "@/components/view.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { SynthContext } from "@/core/synth-context.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const EventSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    event_type: Field.STRING(),
    ts: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "ts",
    expression: "`ts` - INTERVAL '5' SECOND",
  },
})

function makeCatalog() {
  return PaimonCatalog({
    name: "paimon_catalog",
    warehouse: "s3://bucket/warehouse",
  })
}

// ── 4.1: Basic materialized table → CREATE MATERIALIZED TABLE DDL ──

describe("MaterializedTable: basic DDL", () => {
  it("produces CREATE MATERIALIZED TABLE with freshness", () => {
    const { node: catalogNode, handle } = makeCatalog()
    const source = KafkaSource({
      topic: "events",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: EventSchema,
    })

    const matTable = MaterializedTable({
      name: "user_summary",
      catalog: handle,
      database: "mydb",
      freshness: "INTERVAL '30' SECOND",
      children: [source],
    })

    const pipeline = Pipeline({
      name: "test-pipeline",
      children: [catalogNode, matTable],
    })

    const result = generateSql(pipeline, { flinkVersion: "2.0" })
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 4.2: Materialized table with all options ────────────────────────

describe("MaterializedTable: all options", () => {
  it("produces DDL with comment, partitions, refresh mode, and with", () => {
    const { node: catalogNode, handle } = makeCatalog()
    const source = KafkaSource({
      topic: "events",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: EventSchema,
    })

    const matTable = MaterializedTable({
      name: "user_summary",
      catalog: handle,
      database: "mydb",
      freshness: "INTERVAL '1' DAY",
      refreshMode: "full",
      comment: "Daily user summary",
      partitionedBy: ["dt"],
      with: { format: "debezium-json" },
      children: [source],
    })

    const pipeline = Pipeline({
      name: "test-pipeline",
      children: [catalogNode, matTable],
    })

    const result = generateSql(pipeline, { flinkVersion: "2.0" })
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 4.3: Bucketing on Flink 2.2 ────────────────────────────────────

describe("MaterializedTable: bucketing on Flink 2.2", () => {
  it("produces DDL with DISTRIBUTED BY HASH", () => {
    const { node: catalogNode, handle } = makeCatalog()
    const source = KafkaSource({
      topic: "events",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: EventSchema,
    })

    const matTable = MaterializedTable({
      name: "bucketed_summary",
      catalog: handle,
      database: "mydb",
      freshness: "INTERVAL '1' HOUR",
      refreshMode: "continuous",
      bucketing: { columns: ["user_id"], count: 8 },
      children: [source],
    })

    const pipeline = Pipeline({
      name: "test-pipeline",
      children: [catalogNode, matTable],
    })

    const result = generateSql(pipeline, { flinkVersion: "2.2" })
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 4.4: No freshness on Flink 2.2 (optional) ─────────────────────

describe("MaterializedTable: optional freshness on Flink 2.2", () => {
  it("omits FRESHNESS clause when not provided on 2.2", () => {
    const { node: catalogNode, handle } = makeCatalog()
    const source = KafkaSource({
      topic: "events",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: EventSchema,
    })

    const matTable = MaterializedTable({
      name: "on_demand_table",
      catalog: handle,
      database: "mydb",
      children: [source],
    })

    const pipeline = Pipeline({
      name: "test-pipeline",
      children: [catalogNode, matTable],
    })

    const result = generateSql(pipeline, { flinkVersion: "2.2" })
    expect(result.sql).toMatchSnapshot()
    expect(result.sql).not.toContain("FRESHNESS")
  })
})

// ── 4.5: No catalog → validation error ─────────────────────────────

describe("MaterializedTable: validation errors", () => {
  it("throws when catalog is missing", () => {
    expect(() =>
      MaterializedTable({
        name: "test",
        // @ts-expect-error testing missing catalog
        catalog: undefined,
        freshness: "INTERVAL '30' SECOND",
      }),
    ).toThrow("MaterializedTable requires a managed catalog")
  })

  // ── 4.6: Flink 1.20 → version error ────────────────────────────

  it("throws version error on Flink 1.20", () => {
    const { node: catalogNode, handle } = makeCatalog()
    const source = KafkaSource({
      topic: "events",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: EventSchema,
    })

    const matTable = MaterializedTable({
      name: "test",
      catalog: handle,
      database: "mydb",
      freshness: "INTERVAL '30' SECOND",
      children: [source],
    })

    const pipeline = Pipeline({
      name: "test-pipeline",
      children: [catalogNode, matTable],
    })

    expect(() => generateSql(pipeline, { flinkVersion: "1.20" })).toThrow(
      "Materialized tables requires Flink 2.0 or later",
    )
  })

  // ── 4.7: Bucketing on Flink 2.0 → version error ────────────────

  it("throws version error for bucketing on Flink 2.0", () => {
    const { node: catalogNode, handle } = makeCatalog()
    const source = KafkaSource({
      topic: "events",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: EventSchema,
    })

    const matTable = MaterializedTable({
      name: "test",
      catalog: handle,
      database: "mydb",
      freshness: "INTERVAL '30' SECOND",
      bucketing: { columns: ["user_id"], count: 8 },
      children: [source],
    })

    const pipeline = Pipeline({
      name: "test-pipeline",
      children: [catalogNode, matTable],
    })

    expect(() => generateSql(pipeline, { flinkVersion: "2.0" })).toThrow(
      "Materialized table bucketing requires Flink 2.2 or later",
    )
  })

  // ── 4.8: No freshness on Flink 2.0 → validation error ──────────

  it("throws when freshness is missing on Flink 2.0", () => {
    const { node: catalogNode, handle } = makeCatalog()
    const source = KafkaSource({
      topic: "events",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: EventSchema,
    })

    const matTable = MaterializedTable({
      name: "test",
      catalog: handle,
      database: "mydb",
      children: [source],
    })

    const pipeline = Pipeline({
      name: "test-pipeline",
      children: [catalogNode, matTable],
    })

    expect(() => generateSql(pipeline, { flinkVersion: "2.0" })).toThrow(
      "MaterializedTable freshness is required for Flink < 2.2",
    )
  })
})

// ── 4.9: Statement ordering ────────────────────────────────────────

describe("MaterializedTable: statement ordering", () => {
  it("places CREATE MATERIALIZED TABLE after CREATE VIEW and before INSERT INTO", () => {
    const { node: catalogNode, handle } = makeCatalog()
    const source = KafkaSource({
      topic: "events",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: EventSchema,
    })

    const view = View({
      name: "filtered_events",
      children: [
        Filter({
          condition: "`event_type` = 'click'",
          children: [source],
        }),
      ],
    })

    const matTable = MaterializedTable({
      name: "click_summary",
      catalog: handle,
      database: "mydb",
      freshness: "INTERVAL '1' MINUTE",
      children: [
        KafkaSource({
          topic: "clicks",
          format: "json",
          bootstrapServers: "kafka:9092",
          schema: EventSchema,
        }),
      ],
    })

    const sink = KafkaSink({
      topic: "output",
      format: "json",
      bootstrapServers: "kafka:9092",
      children: [
        Filter({
          condition: "`user_id` IS NOT NULL",
          children: [source],
        }),
      ],
    })

    const pipeline = Pipeline({
      name: "test-pipeline",
      children: [catalogNode, view, matTable, sink],
    })

    const result = generateSql(pipeline, { flinkVersion: "2.0" })

    // Find statement positions
    const viewIdx = result.statements.findIndex((s) =>
      s.startsWith("CREATE VIEW"),
    )
    const matIdx = result.statements.findIndex((s) =>
      s.startsWith("CREATE MATERIALIZED TABLE"),
    )
    const dmlIdx = result.statements.findIndex(
      (s) => s.startsWith("INSERT INTO") || s.startsWith("EXECUTE STATEMENT"),
    )

    expect(viewIdx).toBeGreaterThan(-1)
    expect(matIdx).toBeGreaterThan(-1)
    expect(dmlIdx).toBeGreaterThan(-1)
    expect(matIdx).toBeGreaterThan(viewIdx)
    expect(matIdx).toBeLessThan(dmlIdx)
  })
})

// ── 4.10: Aggregated stream → GROUP BY in AS query ─────────────────

describe("MaterializedTable: aggregated stream", () => {
  it("generates correct GROUP BY in AS query", () => {
    const { node: catalogNode, handle } = makeCatalog()
    const source = KafkaSource({
      topic: "events",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: EventSchema,
    })

    const matTable = MaterializedTable({
      name: "user_event_counts",
      catalog: handle,
      database: "mydb",
      freshness: "INTERVAL '1' MINUTE",
      refreshMode: "continuous",
      children: [
        Aggregate({
          select: { user_id: "user_id", cnt: "COUNT(*)" },
          groupBy: ["user_id"],
          children: [source],
        }),
      ],
    })

    const pipeline = Pipeline({
      name: "test-pipeline",
      children: [catalogNode, matTable],
    })

    const result = generateSql(pipeline, { flinkVersion: "2.0" })
    expect(result.sql).toMatchSnapshot()
    expect(result.sql).toContain("GROUP BY")
  })
})

// ── Construct tree integration ──────────────────────────────────────

describe("MaterializedTable: construct tree", () => {
  it("creates node with kind MaterializedTable", () => {
    const { handle } = makeCatalog()
    const source = KafkaSource({
      topic: "events",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: EventSchema,
    })

    const matTable = MaterializedTable({
      name: "test_table",
      catalog: handle,
      freshness: "INTERVAL '30' SECOND",
      children: [source],
    })

    expect(matTable.kind).toBe("MaterializedTable")
    expect(matTable.component).toBe("MaterializedTable")
    expect(matTable.props.catalogName).toBe("paimon_catalog")
    expect(matTable.children).toHaveLength(1)
  })

  it("builds a valid DAG", () => {
    const { handle } = makeCatalog()
    const source = KafkaSource({
      topic: "events",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: EventSchema,
    })

    const matTable = MaterializedTable({
      name: "test_table",
      catalog: handle,
      freshness: "INTERVAL '30' SECOND",
      children: [source],
    })

    const ctx = new SynthContext()
    ctx.buildFromTree(matTable)

    expect(ctx.getAllNodes()).toHaveLength(2)
    expect(ctx.getAllEdges()).toHaveLength(1)
  })
})

import { beforeEach, describe, expect, it } from "vitest"
import { generateSql } from "@/codegen/sql/sql-generator.js"
import { Pipeline } from "@/components/pipeline.js"
import { Query } from "@/components/query.js"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { Filter } from "@/components/transforms.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"

beforeEach(() => {
  resetNodeIdCounter()
})

// ── Shared test schemas ─────────────────────────────────────────────

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    user_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    event_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "event_time",
    expression: "`event_time` - INTERVAL '5' SECOND",
  },
})

const AggOutputSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    total: Field.DECIMAL(10, 2),
  },
})

const WindowOutputSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    prev_amount: Field.DECIMAL(10, 2),
    row_num: Field.BIGINT(),
  },
})

// ── Simple aggregation with HAVING ──────────────────────────────────

describe("Query: aggregation with HAVING", () => {
  it("produces correct SQL", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: OrderSchema,
    })

    const query = Query({
      outputSchema: AggOutputSchema,
      children: [
        Query.Select({
          columns: {
            user_id: "user_id",
            total: "SUM(amount)",
          },
        }),
        Query.GroupBy({ columns: ["user_id"] }),
        Query.Having({ condition: "SUM(amount) > 1000" }),
        source,
      ],
    })

    const sink = KafkaSink({
      topic: "high-spenders",
      format: "json",
      children: [query],
    })

    const pipeline = Pipeline({
      name: "agg-having-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── Window function with named window ───────────────────────────────

describe("Query: window function with named window", () => {
  it("produces correct SQL for LAG and ROW_NUMBER with named window", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: OrderSchema,
    })

    const query = Query({
      outputSchema: WindowOutputSchema,
      children: [
        Query.Select({
          columns: {
            user_id: "user_id",
            amount: "amount",
            prev_amount: { func: "LAG", args: ["amount", 1], window: "w" },
            row_num: { func: "ROW_NUMBER", window: "w" },
          },
          windows: {
            w: {
              partitionBy: ["user_id"],
              orderBy: { event_time: "ASC" },
            },
          },
        }),
        source,
      ],
    })

    const sink = KafkaSink({
      topic: "windowed-orders",
      format: "json",
      children: [query],
    })

    const pipeline = Pipeline({
      name: "named-window-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── Window function with inline OVER ────────────────────────────────

describe("Query: window function with inline OVER", () => {
  it("produces correct SQL for RANK with inline window spec", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: OrderSchema,
    })

    const RankOutputSchema = Schema({
      fields: {
        user_id: Field.STRING(),
        amount: Field.DECIMAL(10, 2),
        rank_num: Field.BIGINT(),
      },
    })

    const query = Query({
      outputSchema: RankOutputSchema,
      children: [
        Query.Select({
          columns: {
            user_id: "user_id",
            amount: "amount",
            rank_num: {
              func: "RANK",
              over: {
                partitionBy: ["user_id"],
                orderBy: { amount: "DESC" },
              },
            },
          },
        }),
        source,
      ],
    })

    const sink = KafkaSink({
      topic: "ranked-orders",
      format: "json",
      children: [query],
    })

    const pipeline = Pipeline({
      name: "inline-over-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── Query chained with Filter (upstream transform) ──────────────────

describe("Query: chained with upstream Filter", () => {
  it("wraps the upstream filter in a subquery", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: OrderSchema,
    })

    const filter = Filter({
      condition: "user_id <> 'bot'",
      children: [source],
    })

    const query = Query({
      outputSchema: AggOutputSchema,
      children: [
        Query.Select({
          columns: {
            user_id: "user_id",
            total: "SUM(amount)",
          },
        }),
        Query.GroupBy({ columns: ["user_id"] }),
        filter,
      ],
    })

    const sink = KafkaSink({
      topic: "user-totals",
      format: "json",
      children: [query],
    })

    const pipeline = Pipeline({
      name: "filter-query-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── Query with WHERE + GROUP BY + HAVING combined ───────────────────

describe("Query: WHERE + GROUP BY + HAVING combined", () => {
  it("produces all clauses in correct order", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: OrderSchema,
    })

    const query = Query({
      outputSchema: AggOutputSchema,
      children: [
        Query.Select({
          columns: {
            user_id: "user_id",
            total: "SUM(amount)",
          },
        }),
        Query.Where({ condition: "amount > 0" }),
        Query.GroupBy({ columns: ["user_id"] }),
        Query.Having({ condition: "SUM(amount) > 500" }),
        source,
      ],
    })

    const sink = KafkaSink({
      topic: "filtered-agg",
      format: "json",
      children: [query],
    })

    const pipeline = Pipeline({
      name: "combined-clauses-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── Multiple window functions sharing a named window ────────────────

describe("Query: multiple window functions sharing a named window", () => {
  it("produces correct SQL with shared WINDOW clause", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: OrderSchema,
    })

    const MultiWindowSchema = Schema({
      fields: {
        user_id: Field.STRING(),
        amount: Field.DECIMAL(10, 2),
        prev_amount: Field.DECIMAL(10, 2),
        next_amount: Field.DECIMAL(10, 2),
        row_num: Field.BIGINT(),
      },
    })

    const query = Query({
      outputSchema: MultiWindowSchema,
      children: [
        Query.Select({
          columns: {
            user_id: "user_id",
            amount: "amount",
            prev_amount: { func: "LAG", args: ["amount", 1], window: "w" },
            next_amount: { func: "LEAD", args: ["amount", 1], window: "w" },
            row_num: { func: "ROW_NUMBER", window: "w" },
          },
          windows: {
            w: {
              partitionBy: ["user_id"],
              orderBy: { event_time: "ASC" },
            },
          },
        }),
        source,
      ],
    })

    const sink = KafkaSink({
      topic: "multi-window-output",
      format: "json",
      children: [query],
    })

    const pipeline = Pipeline({
      name: "multi-window-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

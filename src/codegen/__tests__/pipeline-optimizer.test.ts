import { beforeEach, describe, expect, it } from "vitest"
import { optimizePipeline } from "@/codegen/pipeline-optimizer.js"
import { generateSql } from "@/codegen/sql-generator.js"
import { Join } from "@/components/joins.js"
import { Pipeline } from "@/components/pipeline.js"
import { Route } from "@/components/route.js"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import {
  Aggregate,
  Filter,
  Map as MapTransform,
} from "@/components/transforms.js"
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
    product_id: Field.STRING(),
    category: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "event_time",
    expression: "`event_time` - INTERVAL '5' SECOND",
  },
})

const UserSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    name: Field.STRING(),
    email: Field.STRING(),
  },
})

// ── 6.2: Aggregate → mini-batch SET statements injected ─────────────

describe("SET injection pass", () => {
  it("injects mini-batch SET statements when Aggregate detected", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const agg = Aggregate({
      groupBy: ["user_id"],
      select: { total_amount: "SUM(`amount`)", order_count: "COUNT(*)" },
      children: [source],
    })

    const sink = KafkaSink({
      topic: "user-totals",
      children: [agg],
    })

    const pipeline = Pipeline({
      name: "agg-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline, { optimize: true })

    // Should contain mini-batch SET statements
    expect(result.sql).toContain(
      "SET 'table.exec.mini-batch.enabled' = 'true';",
    )
    expect(result.sql).toContain(
      "SET 'table.exec.mini-batch.allow-latency' = '5 s';",
    )
    expect(result.sql).toContain("SET 'table.exec.mini-batch.size' = '1000';")

    expect(result.sql).toMatchSnapshot()
  })

  it("injects TWO_PHASE for high-cardinality GROUP BY", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    // 3+ group-by columns → high cardinality heuristic
    const agg = Aggregate({
      groupBy: ["user_id", "product_id", "category"],
      select: { total_amount: "SUM(`amount`)" },
      children: [source],
    })

    const sink = KafkaSink({
      topic: "grouped-totals",
      children: [agg],
    })

    const pipeline = Pipeline({
      name: "high-card-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline, { optimize: true })

    expect(result.sql).toContain(
      "SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';",
    )
  })

  it("injects distinct-agg split for DISTINCT aggregates", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const agg = Aggregate({
      groupBy: ["user_id"],
      select: { unique_products: "COUNT(DISTINCT product_id)" },
      children: [source],
    })

    const sink = KafkaSink({
      topic: "distinct-counts",
      children: [agg],
    })

    const pipeline = Pipeline({
      name: "distinct-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline, { optimize: true })

    expect(result.sql).toContain(
      "SET 'table.optimizer.distinct-agg.split.enabled' = 'true';",
    )
    expect(result.sql).toContain(
      "SET 'table.optimizer.distinct-agg.split.bucket-num' = '1024';",
    )
  })
})

// ── 6.3: Join with broadcast hint ───────────────────────────────────

describe("SQL hint emission pass", () => {
  it("emits BROADCAST hint for Join with hint: broadcast", () => {
    const orders = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const users = KafkaSource({
      topic: "users",
      schema: UserSchema,
      bootstrapServers: "kafka:9092",
    })

    const joined = Join({
      left: orders,
      right: users,
      on: "`orders`.`user_id` = `users`.`user_id`",
      type: "inner",
      hints: { broadcast: "right" },
    })

    const sink = KafkaSink({
      topic: "enriched-orders",
      children: [joined],
    })

    const pipeline = Pipeline({
      name: "broadcast-join-pipeline",
      children: [sink],
    })

    // Hint emission pass collects annotations but actual SQL hint rendering
    // is handled by the existing buildJoinQuery in sql-generator.ts
    const result = generateSql(pipeline, { optimize: true })

    // The existing join builder already handles BROADCAST hints
    expect(result.sql).toContain("/*+ BROADCAST(")
    expect(result.sql).toMatchSnapshot()
  })

  // ── 6.4: Join with stateTtl hint ───────────────────────────────────

  it("collects STATE_TTL hint for Join with stateTtl", () => {
    const orders = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const users = KafkaSource({
      topic: "users",
      schema: UserSchema,
      bootstrapServers: "kafka:9092",
    })

    const joined = Join({
      left: orders,
      right: users,
      on: "`orders`.`user_id` = `users`.`user_id`",
      type: "inner",
      stateTtl: "1 hour",
    })

    const sink = KafkaSink({
      topic: "enriched-orders",
      children: [joined],
    })

    const pipeline = Pipeline({
      name: "state-ttl-pipeline",
      children: [sink],
    })

    const result = optimizePipeline(pipeline, "2.0")

    // Should have a STATE_TTL annotation on the join node
    expect(result.hintAnnotations.size).toBeGreaterThan(0)
    const hints = [...result.hintAnnotations.values()]
    expect(hints.some((h) => h.includes("STATE_TTL"))).toBe(true)
  })
})

// ── 6.5: Source with unused columns → narrowed DDL ──────────────────

describe("Source schema narrowing pass", () => {
  it("narrows source DDL to only referenced columns", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    // Only references user_id and amount via Map
    const mapped = MapTransform({
      select: { user_id: "`user_id`", total: "`amount`" },
      children: [source],
    })

    const sink = KafkaSink({
      topic: "slim-orders",
      children: [mapped],
    })

    const pipeline = Pipeline({
      name: "narrowing-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline, { optimize: true })

    // Should NOT contain unused columns in source DDL
    // order_id, product_id, category should be excluded
    // event_time must be kept (watermark column)
    expect(result.sql).toContain("`user_id`")
    expect(result.sql).toContain("`amount`")
    expect(result.sql).toContain("`event_time`") // watermark column must be kept

    expect(result.sql).toMatchSnapshot()
  })

  // ── 6.6: Source with all columns used → full DDL preserved ─────────

  it("preserves full DDL when all columns are used", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    // Filter passes through all columns
    const filter = Filter({
      condition: "`amount` > 100",
      children: [source],
    })

    const sink = KafkaSink({
      topic: "filtered-orders",
      children: [filter],
    })

    const pipeline = Pipeline({
      name: "full-schema-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline, { optimize: true })

    // Filter passes through all columns, so schema should be the same
    // (Filter triggers conservative fallback since it references all upstream)
    expect(result.sql).toContain("`order_id`")
    expect(result.sql).toContain("`user_id`")
    expect(result.sql).toContain("`amount`")
  })
})

// ── 6.7: Route with `1 = 0` → branch eliminated ────────────────────

describe("Dead branch elimination pass", () => {
  it("eliminates Route branches with statically false conditions", () => {
    const source = KafkaSource({
      topic: "events",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const pipeline = Pipeline({
      name: "dead-branch-pipeline",
      children: [
        source,
        Route({
          children: [
            Route.Branch({
              condition: "1 = 0",
              children: [KafkaSink({ topic: "dead-branch" })],
            }),
            Route.Branch({
              condition: "`amount` > 100",
              children: [KafkaSink({ topic: "live-branch" })],
            }),
            Route.Default({
              children: [KafkaSink({ topic: "default-branch" })],
            }),
          ],
        }),
      ],
    })

    const result = generateSql(pipeline, { optimize: true })

    // Should NOT contain the dead-branch sink's topic
    expect(result.sql).not.toContain("'dead-branch'")
    // Should contain live branches
    expect(result.sql).toContain("'live-branch'")
    expect(result.sql).toContain("'default-branch'")

    expect(result.sql).toMatchSnapshot()
  })

  it("eliminates branches with string contradiction", () => {
    const result = optimizePipeline(
      Pipeline({
        name: "test",
        children: [
          KafkaSource({
            topic: "src",
            schema: UserSchema,
            bootstrapServers: "kafka:9092",
          }),
          Route({
            children: [
              Route.Branch({
                condition: "'a' = 'b'",
                children: [KafkaSink({ topic: "dead" })],
              }),
              Route.Branch({
                condition: "`name` = 'test'",
                children: [KafkaSink({ topic: "live" })],
              }),
            ],
          }),
        ],
      }),
      "2.0",
    )

    // The dead branch should be removed from the tree
    const route = result.tree.children.find((c) => c.component === "Route")
    expect(route).toBeDefined()
    const branches = route!.children.filter(
      (c) => c.component === "Route.Branch",
    )
    expect(branches).toHaveLength(1)
  })
})

// ── 6.8: optimize: false → no modifications ─────────────────────────

describe("optimize: false", () => {
  it("produces identical output when optimize is false", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const agg = Aggregate({
      groupBy: ["user_id"],
      select: { total_amount: "SUM(`amount`)", order_count: "COUNT(*)" },
      children: [source],
    })

    const sink = KafkaSink({
      topic: "user-totals",
      children: [agg],
    })

    const pipeline = Pipeline({
      name: "no-optimize-pipeline",
      children: [sink],
    })

    resetNodeIdCounter()
    const withoutOptimize = generateSql(pipeline)

    resetNodeIdCounter()
    const withOptimizeFalse = generateSql(pipeline, { optimize: false })

    expect(withOptimizeFalse.sql).toBe(withoutOptimize.sql)
  })

  it("does not inject SET statements when optimize is disabled", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const agg = Aggregate({
      groupBy: ["user_id"],
      select: { total_amount: "SUM(`amount`)" },
      children: [source],
    })

    const sink = KafkaSink({
      topic: "totals",
      children: [agg],
    })

    const pipeline = Pipeline({
      name: "no-opt",
      children: [sink],
    })

    const result = generateSql(pipeline)

    expect(result.sql).not.toContain("mini-batch")
  })
})

// ── 6.9: User SET overrides auto-injected SET ───────────────────────

describe("User SET precedence", () => {
  it("does not override user-defined SET statements", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const agg = Aggregate({
      groupBy: ["user_id"],
      select: { total_amount: "SUM(`amount`)" },
      children: [source],
    })

    const sink = KafkaSink({
      topic: "totals",
      children: [agg],
    })

    const pipeline = Pipeline({
      name: "user-set-pipeline",
      flinkConfig: {
        "table.exec.mini-batch.enabled": "false",
        "table.exec.mini-batch.allow-latency": "10 s",
      },
      children: [sink],
    })

    const result = generateSql(pipeline, { optimize: true })

    // User's value should appear, optimizer should NOT override
    expect(result.sql).toContain(
      "SET 'table.exec.mini-batch.enabled' = 'false';",
    )
    expect(result.sql).toContain(
      "SET 'table.exec.mini-batch.allow-latency' = '10 s';",
    )

    // Optimizer should NOT inject conflicting values
    const miniBatchEnabledMatches = result.sql.match(
      /SET 'table\.exec\.mini-batch\.enabled'/g,
    )
    expect(miniBatchEnabledMatches).toHaveLength(1) // only user's value

    expect(result.sql).toMatchSnapshot()
  })
})

// ── 6.10: Snapshot tests for deterministic output ────────────────────

describe("Deterministic snapshot tests", () => {
  it("optimizer produces deterministic output for aggregate pipeline", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const agg = Aggregate({
      groupBy: ["user_id"],
      select: { total_amount: "SUM(`amount`)", order_count: "COUNT(*)" },
      children: [source],
    })

    const sink = KafkaSink({
      topic: "user-totals",
      children: [agg],
    })

    const pipeline = Pipeline({
      name: "deterministic-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline, { optimize: true })
    expect(result.sql).toMatchSnapshot()
  })

  it("optimizer produces deterministic output for join pipeline", () => {
    const orders = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const users = KafkaSource({
      topic: "users",
      schema: UserSchema,
      bootstrapServers: "kafka:9092",
    })

    const joined = Join({
      left: orders,
      right: users,
      on: "`orders`.`user_id` = `users`.`user_id`",
      type: "inner",
      hints: { broadcast: "right" },
    })

    const sink = KafkaSink({
      topic: "enriched",
      children: [joined],
    })

    const pipeline = Pipeline({
      name: "deterministic-join-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline, { optimize: true })
    expect(result.sql).toMatchSnapshot()
  })

  it("optimizer produces deterministic output for route pipeline", () => {
    const source = KafkaSource({
      topic: "events",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const pipeline = Pipeline({
      name: "deterministic-route-pipeline",
      children: [
        source,
        Route({
          children: [
            Route.Branch({
              condition: "`amount` > 1000",
              children: [KafkaSink({ topic: "high-value" })],
            }),
            Route.Branch({
              condition: "1 = 0",
              children: [KafkaSink({ topic: "never" })],
            }),
            Route.Default({
              children: [KafkaSink({ topic: "other" })],
            }),
          ],
        }),
      ],
    })

    const result = generateSql(pipeline, { optimize: true })
    expect(result.sql).toMatchSnapshot()
  })
})

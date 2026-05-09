import { beforeEach, describe, expect, it } from "vitest"
import { generateSql } from "@/codegen/sql/sql-generator.js"
import { CatalogSource } from "@/components/catalog-source.js"
import { PaimonCatalog } from "@/components/catalogs.js"
import { MatchRecognize } from "@/components/cep.js"
import { RawSQL, UDF } from "@/components/escape-hatches.js"
import { IntervalJoin, Join, LookupJoin } from "@/components/joins.js"
import { LateralJoin } from "@/components/lateral-join.js"
import { Pipeline } from "@/components/pipeline.js"
import { Route } from "@/components/route.js"
import { SideOutput } from "@/components/side-output.js"
import { JdbcSink, KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import {
  Aggregate,
  Deduplicate,
  Filter,
  Map,
  TopN,
} from "@/components/transforms.js"
import { Validate } from "@/components/validate.js"
import { View } from "@/components/view.js"
import { TumbleWindow } from "@/components/windows.js"
import { createElement, resetNodeIdCounter } from "@/core/jsx-runtime.js"
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

const UserSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    name: Field.STRING(),
    email: Field.STRING(),
  },
})

const EventSchema = Schema({
  fields: {
    event_id: Field.BIGINT(),
    user_id: Field.STRING(),
    event_type: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
    payload: Field.STRING(),
  },
  watermark: {
    column: "event_time",
    expression: "`event_time` - INTERVAL '5' SECOND",
  },
})

// ── 6.1: KafkaSource → Filter → KafkaSink ──────────────────────────

describe("6.1: KafkaSource → Filter → KafkaSink", () => {
  it("produces correct DDL + DML", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      schema: OrderSchema,
    })

    const filter = Filter({
      condition: "`amount` > 100",
      children: [source],
    })

    const sink = KafkaSink({
      topic: "large-orders",
      format: "json",
      children: [filter],
    })

    const pipeline = Pipeline({
      name: "filter-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 6.2: Two-table inner join ───────────────────────────────────────

describe("6.2: Two-table inner join", () => {
  it("produces correct JOIN SQL", () => {
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
      on: "`KafkaSource_0`.`user_id` = `KafkaSource_1`.`user_id`",
      type: "inner",
    })

    const sink = KafkaSink({
      topic: "enriched-orders",
      children: [joined],
    })

    const pipeline = Pipeline({
      name: "join-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 6.3: Windowed aggregation with TUMBLE TVF ──────────────────────

describe("6.3: Windowed aggregation with TUMBLE", () => {
  it("produces correct TVF + GROUP BY", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const agg = Aggregate({
      groupBy: ["user_id"],
      select: { total_amount: "SUM(`amount`)", order_count: "COUNT(*)" },
    })

    // Tree: Sink → TumbleWindow(children: [Aggregate, Source])
    // Window wraps the source; aggregate defines the computation
    const window = TumbleWindow({
      size: "1 hour",
      on: "event_time",
      children: [agg, source],
    })

    const sink = KafkaSink({
      topic: "hourly-totals",
      children: [window],
    })

    const pipeline = Pipeline({
      name: "windowed-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 6.4: Deduplicate → ROW_NUMBER ──────────────────────────────────

describe("6.4: Deduplicate produces ROW_NUMBER pattern", () => {
  it("produces correct ROW_NUMBER with WHERE rownum = 1", () => {
    const source = KafkaSource({
      topic: "events",
      schema: EventSchema,
      bootstrapServers: "kafka:9092",
    })

    const dedup = Deduplicate({
      key: ["event_id"],
      order: "event_time",
      keep: "first",
      children: [source],
    })

    const sink = KafkaSink({
      topic: "deduped-events",
      children: [dedup],
    })

    const pipeline = Pipeline({
      name: "dedup-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 6.5: TopN → ROW_NUMBER ─────────────────────────────────────────

describe("6.5: TopN produces ROW_NUMBER pattern", () => {
  it("produces correct ROW_NUMBER with WHERE rownum <= N", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const topN = TopN({
      partitionBy: ["user_id"],
      orderBy: { amount: "DESC" },
      n: 5,
      children: [source],
    })

    const sink = KafkaSink({
      topic: "top-orders",
      children: [topN],
    })

    const pipeline = Pipeline({
      name: "topn-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── QUALIFY: Version-aware codegen ───────────────────────────────────

describe("QUALIFY: Deduplicate version-aware codegen", () => {
  function makeDedupPipeline() {
    const source = KafkaSource({
      topic: "events",
      schema: EventSchema,
      bootstrapServers: "kafka:9092",
    })
    const dedup = Deduplicate({
      key: ["event_id"],
      order: "event_time",
      keep: "first",
      children: [source],
    })
    const sink = KafkaSink({ topic: "deduped-events", children: [dedup] })
    return Pipeline({ name: "dedup-pipeline", children: [sink] })
  }

  it("uses QUALIFY on Flink 2.0", () => {
    const result = generateSql(makeDedupPipeline(), { flinkVersion: "2.0" })
    expect(result.sql).toContain("QUALIFY rownum = 1")
    expect(result.sql).not.toContain("WHERE rownum")
    expect(result.sql).toMatchSnapshot()
  })

  it("uses subquery wrapper on Flink 1.20", () => {
    const result = generateSql(makeDedupPipeline(), { flinkVersion: "1.20" })
    expect(result.sql).toContain("WHERE rownum = 1")
    expect(result.sql).not.toContain("QUALIFY")
    expect(result.sql).toMatchSnapshot()
  })
})

describe("QUALIFY: TopN version-aware codegen", () => {
  function makeTopNPipeline() {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })
    const topN = TopN({
      partitionBy: ["user_id"],
      orderBy: { amount: "DESC" },
      n: 5,
      children: [source],
    })
    const sink = KafkaSink({ topic: "top-orders", children: [topN] })
    return Pipeline({ name: "topn-pipeline", children: [sink] })
  }

  it("uses QUALIFY on Flink 2.0", () => {
    const result = generateSql(makeTopNPipeline(), { flinkVersion: "2.0" })
    expect(result.sql).toContain("QUALIFY rownum <= 5")
    expect(result.sql).not.toContain("WHERE rownum")
    expect(result.sql).toMatchSnapshot()
  })

  it("uses subquery wrapper on Flink 1.20", () => {
    const result = generateSql(makeTopNPipeline(), { flinkVersion: "1.20" })
    expect(result.sql).toContain("WHERE rownum <= 5")
    expect(result.sql).not.toContain("QUALIFY")
    expect(result.sql).toMatchSnapshot()
  })
})

// ── QUALIFY: Escape-hatch component codegen ─────────────────────────

describe("QUALIFY: Qualify escape-hatch component", () => {
  it("generates QUALIFY with condition only", () => {
    const source = KafkaSource({
      topic: "events",
      schema: EventSchema,
      bootstrapServers: "kafka:9092",
    })
    const qualify = createElement("Qualify", { condition: "rn = 1" }, source)
    const sink = KafkaSink({ topic: "output", children: [qualify] })
    const pipeline = Pipeline({ name: "qualify-pipeline", children: [sink] })

    const result = generateSql(pipeline, { flinkVersion: "2.0" })
    expect(result.sql).toContain("QUALIFY rn = 1")
    expect(result.sql).toMatchSnapshot()
  })

  it("generates QUALIFY with inline window expression", () => {
    const source = KafkaSource({
      topic: "events",
      schema: EventSchema,
      bootstrapServers: "kafka:9092",
    })
    const qualify = createElement(
      "Qualify",
      {
        condition: "rn <= 3",
        window:
          "ROW_NUMBER() OVER (PARTITION BY `event_type` ORDER BY `event_time` DESC) AS rn",
      },
      source,
    )
    const sink = KafkaSink({ topic: "output", children: [qualify] })
    const pipeline = Pipeline({ name: "qualify-pipeline", children: [sink] })

    const result = generateSql(pipeline, { flinkVersion: "2.0" })
    expect(result.sql).toContain("QUALIFY rn <= 3")
    expect(result.sql).toContain("ROW_NUMBER() OVER")
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 6.6: Multi-sink → STATEMENT SET ────────────────────────────────

describe("6.6: Multi-sink produces STATEMENT SET", () => {
  it("wraps multiple INSERT INTO in STATEMENT SET", () => {
    const source = KafkaSource({
      topic: "events",
      schema: EventSchema,
      bootstrapServers: "kafka:9092",
    })

    const _errorSink = KafkaSink({ topic: "errors" })
    const _allSink = KafkaSink({ topic: "all-events" })

    const errorFilter = Filter({
      condition: "`event_type` = 'ERROR'",
      children: [source],
    })

    // Source feeds into both sinks via different paths
    const pipeline = Pipeline({
      name: "multi-sink-pipeline",
      children: [
        KafkaSink({
          topic: "errors",
          children: [errorFilter],
        }),
        KafkaSink({
          topic: "all-events",
          children: [source],
        }),
      ],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 6.7: Catalog pipeline → CREATE CATALOG ─────────────────────────

describe("6.7: Catalog pipeline produces CREATE CATALOG", () => {
  it("generates CREATE CATALOG and catalog-qualified references", () => {
    const { node: catalogNode, handle } = PaimonCatalog({
      name: "lake",
      warehouse: "s3://bucket/paimon",
    })

    const catSource = CatalogSource({
      catalog: handle,
      database: "analytics",
      table: "page_views",
    })

    const sink = KafkaSink({
      topic: "page-view-alerts",
      children: [catSource],
    })

    const pipeline = Pipeline({
      name: "catalog-pipeline",
      children: [catalogNode, sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 6.8: CDC source with debezium-json ──────────────────────────────

describe("6.8: CDC source with debezium-json", () => {
  it("produces Kafka source DDL with debezium-json format", () => {
    const source = KafkaSource({
      topic: "dbserver1.inventory.orders",
      format: "debezium-json",
      bootstrapServers: "kafka:9092",
      schema: Schema({
        fields: {
          order_id: Field.BIGINT(),
          product: Field.STRING(),
          quantity: Field.INT(),
          price: Field.DECIMAL(10, 2),
        },
        primaryKey: { columns: ["order_id"] },
      }),
    })

    const sink = JdbcSink({
      url: "jdbc:postgresql://localhost:5433/warehouse",
      table: "orders_snapshot",
      upsertMode: true,
      keyFields: ["order_id"],
      children: [source],
    })

    const pipeline = Pipeline({
      name: "cdc-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 6.8b: CDC source with debezium-protobuf ─────────────────────────

describe("6.8b: CDC source with debezium-protobuf", () => {
  it("produces Kafka source DDL with debezium-protobuf format and schema-registry.url", () => {
    const source = KafkaSource({
      topic: "dbserver1.inventory.orders",
      format: "debezium-protobuf",
      bootstrapServers: "kafka:9092",
      schemaRegistryUrl: "http://schema-registry:8081",
      schema: Schema({
        fields: {
          order_id: Field.BIGINT(),
          product: Field.STRING(),
          quantity: Field.INT(),
          price: Field.DECIMAL(10, 2),
        },
        primaryKey: { columns: ["order_id"] },
      }),
    })

    const sink = JdbcSink({
      url: "jdbc:postgresql://localhost:5433/warehouse",
      table: "orders_snapshot",
      upsertMode: true,
      keyFields: ["order_id"],
      children: [source],
    })

    const pipeline = Pipeline({
      name: "cdc-protobuf-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
    expect(result.sql).toContain("'format' = 'debezium-protobuf'")
    expect(result.sql).toContain(
      "'debezium-protobuf.schema-registry.url' = 'http://schema-registry:8081'",
    )
  })
})

// ── 6.9: LookupJoin with async config ──────────────────────────────

describe("6.9: LookupJoin with async config", () => {
  it("produces lookup join with proc_time and FOR SYSTEM_TIME AS OF", () => {
    const stream = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const lookupJoined = LookupJoin({
      input: stream,
      table: "dim_users",
      url: "jdbc:mysql://localhost:3306/mydb",
      on: "`KafkaSource_0`.`user_id` = `dim_users`.`user_id`",
      async: { enabled: true, capacity: 100, timeout: "10s" },
      cache: { type: "lru", maxRows: 10000, ttl: "1h" },
    })

    const sink = KafkaSink({
      topic: "enriched-orders",
      children: [lookupJoined],
    })

    const pipeline = Pipeline({
      name: "lookup-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 6.10: IntervalJoin with time bounds ─────────────────────────────

describe("6.10: IntervalJoin with time bounds", () => {
  it("produces BETWEEN ... AND ... time predicates", () => {
    const orders = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const shipments = KafkaSource({
      topic: "shipments",
      schema: Schema({
        fields: {
          order_id: Field.BIGINT(),
          shipped_time: Field.TIMESTAMP(3),
          carrier: Field.STRING(),
        },
        watermark: {
          column: "shipped_time",
          expression: "`shipped_time` - INTERVAL '5' SECOND",
        },
      }),
      bootstrapServers: "kafka:9092",
    })

    const joined = IntervalJoin({
      left: orders,
      right: shipments,
      on: "`KafkaSource_0`.`order_id` = `KafkaSource_1`.`order_id`",
      interval: {
        from: "event_time",
        to: "event_time + INTERVAL '7' DAY",
      },
      type: "inner",
    })

    const sink = KafkaSink({
      topic: "order-shipments",
      children: [joined],
    })

    const pipeline = Pipeline({
      name: "interval-join-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 6.11: Anti join → WHERE NOT EXISTS ──────────────────────────────

describe("6.11: Anti join produces WHERE NOT EXISTS", () => {
  it("generates correlated subquery with NOT EXISTS", () => {
    const allOrders = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const cancelledOrders = KafkaSource({
      topic: "cancellations",
      schema: Schema({
        fields: {
          order_id: Field.BIGINT(),
          cancelled_at: Field.TIMESTAMP(3),
        },
      }),
      bootstrapServers: "kafka:9092",
    })

    const antiJoined = Join({
      left: allOrders,
      right: cancelledOrders,
      on: "`KafkaSource_0`.`order_id` = `KafkaSource_1`.`order_id`",
      type: "anti",
    })

    const sink = KafkaSink({
      topic: "active-orders",
      children: [antiJoined],
    })

    const pipeline = Pipeline({
      name: "anti-join-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 6.12: Broadcast hint in join SQL ────────────────────────────────

describe("6.12: Broadcast hint in join SQL", () => {
  it("generates /*+ BROADCAST(alias) */ hint", () => {
    const orders = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const dimProducts = KafkaSource({
      topic: "products",
      schema: Schema({
        fields: {
          product_id: Field.STRING(),
          product_name: Field.STRING(),
          category: Field.STRING(),
        },
      }),
      bootstrapServers: "kafka:9092",
    })

    const joined = Join({
      left: orders,
      right: dimProducts,
      on: "`KafkaSource_0`.`product_id` = `KafkaSource_1`.`product_id`",
      type: "inner",
      hints: { broadcast: "right" },
    })

    const sink = KafkaSink({
      topic: "enriched-orders",
      children: [joined],
    })

    const pipeline = Pipeline({
      name: "broadcast-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 9.1: RawSQL inlines SQL in pipeline output ──────────────────────

describe("9.1: RawSQL inlines SQL in pipeline", () => {
  it("inlines raw SQL as a subquery", () => {
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

    const raw = RawSQL({
      sql: "SELECT o.*, u.name AS user_name FROM `KafkaSource_0` o JOIN `KafkaSource_1` u ON o.`user_id` = u.`user_id`",
      inputs: [orders, users],
      outputSchema: Schema({
        fields: {
          order_id: Field.BIGINT(),
          user_name: Field.STRING(),
        },
      }),
    })

    const sink = KafkaSink({
      topic: "enriched-orders",
      children: [raw],
    })

    const pipeline = Pipeline({
      name: "raw-sql-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── 9.1.1: RawSQL participates in sibling-chain JSX ─────────────────

describe("9.1.1: RawSQL in sibling-chain", () => {
  it("acts as a chain start when self-contained (no inputs)", () => {
    const WordSchema = Schema({
      fields: { word: Field.STRING(), frequency: Field.INT() },
    })

    const raw = RawSQL({
      sql: "SELECT `word`, `frequency` FROM (VALUES ('Hello', 1), ('World', 2)) AS T(`word`, `frequency`)",
      outputSchema: WordSchema,
    })

    const aggregated = Aggregate({
      groupBy: ["word"],
      select: { word: "`word`", frequency: "SUM(`frequency`)" },
    })

    const sink = KafkaSink({ topic: "wordcount" })

    const pipeline = Pipeline({
      name: "wordcount-sql",
      children: [raw, aggregated, sink],
    })

    const result = generateSql(pipeline)

    // The sibling chain RawSQL → Aggregate → Sink wires automatically:
    //  • the RawSQL body becomes the FROM subquery
    //  • the Aggregate's GROUP BY references columns from RawSQL.outputSchema
    //  • no orphan-source error from a phantom bootstrap
    expect(result.sql).toMatch(/INSERT INTO `wordcount`/)
    expect(result.sql).toMatch(/GROUP BY `word`/)
    expect(result.sql).toMatch(/SUM\(`frequency`\)/)
    expect(result.sql).toMatch(/VALUES/)
    // No phantom source CREATE TABLE — only the sink's CREATE TABLE remains
    const createTableCount = (result.sql.match(/CREATE TABLE/g) ?? []).length
    expect(createTableCount).toBe(1)
  })

  it("validates downstream column references against outputSchema", () => {
    const WordSchema = Schema({
      fields: { word: Field.STRING(), frequency: Field.INT() },
    })

    const raw = RawSQL({
      sql: "SELECT `word`, `frequency` FROM (VALUES ('Hello', 1)) AS T(`word`, `frequency`)",
      outputSchema: WordSchema,
    })

    // `unknown_column` does not exist in WordSchema — synthesis should still
    // succeed (the validator surfaces this as a separate diagnostic), but the
    // outputSchema is what downstream sees, not the input subquery columns.
    const mapped = Map({
      select: { word: "`word`", upper_word: "UPPER(`word`)" },
    })

    const sink = KafkaSink({ topic: "uppercased" })

    const pipeline = Pipeline({
      name: "raw-sql-with-map",
      children: [raw, mapped, sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatch(/INSERT INTO `uppercased`/)
    expect(result.sql).toMatch(/UPPER\(`word`\)/)
  })
})

// ── 9.2: UDF generates CREATE FUNCTION DDL ──────────────────────────

describe("9.2: UDF generates CREATE FUNCTION DDL", () => {
  it("produces CREATE FUNCTION statement", () => {
    const udf = UDF({
      name: "my_hash",
      className: "com.mycompany.HashFunction",
      jarPath: "/path/to/udf.jar",
    })

    const source = KafkaSource({
      topic: "events",
      schema: EventSchema,
      bootstrapServers: "kafka:9092",
    })

    const mapped = Map({
      select: { event_id: "`event_id`", hashed: "my_hash(`payload`)" },
      children: [source],
    })

    const sink = KafkaSink({
      topic: "hashed-events",
      children: [mapped],
    })

    const pipeline = Pipeline({
      name: "udf-pipeline",
      children: [udf, sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
    expect(result.statements.some((s) => s.includes("CREATE FUNCTION"))).toBe(
      true,
    )
    expect(
      result.statements.some((s) => s.includes("'com.mycompany.HashFunction'")),
    ).toBe(true)
  })
})

// ── 9.3: MatchRecognize generates MATCH_RECOGNIZE clause ────────────

describe("9.3: MatchRecognize generates MATCH_RECOGNIZE clause", () => {
  it("produces MATCH_RECOGNIZE with PARTITION BY, ORDER BY, MEASURES, PATTERN, DEFINE", () => {
    const source = KafkaSource({
      topic: "transactions",
      schema: Schema({
        fields: {
          user_id: Field.STRING(),
          amount: Field.DECIMAL(10, 2),
          event_type: Field.STRING(),
          event_time: Field.TIMESTAMP(3),
        },
        watermark: {
          column: "event_time",
          expression: "`event_time` - INTERVAL '5' SECOND",
        },
      }),
      bootstrapServers: "kafka:9092",
    })

    const cep = MatchRecognize({
      input: source,
      pattern: "A B+ C",
      define: {
        A: "A.`event_type` = 'login'",
        B: "B.`amount` > 1000",
        C: "C.`event_type` = 'withdrawal'",
      },
      measures: {
        start_time: "A.`event_time`",
        end_time: "C.`event_time`",
        total_amount: "SUM(B.`amount`)",
      },
      partitionBy: ["user_id"],
      orderBy: "event_time",
      after: "MATCH_RECOGNIZED",
    })

    const sink = KafkaSink({
      topic: "fraud-alerts",
      children: [cep],
    })

    const pipeline = Pipeline({
      name: "cep-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
    expect(result.sql).toContain("MATCH_RECOGNIZE")
    expect(result.sql).toContain("PATTERN (A B+ C)")
    expect(result.sql).toContain("DEFINE")
    expect(result.sql).toContain("MEASURES")
  })
})

// ── SideOutput: mid-pipeline tap with side sink ─────────────────────

describe("SideOutput produces STATEMENT SET with main + side INSERTs", () => {
  it("generates WHERE NOT for main path and WHERE for side path with metadata", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const sideSink = KafkaSink({ topic: "order-errors" })
    const sideSinkWrapper = SideOutput.Sink({ children: sideSink })

    const sideOutput = SideOutput({
      condition: "amount < 0 OR user_id IS NULL",
      tag: "invalid-order",
      children: [sideSinkWrapper, source],
    })

    const mainSink = KafkaSink({
      topic: "processed-orders",
      children: [sideOutput],
    })

    const pipeline = Pipeline({
      name: "side-output-pipeline",
      children: [mainSink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
    expect(result.sql).toContain("WHERE NOT (amount < 0 OR user_id IS NULL)")
    expect(result.sql).toContain("WHERE (amount < 0 OR user_id IS NULL)")
    expect(result.sql).toContain("_side_tag")
    expect(result.sql).toContain("_side_ts")
  })

  it("generates side output without tag", () => {
    const source = KafkaSource({
      topic: "events",
      schema: EventSchema,
      bootstrapServers: "kafka:9092",
    })

    const sideSink = KafkaSink({ topic: "bad-events" })
    const sideSinkWrapper = SideOutput.Sink({ children: sideSink })

    const sideOutput = SideOutput({
      condition: "payload IS NULL",
      children: [sideSinkWrapper, source],
    })

    const mainSink = KafkaSink({
      topic: "good-events",
      children: [sideOutput],
    })

    const pipeline = Pipeline({
      name: "no-tag-pipeline",
      children: [mainSink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
    expect(result.sql).toContain("_side_ts")
    expect(result.sql).not.toContain("_side_tag")
  })
})

// ── Validate: declarative data quality with reject routing ──────────

describe("Validate produces STATEMENT SET with valid + reject INSERTs", () => {
  it("generates full validation SQL with notNull, range, and expression rules", () => {
    const source = KafkaSource({
      topic: "raw-orders",
      schema: Schema({
        fields: {
          order_id: Field.BIGINT(),
          user_id: Field.STRING(),
          amount: Field.DECIMAL(10, 2),
          email: Field.STRING(),
        },
      }),
      bootstrapServers: "kafka:9092",
    })

    const rejectSink = KafkaSink({ topic: "invalid-orders" })
    const rejectWrapper = Validate.Reject({ children: rejectSink })

    const validate = Validate({
      rules: {
        notNull: ["order_id", "user_id", "amount"],
        range: { amount: [0, 1000000] },
        expression: { valid_email: "email LIKE '%@%.%'" },
      },
      children: [rejectWrapper, source],
    })

    const validSink = KafkaSink({
      topic: "valid-orders",
      children: [validate],
    })

    const pipeline = Pipeline({
      name: "validate-pipeline",
      children: [validSink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
    expect(result.sql).toContain("_validation_error")
    expect(result.sql).toContain("_validated_at")
    expect(result.sql).toContain("IS NOT NULL")
    expect(result.sql).toContain("CASE")
  })

  it("generates notNull-only validation", () => {
    const source = KafkaSource({
      topic: "events",
      schema: EventSchema,
      bootstrapServers: "kafka:9092",
    })

    const rejectSink = KafkaSink({ topic: "rejected" })
    const rejectWrapper = Validate.Reject({ children: rejectSink })

    const validate = Validate({
      rules: { notNull: ["event_id", "user_id"] },
      children: [rejectWrapper, source],
    })

    const validSink = KafkaSink({
      topic: "validated",
      children: [validate],
    })

    const pipeline = Pipeline({
      name: "notnull-pipeline",
      children: [validSink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
  })
})

// ── View: named reusable intermediate queries ───────────────────────

describe("View produces CREATE VIEW", () => {
  it("generates CREATE VIEW from upstream Map + Source", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
      bootstrapServers: "kafka:9092",
    })

    const mapped = Map({
      select: { order_id: "`order_id`", total: "amount * quantity" },
      children: [source],
    })

    const view = View({
      name: "enriched_orders",
      children: [mapped],
    })

    // High-value branch reads from the view
    const highValueSink = KafkaSink({
      topic: "high-value",
      children: [
        Filter({
          condition: "total > 1000",
          children: [View({ name: "enriched_orders" })],
        }),
      ],
    })

    // All orders branch also reads from the view
    const allSink = KafkaSink({
      topic: "all-orders",
      children: [View({ name: "enriched_orders" })],
    })

    const pipeline = Pipeline({
      name: "view-pipeline",
      children: [view, highValueSink, allSink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
    expect(result.sql).toContain("CREATE VIEW `enriched_orders`")
  })
})

// ── LateralJoin: table-valued function join ─────────────────────────

describe("LateralJoin produces LATERAL TABLE", () => {
  it("generates cross join lateral table SQL", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: Schema({
        fields: {
          order_id: Field.BIGINT(),
          shipping_address: Field.STRING(),
          amount: Field.DECIMAL(10, 2),
        },
      }),
      bootstrapServers: "kafka:9092",
    })

    const joined = LateralJoin({
      input: source,
      function: "parse_address",
      args: ["shipping_address"],
      as: { street: "STRING", city: "STRING", zip: "STRING" },
    })

    const sink = KafkaSink({
      topic: "enriched-orders",
      children: [joined],
    })

    const udf = UDF({
      name: "parse_address",
      className: "com.mycompany.fns.AddressParser",
      jarPath: "/opt/flink/udfs/address.jar",
    })

    const pipeline = Pipeline({
      name: "lateral-join-pipeline",
      children: [udf, sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
    expect(result.sql).toContain("LATERAL TABLE")
    expect(result.sql).toContain("parse_address")
  })

  it("generates left join lateral table SQL", () => {
    const source = KafkaSource({
      topic: "documents",
      schema: Schema({
        fields: {
          doc_id: Field.BIGINT(),
          content: Field.STRING(),
        },
      }),
      bootstrapServers: "kafka:9092",
    })

    const joined = LateralJoin({
      input: source,
      function: "extract_entities",
      args: ["content"],
      as: { entity: "STRING", entity_type: "STRING" },
      type: "left",
    })

    const sink = KafkaSink({
      topic: "entities",
      children: [joined],
    })

    const pipeline = Pipeline({
      name: "left-lateral-pipeline",
      children: [sink],
    })

    const result = generateSql(pipeline)
    expect(result.sql).toMatchSnapshot()
    expect(result.sql).toContain("LEFT JOIN LATERAL TABLE")
  })
})

// ── Route: conditional split with transforms in branches ─────────────

describe("Route produces correct multi-branch SQL", () => {
  it("generates STATEMENT SET with branch conditions and transforms (JSX forward-reading)", () => {
    const ProductSchema = Schema({
      fields: {
        id: Field.INT(),
        name: Field.STRING(),
        category: Field.STRING(),
        price: Field.DOUBLE(),
        quantity: Field.INT(),
      },
    })

    // JSX forward-reading pattern: Source and Route as siblings in Pipeline
    const source = KafkaSource({
      topic: "cdc.inventory.products",
      format: "debezium-json",
      bootstrapServers: "kafka:9092",
      schema: ProductSchema,
      startupMode: "earliest-offset",
      consumerGroup: "inventory-analytics",
    })

    const errorBranch = Route.Branch({
      condition: "`quantity` < 10",
      children: [
        Filter({ condition: "`quantity` >= 0" }),
        KafkaSink({
          topic: "low-stock-alerts",
          bootstrapServers: "kafka:9092",
        }),
      ],
    })

    const aggBranch = Route.Branch({
      condition: "true",
      children: [
        Aggregate({
          groupBy: ["category"],
          select: {
            total_items: "SUM(`quantity`)",
            avg_price: "AVG(`price`)",
            product_count: "COUNT(*)",
          },
        }),
        KafkaSink({ topic: "category-stats", bootstrapServers: "kafka:9092" }),
      ],
    })

    const route = Route({ children: [errorBranch, aggBranch] })

    const pipeline = Pipeline({
      name: "inventory-analytics",
      children: [source, route],
    })

    const result = generateSql(pipeline)

    // Source should have meaningful name and column definitions
    expect(result.sql).toContain("CREATE TABLE `cdc_inventory_products`")
    expect(result.sql).toContain("`id` INT")

    // Sinks should have column definitions
    expect(result.sql).toContain("CREATE TABLE `low_stock_alerts`")
    expect(result.sql).toContain("CREATE TABLE `category_stats`")

    // DML should reference source, not "unknown"
    expect(result.sql).not.toContain("unknown")

    // Branch 1: Filter + branch condition merged into single WHERE
    expect(result.sql).toContain("WHERE `quantity` >= 0 AND `quantity` < 10")

    // Branch 2: Aggregate
    expect(result.sql).toContain("GROUP BY `category`")
    expect(result.sql).toContain("SUM(`quantity`)")
    expect(result.sql).toContain("AVG(`price`)")
    expect(result.sql).toContain("COUNT(*)")

    // Multiple INSERT → STATEMENT SET
    expect(result.sql).toContain("EXECUTE STATEMENT SET BEGIN")

    expect(result.sql).toMatchSnapshot()
  })

  it("generates Route DML with programmatic reverse-nesting pattern", () => {
    const LogSchema = Schema({
      fields: {
        message: Field.STRING(),
        level: Field.STRING(),
        timestamp: Field.TIMESTAMP(3),
      },
    })

    const source = KafkaSource({
      topic: "logs",
      schema: LogSchema,
      bootstrapServers: "kafka:9092",
    })

    const errorBranch = Route.Branch({
      condition: "`level` = 'ERROR'",
      children: KafkaSink({ topic: "errors" }),
    })
    const warnBranch = Route.Branch({
      condition: "`level` = 'WARN'",
      children: KafkaSink({ topic: "warnings" }),
    })
    const defaultBranch = Route.Default({
      children: KafkaSink({ topic: "other" }),
    })

    const route = Route({
      children: [errorBranch, warnBranch, defaultBranch, source],
    })

    const pipeline = Pipeline({
      name: "log-router",
      children: [route],
    })

    const result = generateSql(pipeline)
    expect(result.sql).not.toContain("unknown")
    expect(result.sql).toContain("WHERE `level` = 'ERROR'")
    expect(result.sql).toContain("WHERE `level` = 'WARN'")
    expect(result.sql).toMatchSnapshot()
  })
})

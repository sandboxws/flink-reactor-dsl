import { Either } from "effect"
import { beforeEach, describe, expect, it } from "vitest"
import { generateSql, generateSqlEither } from "@/codegen/sql-generator.js"
import {
  FlussCatalog,
  GenericCatalog,
  HiveCatalog,
  JdbcCatalog,
  PaimonCatalog,
} from "@/components/catalogs.js"
import {
  AddField,
  Cast,
  Coalesce,
  Drop,
  Rename,
} from "@/components/field-transforms.js"
import { MaterializedTable } from "@/components/materialized-table.js"
import { Pipeline } from "@/components/pipeline.js"
import {
  FileSystemSink,
  FlussSink,
  GenericSink,
  KafkaSink,
} from "@/components/sinks.js"
import {
  DataGenSource,
  GenericSource,
  JdbcSource,
  KafkaSource,
} from "@/components/sources.js"
import { Aggregate, FlatMap, Map as MapTx } from "@/components/transforms.js"
import { SessionWindow, SlideWindow } from "@/components/windows.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import type { PluginDdlGenerator, PluginSqlGenerator } from "@/core/plugin.js"
import { Field, Schema } from "@/core/schema.js"
import type { ConstructNode } from "@/core/types.js"

beforeEach(() => {
  resetNodeIdCounter()
})

// ── Shared fixtures ─────────────────────────────────────────────────

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    user_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    event_time: Field.TIMESTAMP(3),
    tags: Field.ARRAY(Field.STRING()),
  },
  watermark: {
    column: "event_time",
    expression: "`event_time` - INTERVAL '5' SECOND",
  },
})

function basicKafkaSource() {
  return KafkaSource({
    topic: "orders",
    bootstrapServers: "kafka:9092",
    schema: OrderSchema,
  })
}

// ── Reentrancy tripwire ─────────────────────────────────────────────

describe("synthesis reentrancy guard", () => {
  it("throws when generateSql is invoked recursively (synchronous nesting)", () => {
    // The tripwire defends an invariant: codegen is purely synchronous, so
    // BuildContext fragments / synthDepth accumulators can be safely owned
    // per-call. The module-level `_activeSynthesisCount` in
    // sql-build-context.ts catches the case where a plugin re-enters
    // generateSql with a fresh ctx — that path would otherwise interleave
    // fragment writes from two synthesis passes.
    // We trigger it by registering a plugin SQL generator that re-enters
    // generateSql for the same component — that's the only realistic path
    // that exercises the recursive call site without modifying source.
    const innerPipeline = Pipeline({
      name: "inner",
      children: [KafkaSink({ topic: "out", children: [basicKafkaSource()] })],
    })
    const generators = new Map<string, PluginSqlGenerator>([
      [
        "Filter",
        () => {
          generateSql(innerPipeline)
          return "SELECT * FROM nope"
        },
      ],
    ])

    const source = basicKafkaSource()
    const filter = (() => {
      // Hand-build a Filter node so the plugin generator wins dispatch
      // without us depending on Filter's prop signature for this test.
      const Filter = (children: ConstructNode[]): ConstructNode => ({
        component: "Filter",
        kind: "Transform",
        id: "Filter_1",
        props: { condition: "TRUE" },
        children,
      })
      return Filter([source])
    })()
    const sink = KafkaSink({
      topic: "filtered",
      children: [filter as ConstructNode],
    })
    const outer = Pipeline({ name: "outer", children: [sink] })

    expect(() =>
      generateSql(outer, { pluginSqlGenerators: generators }),
    ).toThrow(/reentrant|already in progress|nested/i)
  })
})

// ── AddField name collision ─────────────────────────────────────────

describe("AddField name collision", () => {
  it("throws when an added field shadows an upstream column", () => {
    const source = basicKafkaSource()
    const collide = AddField({
      columns: {
        order_id: "`order_id` * 2", // already a column on OrderSchema
      },
      children: [source],
    })
    const sink = KafkaSink({ topic: "dst", children: [collide] })
    const pipeline = Pipeline({ name: "addfield-collide", children: [sink] })

    expect(() => generateSql(pipeline)).toThrow(
      /AddField name collision.*order_id/,
    )
  })
})

// ── MaterializedTable version checks ────────────────────────────────

describe("MaterializedTable version gating", () => {
  const matBuild = (overrides: Record<string, unknown> = {}) => {
    const cat = PaimonCatalog({ name: "lake", warehouse: "s3://bucket/lake" })
    const mat = MaterializedTable({
      name: "user_summary",
      catalog: cat.handle,
      database: "warehouse",
      freshness: "INTERVAL '30' SECOND",
      ...overrides,
      children: [
        Aggregate({
          select: { user_id: "user_id", cnt: "COUNT(*)" },
          groupBy: ["user_id"],
          children: [basicKafkaSource()],
        }),
      ],
    })
    return Pipeline({ name: "mat-pipeline", children: [cat.node, mat] })
  }

  it("throws on Flink 1.x (MATERIALIZED_TABLE feature gate)", () => {
    expect(() => generateSql(matBuild(), { flinkVersion: "1.20" })).toThrow(
      /materialized.*table|requires Flink/i,
    )
  })

  it("throws when freshness is omitted on Flink < 2.2", () => {
    expect(() =>
      generateSql(matBuild({ freshness: undefined }), {
        flinkVersion: "2.0",
      }),
    ).toThrow(/freshness is required for Flink < 2\.2/)
  })

  it("throws when bucketing is requested on Flink < 2.2", () => {
    expect(() =>
      generateSql(matBuild({ bucketing: { columns: ["user_id"], count: 4 } }), {
        flinkVersion: "2.0",
      }),
    ).toThrow(/bucket/i)
  })

  it("emits CREATE MATERIALIZED TABLE with all clauses on Flink 2.2", () => {
    const { sql } = generateSql(
      matBuild({
        comment: "hourly user rollup",
        partitionedBy: ["user_id"],
        bucketing: { columns: ["user_id"], count: 4 },
        with: { "snapshot.expire.execution-mode": "async" },
        refreshMode: "continuous",
      }),
      { flinkVersion: "2.2" },
    )
    expect(sql).toContain("CREATE MATERIALIZED TABLE")
    expect(sql).toContain("COMMENT 'hourly user rollup'")
    expect(sql).toContain("PARTITIONED BY (`user_id`)")
    expect(sql).toContain("DISTRIBUTED BY HASH(`user_id`) INTO 4 BUCKETS")
    expect(sql).toContain("'snapshot.expire.execution-mode' = 'async'")
    expect(sql).toContain("FRESHNESS = INTERVAL '30' SECOND")
    expect(sql).toContain("REFRESH_MODE = CONTINUOUS")
  })
})

// ── generateSqlEither (Effect-typed variant) ────────────────────────

describe("generateSqlEither", () => {
  it("returns Right(result) on success", () => {
    const pipeline = Pipeline({
      name: "ok",
      children: [KafkaSink({ topic: "dst", children: [basicKafkaSource()] })],
    })
    const result = generateSqlEither(pipeline)
    expect(Either.isRight(result)).toBe(true)
    if (Either.isRight(result)) {
      expect(result.right.sql).toContain("CREATE TABLE")
    }
  })

  it("returns Left(SqlGenerationError) on failure (AddField collision)", () => {
    const pipeline = Pipeline({
      name: "boom",
      children: [
        KafkaSink({
          topic: "dst",
          children: [
            AddField({
              columns: { order_id: "`order_id` * 2" },
              children: [basicKafkaSource()],
            }),
          ],
        }),
      ],
    })
    const result = generateSqlEither(pipeline)
    expect(Either.isLeft(result)).toBe(true)
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("SqlGenerationError")
      expect(result.left.message).toMatch(/AddField name collision/)
    }
  })
})

// ── Source DDL: under-tested variants ───────────────────────────────

describe("source DDL coverage for non-Kafka variants", () => {
  it("emits JdbcSource DDL with lookup-cache options when configured", () => {
    const src = JdbcSource({
      url: "jdbc:postgresql://pg:5432/shop",
      table: "users",
      schema: Schema({
        fields: { user_id: Field.STRING(), name: Field.STRING() },
      }),
      lookupCache: { maxRows: 10000, ttl: "10 min" },
    })
    const sink = KafkaSink({
      topic: "users-out",
      children: [MapTx({ select: { id: "user_id" }, children: [src] })],
    })
    const { sql } = generateSql(
      Pipeline({ name: "jdbc-pipe", children: [sink] }),
    )
    expect(sql).toContain("'connector' = 'jdbc'")
    expect(sql).toContain("'url' = 'jdbc:postgresql://pg:5432/shop'")
    expect(sql).toContain("'lookup.cache.max-rows' = '10000'")
    expect(sql).toContain("'lookup.cache.ttl' = '10 min'")
  })

  it("emits GenericSource DDL with passthrough options", () => {
    const src = GenericSource({
      connector: "filesystem",
      format: "csv",
      schema: Schema({
        fields: { id: Field.BIGINT(), payload: Field.STRING() },
      }),
      options: {
        path: "s3://bucket/in",
        "csv.field-delimiter": "|",
      },
    })
    const sink = KafkaSink({ topic: "dst", children: [src] })
    const { sql } = generateSql(
      Pipeline({ name: "generic-src", children: [sink] }),
    )
    expect(sql).toContain("'connector' = 'filesystem'")
    expect(sql).toContain("'format' = 'csv'")
    expect(sql).toContain("'path' = 's3://bucket/in'")
    expect(sql).toContain("'csv.field-delimiter' = '|'")
  })

  it("emits DataGenSource DDL with rate-limit options", () => {
    const src = DataGenSource({
      schema: Schema({
        fields: { id: Field.BIGINT(), name: Field.STRING() },
      }),
      rowsPerSecond: 100,
      numberOfRows: 1_000,
    })
    const sink = KafkaSink({ topic: "dst", children: [src] })
    const { sql } = generateSql(Pipeline({ name: "datagen", children: [sink] }))
    expect(sql).toContain("'connector' = 'datagen'")
    expect(sql).toContain("'rows-per-second' = '100'")
    expect(sql).toContain("'number-of-rows' = '1000'")
  })
})

// ── Sink DDL: under-tested variants ─────────────────────────────────

describe("sink DDL coverage for non-Kafka variants", () => {
  it("emits FileSystemSink DDL with format and partitionBy", () => {
    const sink = FileSystemSink({
      path: "s3://bucket/orders",
      format: "parquet",
      partitionBy: ["user_id"],
      children: [basicKafkaSource()],
    })
    const { sql } = generateSql(Pipeline({ name: "fs-sink", children: [sink] }))
    expect(sql).toContain("'connector' = 'filesystem'")
    expect(sql).toContain("'path' = 's3://bucket/orders'")
    expect(sql).toContain("'format' = 'parquet'")
    expect(sql).toContain("PARTITIONED BY (`user_id`)")
  })

  it("emits GenericSink DDL with passthrough options", () => {
    const sink = GenericSink({
      connector: "blackhole",
      options: { "sink.parallelism": "4" },
      children: [basicKafkaSource()],
    })
    const { sql } = generateSql(
      Pipeline({ name: "generic-sink", children: [sink] }),
    )
    expect(sql).toContain("'connector' = 'blackhole'")
    expect(sql).toContain("'sink.parallelism' = '4'")
  })
})

// ── Window variants ─────────────────────────────────────────────────

describe("window TVF variants", () => {
  it("emits SlideWindow as HOP TVF", () => {
    const slide = SlideWindow({
      size: "1 hour",
      slide: "15 minutes",
      on: "event_time",
      children: [
        Aggregate({
          select: { user_id: "user_id", cnt: "COUNT(*)" },
          groupBy: ["user_id"],
          children: [basicKafkaSource()],
        }),
      ],
    })
    const sink = KafkaSink({ topic: "rollup", children: [slide] })
    const { sql } = generateSql(
      Pipeline({ name: "slide-window", children: [sink] }),
    )
    expect(sql).toContain("HOP(")
    expect(sql).toContain("INTERVAL '15' MINUTE")
    expect(sql).toContain("INTERVAL '1' HOUR")
  })

  it("emits SessionWindow as SESSION TVF", () => {
    const session = SessionWindow({
      gap: "30 minutes",
      on: "event_time",
      children: [
        Aggregate({
          select: { user_id: "user_id", cnt: "COUNT(*)" },
          groupBy: ["user_id"],
          children: [basicKafkaSource()],
        }),
      ],
    })
    const sink = KafkaSink({ topic: "sessions", children: [session] })
    const { sql } = generateSql(
      Pipeline({ name: "session-window", children: [sink] }),
    )
    expect(sql).toContain("SESSION(")
    expect(sql).toContain("INTERVAL '30' MINUTE")
  })
})

// ── FlatMap ─────────────────────────────────────────────────────────

describe("FlatMap transform", () => {
  it("emits CROSS JOIN UNNEST for array unfolding", () => {
    const flat = FlatMap({
      unnest: "tags",
      as: { tag: "STRING" },
      children: [basicKafkaSource()],
    })
    const sink = KafkaSink({ topic: "flat", children: [flat] })
    const { sql } = generateSql(
      Pipeline({ name: "flat-map", children: [sink] }),
    )
    expect(sql).toContain("CROSS JOIN UNNEST")
    expect(sql).toContain("`tags`")
  })
})

// ── Field-transform error/edge paths ────────────────────────────────

describe("field-transform edge paths", () => {
  it("Rename keeps non-renamed columns intact (partial rename)", () => {
    const renamed = Rename({
      columns: { order_id: "id" },
      children: [basicKafkaSource()],
    })
    const sink = KafkaSink({ topic: "renamed", children: [renamed] })
    const { sql } = generateSql(
      Pipeline({ name: "partial-rename", children: [sink] }),
    )
    expect(sql).toContain("`order_id` AS `id`")
    // user_id retained verbatim
    expect(sql).toMatch(/`user_id`(?!\s*AS)/)
  })

  it("Drop excludes only the named columns", () => {
    const dropped = Drop({
      columns: ["amount"],
      children: [basicKafkaSource()],
    })
    const sink = KafkaSink({ topic: "thin", children: [dropped] })
    const { sql } = generateSql(
      Pipeline({ name: "drop-amount", children: [sink] }),
    )
    expect(sql).not.toMatch(/SELECT[^;]*`amount`/)
    expect(sql).toContain("`order_id`")
  })

  it("Cast emits CAST(x AS T)", () => {
    const cast = Cast({
      columns: { amount: "DOUBLE" },
      children: [basicKafkaSource()],
    })
    const sink = KafkaSink({ topic: "casted", children: [cast] })
    const { sql } = generateSql(
      Pipeline({ name: "cast-amount", children: [sink] }),
    )
    expect(sql).toContain("CAST(`amount` AS DOUBLE)")
  })

  it("Coalesce wraps the column with COALESCE(col, fallback)", () => {
    const coalesced = Coalesce({
      columns: { amount: "0" },
      children: [basicKafkaSource()],
    })
    const sink = KafkaSink({ topic: "filled", children: [coalesced] })
    const { sql } = generateSql(
      Pipeline({ name: "coalesce-amount", children: [sink] }),
    )
    expect(sql).toContain("COALESCE(`amount`, 0)")
  })
})

// ── Plugin SQL generator dispatch ───────────────────────────────────

describe("plugin SQL generator dispatch", () => {
  it("uses pluginSqlGenerators[node.component] when provided", () => {
    // Build a custom transform node that buildQuery() doesn't recognize;
    // the plugin entry must take precedence.
    const generators = new Map<string, PluginSqlGenerator>([
      [
        "MyCustomTransform",
        (node, _index) =>
          `SELECT 'plugin-rendered' AS marker, * FROM ${node.children[0].id}`,
      ],
    ])
    const source = basicKafkaSource()
    const customNode: ConstructNode = {
      component: "MyCustomTransform",
      kind: "Transform",
      id: "MyCustomTransform_1",
      props: {},
      children: [source],
    }
    const sink = KafkaSink({
      topic: "via-plugin",
      children: [customNode],
    })
    const { sql } = generateSql(
      Pipeline({ name: "plugin-dispatch", children: [sink] }),
      { pluginSqlGenerators: generators },
    )
    expect(sql).toContain("'plugin-rendered' AS marker")
  })

  it("uses pluginDdlGenerators[node.component] for sink DDL when provided", () => {
    // The sink-DDL plugin path: when a custom-component sink has a registered
    // DDL generator, the orchestrator emits its return value verbatim and
    // skips generateSinkDdl. Returning null is also valid — it means "skip
    // DDL emission entirely" (e.g. for sinks pre-registered out-of-band).
    const ddlGenerators = new Map<string, PluginDdlGenerator>([
      [
        "MyCustomSink",
        (node) =>
          `CREATE TABLE \`${node.id}\` (id BIGINT) WITH ('connector'='custom');`,
      ],
    ])
    const customSink: ConstructNode = {
      component: "MyCustomSink",
      kind: "Sink",
      id: "MyCustomSink_1",
      props: {},
      children: [basicKafkaSource()],
    }
    const { sql } = generateSql(
      Pipeline({ name: "plugin-ddl", children: [customSink] }),
      { pluginDdlGenerators: ddlGenerators },
    )
    expect(sql).toContain(
      "CREATE TABLE `MyCustomSink_1` (id BIGINT) WITH ('connector'='custom');",
    )
  })
})

// ── FlussSink without catalog (collectFlussTableProps direct path) ──

describe("FlussSink table-property emission", () => {
  it("collects bucket.num when buckets is set on a Flink-SQL FlussSink", () => {
    // collectFlussTableProps fires only on the Flink-SQL FlussSink path
    // (catalog-managed via FlussCatalog). The Pipeline-Connector path uses
    // a different emitter (pipeline-yaml-generator) and intentionally omits
    // these per-table properties.
    const cat = FlussCatalog({
      name: "fluss",
      bootstrapServers: "fluss:9123",
    })
    const sink = FlussSink({
      catalog: cat.handle,
      database: "shop",
      table: "orders",
      primaryKey: ["order_id"],
      buckets: 8,
      children: [basicKafkaSource()],
    })
    const { sql } = generateSql(
      Pipeline({ name: "fluss-buckets", children: [cat.node, sink] }),
    )
    expect(sql).toContain("'bucket.num' = '8'")
  })
})

// ── Catalog DDL: under-tested catalog types ─────────────────────────

describe("catalog DDL coverage for non-Paimon/Iceberg/Fluss catalogs", () => {
  // generateCatalogDdl emits CREATE CATALOG when the catalog appears in the
  // pipeline tree, even if no source/sink references it directly. We pair
  // each catalog with a Kafka source→sink so the pipeline has work to do.
  const noopSink = () =>
    KafkaSink({ topic: "dst", children: [basicKafkaSource()] })

  it("emits CREATE CATALOG for HiveCatalog", () => {
    const cat = HiveCatalog({
      name: "hive_meta",
      hiveConfDir: "/opt/flink/conf/hive",
    })
    const { sql } = generateSql(
      Pipeline({ name: "hive", children: [cat.node, noopSink()] }),
    )
    expect(sql).toContain("CREATE CATALOG `hive_meta`")
    expect(sql).toContain("'type' = 'hive'")
    expect(sql).toContain("'hive-conf-dir' = '/opt/flink/conf/hive'")
  })

  it("emits CREATE CATALOG for JdbcCatalog", () => {
    const cat = JdbcCatalog({
      name: "pg_cat",
      baseUrl: "jdbc:postgresql://pg:5432/",
      defaultDatabase: "shop",
    })
    const { sql } = generateSql(
      Pipeline({ name: "jdbc-cat", children: [cat.node, noopSink()] }),
    )
    expect(sql).toContain("CREATE CATALOG `pg_cat`")
    expect(sql).toContain("'type' = 'jdbc'")
    expect(sql).toContain("'base-url' = 'jdbc:postgresql://pg:5432/'")
    expect(sql).toContain("'default-database' = 'shop'")
  })

  it("emits CREATE CATALOG for GenericCatalog with passthrough options", () => {
    const cat = GenericCatalog({
      name: "custom_cat",
      type: "my-catalog",
      options: { uri: "thrift://meta:9083", "client.factor": "2" },
    })
    const { sql } = generateSql(
      Pipeline({ name: "generic-cat", children: [cat.node, noopSink()] }),
    )
    expect(sql).toContain("CREATE CATALOG `custom_cat`")
    expect(sql).toContain("'type' = 'my-catalog'")
    expect(sql).toContain("'uri' = 'thrift://meta:9083'")
    expect(sql).toContain("'client.factor' = '2'")
  })
})

// ── FileSystemSink partition function expansion ─────────────────────

describe("FileSystemSink partition function expansion", () => {
  // Flink PARTITIONED BY only takes physical column references, so the
  // emitter expands `DATE(col)` / `YEAR(col)` etc. into computed columns
  // and references the synthetic name in the partition clause. Each
  // function shape exercises a different branch of resolvePartitionExpression.
  it("expands DATE(col) into a computed partition column", () => {
    const sink = FileSystemSink({
      path: "s3://bucket/by-date",
      format: "parquet",
      partitionBy: ["DATE(event_time)"],
      children: [basicKafkaSource()],
    })
    const { sql } = generateSql(Pipeline({ name: "fs-date", children: [sink] }))
    expect(sql).toContain("PARTITIONED BY")
    // Computed partition column appears in the column list; the partition
    // clause references the synthetic column name, not the function call.
    expect(sql).not.toContain("PARTITIONED BY (`DATE(event_time)`)")
  })

  it("expands YEAR / MONTH / DAY / HOUR partition functions", () => {
    const sink = FileSystemSink({
      path: "s3://bucket/by-time",
      format: "parquet",
      partitionBy: [
        "YEAR(event_time)",
        "MONTH(event_time)",
        "DAY(event_time)",
        "HOUR(event_time)",
      ],
      children: [basicKafkaSource()],
    })
    const { sql } = generateSql(Pipeline({ name: "fs-ymdh", children: [sink] }))
    // Each function lifted to a synthetic column with stable name + type
    expect(sql).toContain("`yr` BIGINT")
    expect(sql).toContain("`mo` BIGINT")
    expect(sql).toContain("`dy` BIGINT")
    expect(sql).toContain("`hr` BIGINT")
    expect(sql).toContain("YEAR(CAST(`event_time` AS TIMESTAMP))")
  })

  it("falls through to a generic computed column for unknown partition functions", () => {
    const sink = FileSystemSink({
      path: "s3://bucket/by-region",
      format: "parquet",
      partitionBy: ["UPPER(user_id)"],
      children: [basicKafkaSource()],
    })
    const { sql } = generateSql(
      Pipeline({ name: "fs-upper", children: [sink] }),
    )
    expect(sql).toContain("`_p_upper`")
    expect(sql).toContain("UPPER(`user_id`)")
  })
})

// ── KafkaSink debezium schema-registry option ───────────────────────

describe("KafkaSink schema-registry threading", () => {
  it("threads schemaRegistryUrl onto debezium-avro KafkaSink WITH-clause", () => {
    const src = KafkaSource({
      topic: "src",
      bootstrapServers: "kafka:9092",
      format: "debezium-avro",
      schema: OrderSchema,
      schemaRegistryUrl: "http://schema-registry:8081",
    })
    const sink = KafkaSink({
      topic: "dst",
      format: "debezium-avro",
      schemaRegistryUrl: "http://schema-registry:8081",
      children: [src],
    })
    const { sql } = generateSql(
      Pipeline({ name: "schema-registry", children: [sink] }),
    )
    expect(sql).toContain(
      "'debezium-avro.schema-registry.url' = 'http://schema-registry:8081'",
    )
  })
})

// ── KafkaSource primaryKey / consumer group / format variants ───────

describe("KafkaSource WITH-clause branches", () => {
  it("emits upsert-kafka connector when primaryKey is set on a plain JSON source", () => {
    const src = KafkaSource({
      topic: "users",
      bootstrapServers: "kafka:9092",
      format: "json",
      primaryKey: ["user_id"],
      schema: Schema({
        fields: { user_id: Field.STRING(), name: Field.STRING() },
      }),
    })
    const sink = KafkaSink({ topic: "out", children: [src] })
    const { sql } = generateSql(
      Pipeline({ name: "kafka-upsert", children: [sink] }),
    )
    expect(sql).toContain("'connector' = 'upsert-kafka'")
    expect(sql).toContain("'key.format' = 'json'")
    expect(sql).toContain("'value.format' = 'json'")
    // upsert-kafka rejects scan.startup.mode
    expect(sql).not.toContain("scan.startup.mode")
  })

  it("emits scan.startup.mode + group.id for plain KafkaSource with consumerGroup", () => {
    const src = KafkaSource({
      topic: "events",
      bootstrapServers: "kafka:9092",
      format: "json",
      consumerGroup: "my-app",
      startupMode: "latest-offset",
      schema: Schema({
        fields: { id: Field.BIGINT(), payload: Field.STRING() },
      }),
    })
    const sink = KafkaSink({ topic: "out", children: [src] })
    const { sql } = generateSql(
      Pipeline({ name: "kafka-cg", children: [sink] }),
    )
    expect(sql).toContain("'scan.startup.mode' = 'latest-offset'")
    expect(sql).toContain("'properties.group.id' = 'my-app'")
  })

  it("keeps changelog formats on plain `kafka` connector with PK (debezium-json)", () => {
    const src = KafkaSource({
      topic: "cdc-orders",
      bootstrapServers: "kafka:9092",
      format: "debezium-json",
      primaryKey: ["order_id"],
      schema: OrderSchema,
    })
    const sink = KafkaSink({ topic: "out", children: [src] })
    const { sql } = generateSql(
      Pipeline({ name: "cdc-kafka", children: [sink] }),
    )
    // debezium-json natively carries op-state, no upsert-kafka rewrite
    expect(sql).toContain("'connector' = 'kafka'")
    expect(sql).toContain("'format' = 'debezium-json'")
  })
})

// ── KafkaSink upsert path (retract + primaryKey) ────────────────────

describe("KafkaSink upsert-kafka rewrite", () => {
  it("rewrites KafkaSink to upsert-kafka when downstream of a retract source", () => {
    // Retract changelog mode + primaryKey triggers the upsert-kafka branch
    // in generateSinkWithClause; the value format is also stripped to its
    // insert-only base.
    const src = KafkaSource({
      topic: "users-cdc",
      bootstrapServers: "kafka:9092",
      format: "debezium-json",
      primaryKey: ["user_id"],
      schema: Schema({
        fields: { user_id: Field.STRING(), email: Field.STRING() },
        primaryKey: { columns: ["user_id"] },
      }),
    })
    const sink = KafkaSink({
      topic: "users-mat",
      bootstrapServers: "kafka:9092",
      format: "debezium-json",
      children: [src],
    })
    const { sql } = generateSql(
      Pipeline({ name: "kafka-upsert-sink", children: [sink] }),
    )
    expect(sql).toContain("'connector' = 'upsert-kafka'")
    // debezium-json gets reduced to its insert-only base form
    expect(sql).toContain("'value.format' = 'json'")
  })
})

// ── Schema metadata columns ─────────────────────────────────────────

describe("source DDL with metadata columns", () => {
  it("emits METADATA FROM/VIRTUAL clauses when schema declares metadata columns", () => {
    const source = KafkaSource({
      topic: "events",
      bootstrapServers: "kafka:9092",
      schema: Schema({
        fields: { id: Field.BIGINT(), payload: Field.STRING() },
        metadataColumns: [
          {
            column: "kafka_ts",
            type: "TIMESTAMP_LTZ(3)",
            from: "timestamp",
            isVirtual: false,
          },
          {
            column: "headers",
            type: "MAP<STRING, BYTES>",
            from: "headers",
            isVirtual: true,
          },
        ],
      }),
    })
    const sink = KafkaSink({ topic: "dst", children: [source] })
    const { sql } = generateSql(
      Pipeline({ name: "metadata-cols", children: [sink] }),
    )
    expect(sql).toContain(
      "`kafka_ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'",
    )
    expect(sql).toContain(
      "`headers` MAP<STRING, BYTES> METADATA FROM 'headers' VIRTUAL",
    )
  })
})

import { beforeEach, describe, expect, it } from "vitest"
import { generateSql, generateTapManifest } from "@/codegen/sql-generator.js"
import { Pipeline } from "@/components/pipeline.js"
import { FileSystemSink, KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { Filter } from "@/components/transforms.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import {
  buildConsumerGroupId,
  buildObservationSql,
  generateTapMetadata,
  normalizeTapConfig,
  resolveObservationStrategy,
  validateTapConfig,
} from "@/core/tap.js"

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

// ── 6.1: buildConsumerGroupId ─────────────────────────────────────

describe("buildConsumerGroupId", () => {
  it("generates correct format with pipeline name and node ID", () => {
    const id = buildConsumerGroupId("ecommerce", "sink_1")
    expect(id).toMatch(/^flink-reactor-tap-ecommerce-sink_1-[0-9a-f]{8}$/)
  })

  it("is deterministic for the same inputs", () => {
    const id1 = buildConsumerGroupId("ecommerce", "sink_1")
    const id2 = buildConsumerGroupId("ecommerce", "sink_1")
    expect(id1).toBe(id2)
  })

  it("differs for different pipeline names", () => {
    const id1 = buildConsumerGroupId("ecommerce", "sink_1")
    const id2 = buildConsumerGroupId("analytics", "sink_1")
    expect(id1).not.toBe(id2)
  })

  it("differs for different node IDs", () => {
    const id1 = buildConsumerGroupId("ecommerce", "sink_1")
    const id2 = buildConsumerGroupId("ecommerce", "sink_2")
    expect(id1).not.toBe(id2)
  })

  it("uses custom prefix when provided", () => {
    const id = buildConsumerGroupId("ecommerce", "sink_1", "debug-session")
    expect(id).toMatch(/^debug-session-sink_1-[0-9a-f]{8}$/)
  })
})

// ── 6.2: resolveObservationStrategy ──────────────────────────────

describe("resolveObservationStrategy", () => {
  it("maps kafka to consumer-group-clone", () => {
    expect(resolveObservationStrategy("kafka")).toBe("consumer-group-clone")
  })

  it("maps upsert-kafka to consumer-group-clone", () => {
    expect(resolveObservationStrategy("upsert-kafka")).toBe(
      "consumer-group-clone",
    )
  })

  it("maps jdbc to periodic-poll", () => {
    expect(resolveObservationStrategy("jdbc")).toBe("periodic-poll")
  })

  it("maps paimon to incremental-read", () => {
    expect(resolveObservationStrategy("paimon")).toBe("incremental-read")
  })

  it("maps iceberg to incremental-read", () => {
    expect(resolveObservationStrategy("iceberg")).toBe("incremental-read")
  })

  it("maps datagen to direct-read", () => {
    expect(resolveObservationStrategy("datagen")).toBe("direct-read")
  })

  it("maps filesystem to unsupported", () => {
    expect(resolveObservationStrategy("filesystem")).toBe("unsupported")
  })

  it("maps unknown connector to unsupported", () => {
    expect(resolveObservationStrategy("custom-connector")).toBe("unsupported")
  })
})

// ── 6.3: Kafka source tap observation SQL ─────────────────────────

describe("Kafka source tap observation SQL", () => {
  it("generates correct observation SQL with consumer group clone", () => {
    const schema = {
      order_id: "BIGINT",
      user_id: "STRING",
      amount: "DECIMAL(10, 2)",
    }
    const connectorProps = {
      connector: "kafka",
      topic: "orders",
      format: "json",
      "properties.bootstrap.servers": "kafka:9092",
    }

    const sql = buildObservationSql(
      "orders",
      schema,
      connectorProps,
      "flink-reactor-tap-test-orders-abc12345",
      {
        name: "KafkaSource (orders)",
        groupIdPrefix: "",
        offsetMode: "latest",
        startTimestamp: "",
        endTimestamp: "",
      },
    )

    expect(sql).toMatchSnapshot()
  })
})

// ── 6.4: Kafka sink tap observation SQL ───────────────────────────

describe("Kafka sink tap observation SQL", () => {
  it("generates correct observation SQL with latest offset", () => {
    const schema = { order_id: "BIGINT", total: "DECIMAL(10, 2)" }
    const connectorProps = {
      connector: "kafka",
      topic: "large-orders",
      format: "json",
      "properties.bootstrap.servers": "kafka:9092",
    }

    const sql = buildObservationSql(
      "large_orders",
      schema,
      connectorProps,
      "flink-reactor-tap-test-large_orders-def56789",
      {
        name: "KafkaSink (large-orders)",
        groupIdPrefix: "",
        offsetMode: "latest",
        startTimestamp: "",
        endTimestamp: "",
      },
    )

    expect(sql).toMatchSnapshot()
  })
})

// ── 6.5: Kafka sink tap with timestamp offset ─────────────────────

describe("Kafka tap with timestamp offset mode", () => {
  it("generates correct startup timestamp config", () => {
    const schema = { order_id: "BIGINT" }
    const connectorProps = {
      connector: "kafka",
      topic: "orders",
      format: "json",
      "properties.bootstrap.servers": "kafka:9092",
    }

    const sql = buildObservationSql(
      "orders",
      schema,
      connectorProps,
      "flink-reactor-tap-test-orders-abc12345",
      {
        name: "KafkaSource (orders)",
        groupIdPrefix: "",
        offsetMode: "timestamp",
        startTimestamp: "2024-01-15T10:00:00Z",
        endTimestamp: "",
      },
    )

    expect(sql).toMatchSnapshot()
    expect(sql).toContain("'scan.startup.mode' = 'timestamp'")
    expect(sql).toContain("'scan.startup.timestamp-millis'")
  })
})

// ── 6.6: JDBC sink tap observation SQL ────────────────────────────

describe("JDBC sink tap observation SQL", () => {
  it("generates correct observation SQL with periodic poll", () => {
    const schema = { id: "BIGINT", name: "STRING" }
    const connectorProps = {
      connector: "jdbc",
      url: "jdbc:postgresql://localhost:5433/db",
      "table-name": "users",
    }

    const sql = buildObservationSql(
      "users",
      schema,
      connectorProps,
      "flink-reactor-tap-test-users-abc12345",
      {
        name: "JdbcSink (users)",
        groupIdPrefix: "",
        offsetMode: "latest",
        startTimestamp: "",
        endTimestamp: "",
      },
    )

    expect(sql).toMatchSnapshot()
  })
})

// ── 6.7: generateTapMetadata dev mode ─────────────────────────────

describe("generateTapMetadata - dev mode", () => {
  it("auto-taps all sinks but not untapped transforms", () => {
    const source = KafkaSource({
      topic: "orders",
      bootstrapServers: "kafka:9092",
      format: "json",
      schema: OrderSchema,
    })

    const filter = Filter({
      condition: "amount > 100",
      children: [source],
    })

    const sink = KafkaSink({
      topic: "large-orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      children: [filter],
    })

    const pipeline = Pipeline({ name: "ecommerce", children: [sink] })

    const { taps, diagnostics } = generateTapMetadata(
      pipeline,
      "ecommerce",
      "2.0",
      true,
    )

    // Should only tap the sink (auto-tap), not the source or filter
    expect(taps).toHaveLength(1)
    expect(taps[0].componentName).toBe("KafkaSink")
    expect(diagnostics).toHaveLength(0)
  })

  it("includes explicitly tapped transforms alongside auto-tapped sinks", () => {
    const source = KafkaSource({
      topic: "orders",
      bootstrapServers: "kafka:9092",
      format: "json",
      schema: OrderSchema,
    })

    const filter = Filter({
      condition: "amount > 100",
      tap: { name: "debug-filter" },
      children: [source],
    })

    const sink = KafkaSink({
      topic: "large-orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      children: [filter],
    })

    const pipeline = Pipeline({ name: "ecommerce", children: [sink] })

    const { taps } = generateTapMetadata(pipeline, "ecommerce", "2.0", true)

    // Sink (auto-tap) + Filter (explicit tap)
    expect(taps).toHaveLength(2)
    const componentNames = taps.map((t) => t.componentName)
    expect(componentNames).toContain("KafkaSink")
    expect(componentNames).toContain("Filter")
  })
})

// ── 6.8: generateTapMetadata prod mode ────────────────────────────

describe("generateTapMetadata - prod mode", () => {
  it("only taps explicitly marked operators", () => {
    const source = KafkaSource({
      topic: "orders",
      bootstrapServers: "kafka:9092",
      format: "json",
      schema: OrderSchema,
    })

    const filter = Filter({
      condition: "amount > 100",
      children: [source],
    })

    const sink = KafkaSink({
      topic: "large-orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      children: [filter],
    })

    const pipeline = Pipeline({ name: "ecommerce", children: [sink] })

    const { taps } = generateTapMetadata(pipeline, "ecommerce", "2.0", false)

    // No taps — nothing is explicitly marked
    expect(taps).toHaveLength(0)
  })

  it("taps only explicitly marked operators", () => {
    const source = KafkaSource({
      topic: "orders",
      bootstrapServers: "kafka:9092",
      format: "json",
      schema: OrderSchema,
      tap: true,
    })

    const filter = Filter({
      condition: "amount > 100",
      children: [source],
    })

    const sink = KafkaSink({
      topic: "large-orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      children: [filter],
    })

    const pipeline = Pipeline({ name: "ecommerce", children: [sink] })

    const { taps } = generateTapMetadata(pipeline, "ecommerce", "2.0", false)

    // Only the explicitly tapped source
    expect(taps).toHaveLength(1)
    expect(taps[0].componentName).toBe("KafkaSource")
  })
})

// ── 6.9: normalizeTapConfig ───────────────────────────────────────

describe("normalizeTapConfig", () => {
  it("converts boolean to full config with defaults", () => {
    const config = normalizeTapConfig(true, "KafkaSink", "orders")
    expect(config.offsetMode).toBe("latest")
    expect(config.name).toBe("KafkaSink (orders)")
    expect(config.groupIdPrefix).toBe("")
    expect(config.startTimestamp).toBe("")
    expect(config.endTimestamp).toBe("")
  })

  it("preserves user-provided values", () => {
    const config = normalizeTapConfig(
      { name: "my-tap", offsetMode: "earliest", groupIdPrefix: "debug" },
      "Filter",
      "Filter_0",
    )
    expect(config.name).toBe("my-tap")
    expect(config.offsetMode).toBe("earliest")
    expect(config.groupIdPrefix).toBe("debug")
  })

  it("generates auto name for auto-generated node IDs", () => {
    const config = normalizeTapConfig(true, "Filter", "Filter_0")
    expect(config.name).toBe("Filter Filter_0")
  })

  it("generates contextual name for meaningful node IDs", () => {
    const config = normalizeTapConfig(true, "KafkaSink", "large_orders")
    expect(config.name).toBe("KafkaSink (large_orders)")
  })
})

// ── 6.10: normalizeTapConfig validation ───────────────────────────

describe("validateTapConfig", () => {
  it("validates timestamp offset mode requires startTimestamp", () => {
    const config = normalizeTapConfig(
      { offsetMode: "timestamp" },
      "KafkaSource",
      "orders",
    )
    const diagnostics = validateTapConfig(config, "orders")
    expect(diagnostics).toHaveLength(1)
    expect(diagnostics[0].severity).toBe("error")
    expect(diagnostics[0].message).toContain("startTimestamp is required")
  })

  it("validates endTimestamp must be after startTimestamp", () => {
    const config = normalizeTapConfig(
      {
        offsetMode: "timestamp",
        startTimestamp: "2024-01-15T12:00:00Z",
        endTimestamp: "2024-01-15T10:00:00Z",
      },
      "KafkaSource",
      "orders",
    )
    const diagnostics = validateTapConfig(config, "orders")
    expect(diagnostics).toHaveLength(1)
    expect(diagnostics[0].message).toContain(
      "endTimestamp must be after startTimestamp",
    )
  })

  it("passes for valid timestamp config", () => {
    const config = normalizeTapConfig(
      {
        offsetMode: "timestamp",
        startTimestamp: "2024-01-15T10:00:00Z",
        endTimestamp: "2024-01-15T12:00:00Z",
      },
      "KafkaSource",
      "orders",
    )
    const diagnostics = validateTapConfig(config, "orders")
    expect(diagnostics).toHaveLength(0)
  })
})

// ── 6.11: Full pipeline TapManifest ───────────────────────────────

describe("Full pipeline TapManifest", () => {
  it("generates correct manifest JSON for mixed tapped/untapped operators", () => {
    const source = KafkaSource({
      topic: "orders",
      bootstrapServers: "kafka:9092",
      format: "json",
      schema: OrderSchema,
      tap: true,
    })

    const filter = Filter({
      condition: "amount > 100",
      children: [source],
    })

    const sink = KafkaSink({
      topic: "large-orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      tap: { name: "output-tap", offsetMode: "earliest" },
      children: [filter],
    })

    const pipeline = Pipeline({ name: "ecommerce", children: [sink] })

    const { manifest, diagnostics } = generateTapManifest(pipeline, {
      flinkVersion: "2.0",
    })

    expect(diagnostics).toHaveLength(0)
    expect(manifest).not.toBeNull()
    expect(manifest?.pipelineName).toBe("ecommerce")
    expect(manifest?.flinkVersion).toBe("2.0")
    expect(manifest?.taps).toHaveLength(2)

    // Verify the tapped source
    const sourceTap = manifest?.taps.find(
      (t) => t.componentName === "KafkaSource",
    )
    expect(sourceTap).toBeDefined()
    expect(sourceTap?.connectorType).toBe("kafka")

    // Verify the tapped sink
    const sinkTap = manifest?.taps.find((t) => t.componentName === "KafkaSink")
    expect(sinkTap).toBeDefined()
    expect(sinkTap?.name).toBe("output-tap")
    expect(sinkTap?.config.offsetMode).toBe("earliest")

    // Snapshot the manifest (minus generatedAt which is non-deterministic)
    const sanitized = {
      ...manifest!,
      generatedAt: "<redacted>",
    }
    expect(sanitized).toMatchSnapshot()
  })

  it("returns null manifest when noTap is set", () => {
    const source = KafkaSource({
      topic: "orders",
      bootstrapServers: "kafka:9092",
      format: "json",
      schema: OrderSchema,
      tap: true,
    })

    const sink = KafkaSink({
      topic: "large-orders",
      format: "json",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const { manifest } = generateTapManifest(pipeline, { noTap: true })
    expect(manifest).toBeNull()
  })

  it("returns null manifest when no operators are tapped in prod mode", () => {
    const source = KafkaSource({
      topic: "orders",
      bootstrapServers: "kafka:9092",
      format: "json",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "large-orders",
      format: "json",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const { manifest } = generateTapManifest(pipeline, { devMode: false })
    expect(manifest).toBeNull()
  })
})

// ── 6.12: FileSystem sink tap unsupported warning ─────────────────

describe("FileSystem sink tap unsupported", () => {
  it("emits unsupported warning diagnostic", () => {
    const source = KafkaSource({
      topic: "orders",
      bootstrapServers: "kafka:9092",
      format: "json",
      schema: OrderSchema,
    })

    const sink = FileSystemSink({
      path: "s3://bucket/output",
      format: "parquet",
      tap: true,
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const { taps, diagnostics } = generateTapMetadata(
      pipeline,
      "test",
      "2.0",
      false,
    )

    // No tap metadata generated for unsupported connector
    expect(taps).toHaveLength(0)

    // Warning diagnostic emitted
    expect(diagnostics).toHaveLength(1)
    expect(diagnostics[0].severity).toBe("warning")
    expect(diagnostics[0].message).toContain("FileSystem")
  })
})

// ── Tapped operators produce identical SQL ────────────────────────

describe("Tapped operators produce identical SQL", () => {
  it("tap prop does not affect generated SQL output", () => {
    // Pipeline WITHOUT taps
    resetNodeIdCounter()
    const source1 = KafkaSource({
      topic: "orders",
      bootstrapServers: "kafka:9092",
      format: "json",
      schema: OrderSchema,
    })
    const filter1 = Filter({
      condition: "amount > 100",
      children: [source1],
    })
    const sink1 = KafkaSink({
      topic: "large-orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      children: [filter1],
    })
    const pipeline1 = Pipeline({ name: "ecommerce", children: [sink1] })
    const result1 = generateSql(pipeline1)

    // Pipeline WITH taps on all operators
    resetNodeIdCounter()
    const source2 = KafkaSource({
      topic: "orders",
      bootstrapServers: "kafka:9092",
      format: "json",
      schema: OrderSchema,
      tap: true,
    })
    const filter2 = Filter({
      condition: "amount > 100",
      tap: { name: "debug-filter" },
      children: [source2],
    })
    const sink2 = KafkaSink({
      topic: "large-orders",
      format: "json",
      bootstrapServers: "kafka:9092",
      tap: true,
      children: [filter2],
    })
    const pipeline2 = Pipeline({ name: "ecommerce", children: [sink2] })
    const result2 = generateSql(pipeline2)

    // SQL output should be byte-for-byte identical
    expect(result1.sql).toBe(result2.sql)
  })
})

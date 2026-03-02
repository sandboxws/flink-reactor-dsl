import { beforeEach, describe, expect, it } from "vitest"
import { Pipeline } from "@/components/pipeline.js"
import { FileSystemSink, JdbcSink, KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { SynthContext } from "@/core/synth-context.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const OrderSchema = Schema({
  fields: {
    orderId: Field.BIGINT(),
    userId: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    createdAt: Field.TIMESTAMP(3),
  },
})

describe("Pipeline component", () => {
  it("creates a Pipeline node with name and default streaming mode", () => {
    const node = Pipeline({ name: "my-pipeline" })

    expect(node.kind).toBe("Pipeline")
    expect(node.component).toBe("Pipeline")
    expect(node.props.name).toBe("my-pipeline")
    // mode is not set explicitly, so it's undefined in props (default applied at codegen)
  })

  it("stores mode, parallelism, checkpoint, stateBackend, stateTtl, restartStrategy", () => {
    const node = Pipeline({
      name: "full-config",
      mode: "batch",
      parallelism: 8,
      checkpoint: { interval: "30s", mode: "exactly-once" },
      stateBackend: "rocksdb",
      stateTtl: "1h",
      restartStrategy: { type: "fixed-delay", attempts: 3, delay: "10s" },
    })

    expect(node.props.mode).toBe("batch")
    expect(node.props.parallelism).toBe(8)
    expect(node.props.checkpoint).toEqual({
      interval: "30s",
      mode: "exactly-once",
    })
    expect(node.props.stateBackend).toBe("rocksdb")
    expect(node.props.stateTtl).toBe("1h")
    expect(node.props.restartStrategy).toEqual({
      type: "fixed-delay",
      attempts: 3,
      delay: "10s",
    })
  })

  it("stores raw flinkConfig key-value pairs", () => {
    const node = Pipeline({
      name: "custom-config",
      flinkConfig: {
        "table.exec.mini-batch.enabled": "true",
        "table.exec.mini-batch.allow-latency": "5s",
      },
    })

    expect(node.props.flinkConfig).toEqual({
      "table.exec.mini-batch.enabled": "true",
      "table.exec.mini-batch.allow-latency": "5s",
    })
  })

  it("throws on invalid pipeline mode", () => {
    expect(() =>
      Pipeline({ name: "bad", mode: "realtime" as "streaming" }),
    ).toThrow("Invalid pipeline mode 'realtime'")
  })

  it("throws on invalid checkpoint mode", () => {
    expect(() =>
      Pipeline({
        name: "bad",
        checkpoint: { interval: "10s", mode: "maybe" as "exactly-once" },
      }),
    ).toThrow("Invalid checkpoint mode 'maybe'")
  })

  it("throws when checkpoint has no interval", () => {
    expect(() =>
      Pipeline({
        name: "bad",
        checkpoint: { interval: "" },
      }),
    ).toThrow("Checkpoint config requires an interval")
  })

  it("wraps sources and sinks as children", () => {
    const source = KafkaSource({ topic: "in", schema: OrderSchema })
    const sink = KafkaSink({ topic: "out" })
    const pipeline = Pipeline({
      name: "wrap-test",
      children: [source, sink],
    })

    expect(pipeline.children).toHaveLength(2)
    expect(pipeline.children[0].component).toBe("KafkaSource")
    expect(pipeline.children[1].component).toBe("KafkaSink")
  })

  it("builds a valid construct tree that SynthContext can walk", () => {
    const sink = KafkaSink({ topic: "out" })
    const source = KafkaSource({
      topic: "in",
      schema: OrderSchema,
      children: sink,
    })
    const pipeline = Pipeline({ name: "dag-test", children: source })

    const ctx = new SynthContext()
    ctx.buildFromTree(pipeline)

    expect(ctx.getAllNodes()).toHaveLength(3)
    expect(ctx.getAllEdges()).toHaveLength(2)
    expect(ctx.validate()).toHaveLength(0)
  })
})

describe("KafkaSource → KafkaSink linear chain", () => {
  it("creates a valid source → sink construct tree", () => {
    const sink = KafkaSink({ topic: "output" })
    const source = KafkaSource({
      topic: "input",
      schema: OrderSchema,
      children: sink,
    })

    expect(source.kind).toBe("Source")
    expect(source.children).toHaveLength(1)
    expect(source.children[0].kind).toBe("Sink")
    expect(source.children[0].props.topic).toBe("output")

    const ctx = new SynthContext()
    ctx.buildFromTree(source)
    const sorted = ctx.topologicalSort()
    expect(sorted.map((n) => n.component)).toEqual(["KafkaSource", "KafkaSink"])
  })
})

describe("fan-out: one source, two sinks", () => {
  it("constructs a fan-out DAG with proper edges", () => {
    const sinkA = KafkaSink({ topic: "output-a" })
    const sinkB = KafkaSink({ topic: "output-b" })
    const source = KafkaSource({
      topic: "input",
      schema: OrderSchema,
      children: [sinkA, sinkB],
    })

    expect(source.children).toHaveLength(2)

    const ctx = new SynthContext()
    ctx.buildFromTree(source)

    expect(ctx.getAllNodes()).toHaveLength(3)
    expect(ctx.getAllEdges()).toHaveLength(2)

    const sourceNode = ctx.getNodesByKind("Source")
    expect(sourceNode).toHaveLength(1)
    expect(ctx.getOutgoing(sourceNode[0].id).size).toBe(2)

    expect(ctx.validate()).toHaveLength(0)
  })
})

describe("FileSystemSink with partitioning", () => {
  it("stores format, partitionBy, and rollingPolicy on construct node", () => {
    const node = FileSystemSink({
      path: "s3://bucket/output/",
      format: "parquet",
      partitionBy: ["DATE(event_time)"],
      rollingPolicy: { size: "128MB", interval: "15min" },
    })

    expect(node.kind).toBe("Sink")
    expect(node.component).toBe("FileSystemSink")
    expect(node.props.path).toBe("s3://bucket/output/")
    expect(node.props.format).toBe("parquet")
    expect(node.props.partitionBy).toEqual(["DATE(event_time)"])
    expect(node.props.rollingPolicy).toEqual({
      size: "128MB",
      interval: "15min",
    })
  })
})

describe("JdbcSink upsert mode", () => {
  it("stores upsert config with key fields on construct node", () => {
    const node = JdbcSink({
      url: "jdbc:postgresql://localhost:5432/db",
      table: "user_stats",
      upsertMode: true,
      keyFields: ["user_id"],
    })

    expect(node.kind).toBe("Sink")
    expect(node.component).toBe("JdbcSink")
    expect(node.props.url).toBe("jdbc:postgresql://localhost:5432/db")
    expect(node.props.table).toBe("user_stats")
    expect(node.props.upsertMode).toBe(true)
    expect(node.props.keyFields).toEqual(["user_id"])
  })
})

describe("per-operator parallelism", () => {
  it("stores parallelism on source construct node", () => {
    const node = KafkaSource({
      topic: "events",
      schema: OrderSchema,
      parallelism: 8,
    })

    expect(node.props.parallelism).toBe(8)
  })

  it("stores parallelism on sink construct node", () => {
    const node = KafkaSink({
      topic: "output",
      parallelism: 4,
    })

    expect(node.props.parallelism).toBe(4)
  })

  it("per-operator parallelism overrides pipeline-level in the tree", () => {
    const sink = KafkaSink({ topic: "out", parallelism: 2 })
    const source = KafkaSource({
      topic: "in",
      schema: OrderSchema,
      parallelism: 8,
      children: sink,
    })
    const pipeline = Pipeline({
      name: "parallel-test",
      parallelism: 4,
      children: source,
    })

    // Pipeline has parallelism=4, source has 8, sink has 2
    expect(pipeline.props.parallelism).toBe(4)
    expect(pipeline.children[0].props.parallelism).toBe(8)
    expect(pipeline.children[0].children[0].props.parallelism).toBe(2)
  })
})

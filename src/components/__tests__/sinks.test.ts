import { beforeEach, describe, expect, it } from "vitest"
import {
  FileSystemSink,
  GenericSink,
  JdbcSink,
  KafkaSink,
} from "@/components/sinks.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"

beforeEach(() => {
  resetNodeIdCounter()
})

describe("KafkaSink", () => {
  it("creates a Sink node with topic", () => {
    const node = KafkaSink({ topic: "output-events" })

    expect(node.kind).toBe("Sink")
    expect(node.component).toBe("KafkaSink")
    expect(node.props.topic).toBe("output-events")
  })

  it("stores format and bootstrapServers", () => {
    const node = KafkaSink({
      topic: "output",
      format: "avro",
      bootstrapServers: "kafka:9092",
    })

    expect(node.props.format).toBe("avro")
    expect(node.props.bootstrapServers).toBe("kafka:9092")
  })
})

describe("JdbcSink", () => {
  it("creates a Sink node with url and table", () => {
    const node = JdbcSink({
      url: "jdbc:postgresql://localhost:5433/db",
      table: "results",
    })

    expect(node.kind).toBe("Sink")
    expect(node.component).toBe("JdbcSink")
    expect(node.props.url).toBe("jdbc:postgresql://localhost:5433/db")
    expect(node.props.table).toBe("results")
  })

  it("defaults upsertMode to undefined (false at codegen)", () => {
    const node = JdbcSink({
      url: "jdbc:postgresql://localhost:5433/db",
      table: "results",
    })

    expect(node.props.upsertMode).toBeUndefined()
  })

  it("stores upsert mode with key fields", () => {
    const node = JdbcSink({
      url: "jdbc:postgresql://localhost:5433/db",
      table: "user_stats",
      upsertMode: true,
      keyFields: ["user_id"],
    })

    expect(node.props.upsertMode).toBe(true)
    expect(node.props.keyFields).toEqual(["user_id"])
  })

  it("stores multiple key fields for composite primary keys", () => {
    const node = JdbcSink({
      url: "jdbc:postgresql://localhost:5433/db",
      table: "order_items",
      upsertMode: true,
      keyFields: ["order_id", "item_id"],
    })

    expect(node.props.keyFields).toEqual(["order_id", "item_id"])
  })
})

describe("FileSystemSink", () => {
  it("creates a Sink node with path", () => {
    const node = FileSystemSink({ path: "s3://bucket/output/" })

    expect(node.kind).toBe("Sink")
    expect(node.component).toBe("FileSystemSink")
    expect(node.props.path).toBe("s3://bucket/output/")
  })

  it("stores parquet format with partition config", () => {
    const node = FileSystemSink({
      path: "s3://bucket/output/",
      format: "parquet",
      partitionBy: ["DATE(event_time)"],
    })

    expect(node.props.format).toBe("parquet")
    expect(node.props.partitionBy).toEqual(["DATE(event_time)"])
  })

  it("stores rolling policy", () => {
    const node = FileSystemSink({
      path: "/data/output/",
      format: "json",
      rollingPolicy: { size: "256MB", interval: "1h" },
    })

    expect(node.props.rollingPolicy).toEqual({ size: "256MB", interval: "1h" })
  })
})

describe("GenericSink", () => {
  it("creates a Sink node with connector and options", () => {
    const node = GenericSink({
      connector: "elasticsearch-7",
      options: {
        hosts: "http://localhost:9200",
        index: "events",
      },
    })

    expect(node.kind).toBe("Sink")
    expect(node.component).toBe("GenericSink")
    expect(node.props.connector).toBe("elasticsearch-7")
    expect(node.props.options).toEqual({
      hosts: "http://localhost:9200",
      index: "events",
    })
  })
})

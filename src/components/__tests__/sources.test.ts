import { beforeEach, describe, expect, it } from "vitest"
import { resetNodeIdCounter } from "../../core/jsx-runtime.js"
import { Field, Schema } from "../../core/schema.js"
import { GenericSource, JdbcSource, KafkaSource } from "../sources.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const EventSchema = Schema({
  fields: {
    id: Field.BIGINT(),
    userId: Field.STRING(),
    eventType: Field.STRING(),
    eventTime: Field.TIMESTAMP(3),
  },
})

describe("KafkaSource", () => {
  it("creates a Source node with topic and schema", () => {
    const node = KafkaSource({ topic: "events", schema: EventSchema })

    expect(node.kind).toBe("Source")
    expect(node.component).toBe("KafkaSource")
    expect(node.props.topic).toBe("events")
    expect(node.props.schema).toBe(EventSchema)
  })

  it("stores all optional Kafka props", () => {
    const node = KafkaSource({
      topic: "events",
      schema: EventSchema,
      bootstrapServers: "kafka:9092",
      format: "avro",
      startupMode: "earliest-offset",
      consumerGroup: "my-group",
    })

    expect(node.props.bootstrapServers).toBe("kafka:9092")
    expect(node.props.format).toBe("avro")
    expect(node.props.startupMode).toBe("earliest-offset")
    expect(node.props.consumerGroup).toBe("my-group")
  })

  it("stores watermark metadata on the construct node", () => {
    const node = KafkaSource({
      topic: "events",
      schema: EventSchema,
      watermark: {
        column: "eventTime",
        expression: "eventTime - INTERVAL '5' SECOND",
      },
    })

    expect(node.props.watermark).toEqual({
      column: "eventTime",
      expression: "eventTime - INTERVAL '5' SECOND",
    })
  })

  it("supports debezium-json format for CDC", () => {
    const node = KafkaSource({
      topic: "db.inventory.orders",
      schema: EventSchema,
      format: "debezium-json",
    })

    expect(node.props.format).toBe("debezium-json")
  })
})

describe("JdbcSource", () => {
  it("creates a Source node with url, table, and schema", () => {
    const node = JdbcSource({
      url: "jdbc:postgresql://localhost:5432/mydb",
      table: "users",
      schema: EventSchema,
    })

    expect(node.kind).toBe("Source")
    expect(node.component).toBe("JdbcSource")
    expect(node.props.url).toBe("jdbc:postgresql://localhost:5432/mydb")
    expect(node.props.table).toBe("users")
  })

  it("stores lookup cache config for dimension table use", () => {
    const node = JdbcSource({
      url: "jdbc:mysql://localhost:3306/mydb",
      table: "dim_users",
      schema: EventSchema,
      lookupCache: { maxRows: 10000, ttl: "1h" },
    })

    expect(node.props.lookupCache).toEqual({ maxRows: 10000, ttl: "1h" })
  })
})

describe("GenericSource", () => {
  it("creates a Source node with connector, format, and options", () => {
    const node = GenericSource({
      connector: "filesystem",
      format: "csv",
      schema: EventSchema,
      options: { path: "/data/input/", "csv.field-delimiter": "|" },
    })

    expect(node.kind).toBe("Source")
    expect(node.component).toBe("GenericSource")
    expect(node.props.connector).toBe("filesystem")
    expect(node.props.format).toBe("csv")
    expect(node.props.options).toEqual({
      path: "/data/input/",
      "csv.field-delimiter": "|",
    })
  })

  it("stores the schema for DDL generation", () => {
    const node = GenericSource({
      connector: "datagen",
      schema: EventSchema,
    })

    expect(node.props.schema).toBe(EventSchema)
  })
})

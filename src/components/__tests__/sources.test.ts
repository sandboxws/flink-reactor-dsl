import { beforeEach, describe, expect, it } from "vitest"
import {
  GenericSource,
  JdbcSource,
  KafkaSource,
  PostgresCdcPipelineSource,
} from "@/components/sources.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { secretRef } from "@/core/secret-ref.js"

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

describe("PostgresCdcPipelineSource", () => {
  it("creates a Source node with hostname/database/tableList", () => {
    const node = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })

    expect(node.kind).toBe("Source")
    expect(node.component).toBe("PostgresCdcPipelineSource")
    expect(node.props.hostname).toBe("pg-primary")
    expect(node.props.database).toBe("shop")
    expect(node.props.tableList).toEqual(["public.orders"])
  })

  it("sets ChangelogMode to 'retract' unconditionally", () => {
    const node = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })

    expect(node.props.changelogMode).toBe("retract")
  })

  it("stores the SecretRef password unmodified on the node", () => {
    const ref = secretRef("pg-primary-password")
    const node = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: ref,
      schemaList: ["public"],
      tableList: ["public.orders"],
    })

    expect(node.props.password).toBe(ref)
  })

  it("throws when tableList is missing", () => {
    expect(() =>
      // biome-ignore lint/suspicious/noExplicitAny: deliberately missing prop
      PostgresCdcPipelineSource({
        hostname: "pg-primary",
        database: "shop",
        username: "postgres",
        password: secretRef("pg-primary-password"),
        schemaList: ["public"],
      } as any),
    ).toThrow(/PostgresCdcPipelineSource.*tableList/)
  })

  it("throws when password is missing", () => {
    expect(() =>
      // biome-ignore lint/suspicious/noExplicitAny: deliberately missing prop
      PostgresCdcPipelineSource({
        hostname: "pg-primary",
        database: "shop",
        username: "postgres",
        schemaList: ["public"],
        tableList: ["public.orders"],
      } as any),
    ).toThrow(/PostgresCdcPipelineSource.*password/)
  })

  it("preserves optional snapshot/startup/chunk/parallelism props", () => {
    const node = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
      snapshotMode: "initial_only",
      startupMode: "latest-offset",
      chunkSize: 100000,
      parallelism: 8,
      replicationSlotName: "fr_shop_orders",
      publicationName: "fr_shop_orders_pub",
      slotDropOnStop: true,
      heartbeatIntervalMs: 30000,
      decodingPluginName: "wal2json",
    })

    expect(node.props.snapshotMode).toBe("initial_only")
    expect(node.props.startupMode).toBe("latest-offset")
    expect(node.props.chunkSize).toBe(100000)
    expect(node.props.parallelism).toBe(8)
    expect(node.props.replicationSlotName).toBe("fr_shop_orders")
    expect(node.props.publicationName).toBe("fr_shop_orders_pub")
    expect(node.props.slotDropOnStop).toBe(true)
    expect(node.props.heartbeatIntervalMs).toBe(30000)
    expect(node.props.decodingPluginName).toBe("wal2json")
  })
})

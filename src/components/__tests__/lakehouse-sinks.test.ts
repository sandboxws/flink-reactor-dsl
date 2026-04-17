import { beforeEach, describe, expect, it } from "vitest"
import { IcebergCatalog, PaimonCatalog } from "@/components/catalogs.js"
import { IcebergSink, PaimonSink } from "@/components/sinks.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"

beforeEach(() => {
  resetNodeIdCounter()
})

describe("PaimonSink", () => {
  it("creates a Sink node referencing a Paimon catalog", () => {
    const { handle } = PaimonCatalog({
      name: "lake",
      warehouse: "s3://bucket/paimon",
    })

    const node = PaimonSink({
      catalog: handle,
      database: "analytics",
      table: "orders",
      primaryKey: ["order_id"],
    })

    expect(node.kind).toBe("Sink")
    expect(node.component).toBe("PaimonSink")
    expect(node.props.catalogName).toBe("lake")
    expect(node.props.catalogNodeId).toBe(handle.nodeId)
    expect(node.props.database).toBe("analytics")
    expect(node.props.table).toBe("orders")
    expect(node.props.primaryKey).toEqual(["order_id"])
  })

  it("stores merge engine and changelog producer config", () => {
    const { handle } = PaimonCatalog({
      name: "lake",
      warehouse: "s3://bucket/paimon",
    })

    const node = PaimonSink({
      catalog: handle,
      database: "analytics",
      table: "orders",
      primaryKey: ["order_id"],
      mergeEngine: "deduplicate",
      changelogProducer: "input",
    })

    expect(node.props.mergeEngine).toBe("deduplicate")
    expect(node.props.changelogProducer).toBe("input")
  })

  it("stores sequence field for ordering", () => {
    const { handle } = PaimonCatalog({
      name: "lake",
      warehouse: "s3://bucket/paimon",
    })

    const node = PaimonSink({
      catalog: handle,
      database: "analytics",
      table: "orders",
      primaryKey: ["order_id"],
      mergeEngine: "deduplicate",
      sequenceField: "updated_at",
    })

    expect(node.props.sequenceField).toBe("updated_at")
  })

  it("supports partial-update merge engine", () => {
    const { handle } = PaimonCatalog({
      name: "lake",
      warehouse: "s3://bucket/paimon",
    })

    const node = PaimonSink({
      catalog: handle,
      database: "analytics",
      table: "user_profiles",
      primaryKey: ["user_id"],
      mergeEngine: "partial-update",
      changelogProducer: "lookup",
    })

    expect(node.props.mergeEngine).toBe("partial-update")
    expect(node.props.changelogProducer).toBe("lookup")
  })
})

describe("IcebergSink", () => {
  it("creates a Sink node referencing an Iceberg catalog", () => {
    const { handle } = IcebergCatalog({
      name: "iceberg_cat",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })

    const node = IcebergSink({
      catalog: handle,
      database: "lakehouse",
      table: "events",
    })

    expect(node.kind).toBe("Sink")
    expect(node.component).toBe("IcebergSink")
    expect(node.props.catalogName).toBe("iceberg_cat")
    expect(node.props.catalogNodeId).toBe(handle.nodeId)
    expect(node.props.database).toBe("lakehouse")
    expect(node.props.table).toBe("events")
  })

  it("stores upsert config with format version 2", () => {
    const { handle } = IcebergCatalog({
      name: "iceberg_cat",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })

    const node = IcebergSink({
      catalog: handle,
      database: "lakehouse",
      table: "orders",
      primaryKey: ["order_id"],
      formatVersion: 2,
      upsertEnabled: true,
    })

    expect(node.props.formatVersion).toBe(2)
    expect(node.props.upsertEnabled).toBe(true)
    expect(node.props.primaryKey).toEqual(["order_id"])
  })

  it("stores MoR configuration props", () => {
    const { handle } = IcebergCatalog({
      name: "iceberg_cat",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })

    const node = IcebergSink({
      catalog: handle,
      database: "lakehouse",
      table: "orders",
      primaryKey: ["order_id"],
      formatVersion: 2,
      upsertEnabled: true,
      equalityFieldColumns: ["order_id"],
      commitIntervalSeconds: 5,
      writeDistributionMode: "hash",
      targetFileSizeMB: 128,
      writeParquetCompression: "zstd",
    })

    expect(node.props.equalityFieldColumns).toEqual(["order_id"])
    expect(node.props.commitIntervalSeconds).toBe(5)
    expect(node.props.writeDistributionMode).toBe("hash")
    expect(node.props.targetFileSizeMB).toBe(128)
    expect(node.props.writeParquetCompression).toBe("zstd")
  })

  it("defaults formatVersion and upsertEnabled to undefined", () => {
    const { handle } = IcebergCatalog({
      name: "iceberg_cat",
      catalogType: "hive",
      uri: "thrift://metastore:9083",
    })

    const node = IcebergSink({
      catalog: handle,
      database: "lakehouse",
      table: "events",
    })

    expect(node.props.formatVersion).toBeUndefined()
    expect(node.props.upsertEnabled).toBeUndefined()
  })
})

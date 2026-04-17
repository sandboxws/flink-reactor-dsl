import { IcebergCatalog } from "@/components/catalogs.js"
import { Pipeline } from "@/components/pipeline.js"
import { IcebergSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { createElement } from "@/core/jsx-runtime.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"
import { OrdersSchema } from "./schemas/orders.js"

export type F1WireFormat = "json" | "avro" | "protobuf"
export type F1CommitMode = "throughput" | "latency"

export interface F1Params {
  readonly wireFormat: F1WireFormat
  readonly commitMode: F1CommitMode
  readonly parallelism?: number
  readonly flinkVersion?: FlinkMajorVersion
  readonly bootstrapServers?: string
  readonly schemaRegistryUrl?: string
  readonly catalogUri?: string
  readonly topic?: string
}

const WIRE_FORMAT_MAP: Record<
  F1WireFormat,
  "debezium-json" | "debezium-avro" | "debezium-protobuf"
> = {
  json: "debezium-json",
  avro: "debezium-avro",
  protobuf: "debezium-protobuf",
}

const COMMIT_INTERVAL_SECONDS: Record<F1CommitMode, number> = {
  throughput: 10,
  latency: 2,
}

export default function f1Pipeline(params: F1Params): ConstructNode {
  const {
    wireFormat,
    commitMode,
    parallelism = 4,
    bootstrapServers = "kafka:9092",
    schemaRegistryUrl,
    catalogUri = "http://lakekeeper:8181/catalog",
    topic = "tpch.public.orders",
  } = params

  const format = WIRE_FORMAT_MAP[wireFormat]
  const needsRegistry = wireFormat === "avro" || wireFormat === "protobuf"
  const registryUrl = needsRegistry
    ? (schemaRegistryUrl ?? "http://schema-registry:8081")
    : undefined

  const catalog = IcebergCatalog({
    name: "lakekeeper",
    catalogType: "rest",
    uri: catalogUri,
  })

  const source = KafkaSource({
    topic,
    bootstrapServers,
    format,
    schema: OrdersSchema,
    primaryKey: ["o_orderkey"],
    startupMode: "earliest-offset",
    ...(registryUrl ? { schemaRegistryUrl: registryUrl } : {}),
  })

  const sink = IcebergSink({
    catalog: catalog.handle,
    database: "tpch",
    table: "orders",
    primaryKey: ["o_orderkey"],
    formatVersion: 2,
    upsertEnabled: true,
    equalityFieldColumns: ["o_orderkey"],
    commitIntervalSeconds: COMMIT_INTERVAL_SECONDS[commitMode],
    writeDistributionMode: "hash",
    targetFileSizeMB: 256,
    writeParquetCompression: "zstd",
    children: [source],
  })

  return (
    <Pipeline
      name="pg-cdc-iceberg-f1"
      parallelism={parallelism}
      checkpoint={{ interval: "60s", mode: "exactly-once" }}
    >
      {catalog.node}
      {sink}
    </Pipeline>
  )
}

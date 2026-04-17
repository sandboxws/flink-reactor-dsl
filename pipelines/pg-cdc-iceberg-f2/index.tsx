import { IcebergCatalog } from "@/components/catalogs.js"
import { Pipeline } from "@/components/pipeline.js"
import { IcebergSink } from "@/components/sinks.js"
import type { PostgresCdcSnapshotMode } from "@/components/sources.js"
import { PostgresCdcPipelineSource } from "@/components/sources.js"
import { createElement } from "@/core/jsx-runtime.js"
import { secretRef } from "@/core/secret-ref.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"

export type F2CommitMode = "throughput" | "latency"

export interface F2Params {
  readonly snapshotMode: PostgresCdcSnapshotMode
  readonly commitMode: F2CommitMode
  readonly parallelism?: number
  readonly flinkVersion?: FlinkMajorVersion
  readonly hostname?: string
  readonly database?: string
  readonly username?: string
  readonly passwordSecretName?: string
  readonly schemaList?: readonly string[]
  readonly tableList?: readonly string[]
  readonly catalogUri?: string
  readonly chunkSize?: number
}

const COMMIT_INTERVAL_SECONDS: Record<F2CommitMode, number> = {
  throughput: 10,
  latency: 2,
}

export default function f2Pipeline(params: F2Params): ConstructNode {
  const {
    snapshotMode,
    commitMode,
    parallelism = 4,
    hostname = "pg-primary",
    database = "tpch",
    username = "flink_cdc",
    passwordSecretName = "pg-primary-password",
    schemaList = ["public"],
    tableList = ["public.orders", "public.lineitem", "public.customer"],
    catalogUri = "http://lakekeeper:8181/catalog",
    chunkSize = 100_000,
  } = params

  const catalog = IcebergCatalog({
    name: "lakekeeper",
    catalogType: "rest",
    uri: catalogUri,
  })

  const source = PostgresCdcPipelineSource({
    hostname,
    port: 5432,
    database,
    username,
    password: secretRef(passwordSecretName),
    schemaList,
    tableList,
    snapshotMode,
    chunkSize,
    slotDropOnStop: snapshotMode === "initial_only",
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
      name="pg-cdc-iceberg-f2"
      mode={snapshotMode === "initial_only" ? "batch" : "streaming"}
      parallelism={parallelism}
      checkpoint={{ interval: "60s", mode: "exactly-once" }}
    >
      {catalog.node}
      {sink}
    </Pipeline>
  )
}

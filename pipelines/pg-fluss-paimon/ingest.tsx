import { FlussCatalog } from "@/components/catalogs.js"
import { Pipeline } from "@/components/pipeline.js"
import { FlussSink } from "@/components/sinks.js"
import {
  PostgresCdcPipelineSource,
  type PostgresCdcSnapshotMode,
} from "@/components/sources.js"
import { createElement } from "@/core/jsx-runtime.js"
import { secretRef } from "@/core/secret-ref.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"

export type IngestSnapshotMode = Extract<
  PostgresCdcSnapshotMode,
  "initial" | "never"
>
export type IngestCommitMode = "throughput" | "latency"

export interface IngestParams {
  readonly snapshotMode: IngestSnapshotMode
  readonly commitMode: IngestCommitMode
  readonly parallelism?: number
  readonly flinkVersion?: FlinkMajorVersion
  readonly hostname?: string
  readonly database?: string
  readonly username?: string
  readonly passwordSecretName?: string
  readonly schemaList?: readonly string[]
  readonly tableList?: readonly string[]
  readonly bootstrapServers?: string
  readonly chunkSize?: number
}

const COMMIT_INTERVAL_SECONDS: Record<IngestCommitMode, number> = {
  throughput: 10,
  latency: 2,
}

export default function ingestPipeline(params: IngestParams): ConstructNode {
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
    bootstrapServers = "fluss-coordinator:9123",
    chunkSize = 100_000,
  } = params

  const catalog = FlussCatalog({
    name: "fluss_catalog",
    bootstrapServers,
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
  })

  const sink = FlussSink({
    catalog: catalog.handle,
    database: "benchmark",
    table: "orders",
    primaryKey: ["o_orderkey"],
    buckets: 8,
    children: [source],
  })

  return (
    <Pipeline
      name="pg-fluss-paimon-ingest"
      parallelism={parallelism}
      checkpoint={{
        interval: `${COMMIT_INTERVAL_SECONDS[commitMode]}s`,
        mode: "exactly-once",
      }}
    >
      {catalog.node}
      {sink}
    </Pipeline>
  )
}

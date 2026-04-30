import { FlussCatalog, PaimonCatalog } from "@/components/catalogs.js"
import { Pipeline } from "@/components/pipeline.js"
import { type PaimonMergeEngine, PaimonSink } from "@/components/sinks.js"
import { FlussSource } from "@/components/sources.js"
import { Filter } from "@/components/transforms.js"
import { createElement } from "@/core/jsx-runtime.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"
import { OrdersSchema } from "./schemas/orders.js"

export type ServeMergeEngine = Extract<
  PaimonMergeEngine,
  "deduplicate" | "partial-update"
>
export type ServeCommitMode = "throughput" | "latency"

export interface ServeParams {
  readonly paimonMergeEngine: ServeMergeEngine
  readonly commitMode: ServeCommitMode
  readonly parallelism?: number
  readonly flinkVersion?: FlinkMajorVersion
  readonly bootstrapServers?: string
  readonly warehouse?: string
}

const COMMIT_INTERVAL_SECONDS: Record<ServeCommitMode, number> = {
  throughput: 10,
  latency: 2,
}

export default function servePipeline(params: ServeParams): ConstructNode {
  const {
    paimonMergeEngine,
    commitMode,
    parallelism = 4,
    bootstrapServers = "fluss-coordinator:9123",
    warehouse = "s3a://benchmark/paimon",
  } = params

  const fluss = FlussCatalog({
    name: "fluss_catalog",
    bootstrapServers,
  })

  const paimon = PaimonCatalog({
    name: "paimon_catalog",
    warehouse,
  })

  const source = FlussSource({
    catalog: fluss.handle,
    database: "benchmark",
    table: "orders",
    schema: OrdersSchema,
    primaryKey: ["o_orderkey"],
    scanStartupMode: "initial",
  })

  const openOrders = Filter({
    condition: "o_orderstatus = 'O'",
    children: [source],
  })

  const sink = PaimonSink({
    catalog: paimon.handle,
    database: "benchmark",
    table: "orders",
    primaryKey: ["o_orderkey"],
    mergeEngine: paimonMergeEngine,
    changelogProducer: "input",
    bucket: 8,
    children: [openOrders],
  })

  return (
    <Pipeline
      name="pg-fluss-paimon-serve"
      parallelism={parallelism}
      checkpoint={{
        interval: `${COMMIT_INTERVAL_SECONDS[commitMode]}s`,
        mode: "exactly-once",
      }}
    >
      {fluss.node}
      {paimon.node}
      {sink}
    </Pipeline>
  )
}

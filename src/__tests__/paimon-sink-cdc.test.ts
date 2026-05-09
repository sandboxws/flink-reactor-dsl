import { beforeEach, describe, expect, it } from "vitest"
import { generateSql } from "@/codegen/sql/sql-generator.js"
import { FlussCatalog, PaimonCatalog } from "@/components/catalogs.js"
import { Pipeline } from "@/components/pipeline.js"
import { PaimonSink } from "@/components/sinks.js"
import { FlussSource } from "@/components/sources.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    customer_region: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    order_ts: Field.TIMESTAMP(3),
  },
})

beforeEach(() => {
  resetNodeIdCounter()
})

function makePipeline(sinkProps: Record<string, unknown>) {
  const fluss = FlussCatalog({
    name: "fluss_cat",
    bootstrapServers: "fluss-coordinator:9123",
  })
  const paimon = PaimonCatalog({
    name: "lake",
    warehouse: "s3://bucket/lake",
  })
  const source = FlussSource({
    catalog: fluss.handle,
    database: "shop",
    table: "orders",
    schema: OrderSchema,
    primaryKey: ["order_id"],
  })
  const sink = PaimonSink({
    catalog: paimon.handle,
    database: "warehouse",
    table: "orders_lake",
    primaryKey: ["order_id"],
    children: [source],
    ...sinkProps,
  })
  return Pipeline({
    name: "pg-fluss-paimon",
    children: [fluss.node, paimon.node, sink],
  })
}

describe("PaimonSink CDC tuning props (sql-generator)", () => {
  it("renders bucket option", () => {
    const { sql } = generateSql(
      makePipeline({ mergeEngine: "deduplicate", bucket: 8 }),
    )
    expect(sql).toContain("'bucket' = '8'")
    expect(sql).toMatchSnapshot()
  })

  it("falls back bucket-key to primaryKey when bucket is set and bucketKey is unset", () => {
    const { sql } = generateSql(
      makePipeline({ mergeEngine: "deduplicate", bucket: 8 }),
    )
    expect(sql).toContain("'bucket-key' = 'order_id'")
  })

  it("uses explicit bucketKey when provided, even when primaryKey is set", () => {
    const { sql } = generateSql(
      makePipeline({
        mergeEngine: "deduplicate",
        bucket: 4,
        bucketKey: ["customer_region"],
      }),
    )
    expect(sql).toContain("'bucket-key' = 'customer_region'")
    expect(sql).not.toMatch(/'bucket-key' = 'order_id'/)
  })

  it("renders fullCompactionDeltaCommits", () => {
    const { sql } = generateSql(
      makePipeline({
        mergeEngine: "deduplicate",
        fullCompactionDeltaCommits: 10,
      }),
    )
    expect(sql).toContain("'full-compaction.delta-commits' = '10'")
    expect(sql).toMatchSnapshot()
  })

  it("renders writeBufferSizeMB as bytes", () => {
    const { sql } = generateSql(
      makePipeline({ mergeEngine: "deduplicate", writeBufferSizeMB: 64 }),
    )
    // 64 MB = 67108864 bytes
    expect(sql).toContain("'write-buffer-size' = '67108864'")
    expect(sql).toMatchSnapshot()
  })

  it("renders snapshot retention bounds", () => {
    const { sql } = generateSql(
      makePipeline({
        mergeEngine: "deduplicate",
        snapshotNumRetainedMin: 10,
        snapshotNumRetainedMax: 20,
      }),
    )
    expect(sql).toContain("'snapshot.num-retained.min' = '10'")
    expect(sql).toContain("'snapshot.num-retained.max' = '20'")
  })

  it("renders 'first-row' merge engine", () => {
    const { sql } = generateSql(makePipeline({ mergeEngine: "first-row" }))
    expect(sql).toContain("'merge-engine' = 'first-row'")
    expect(sql).toMatchSnapshot()
  })

  it("emits no Paimon table-option keys when all CDC props are omitted", () => {
    // mergeEngine still required for retract upstream — set it but leave the
    // CDC tuning props alone, then verify no spurious keys appear.
    const { sql } = generateSql(makePipeline({ mergeEngine: "deduplicate" }))
    expect(sql).not.toMatch(/'bucket'/)
    expect(sql).not.toMatch(/'bucket-key'/)
    expect(sql).not.toMatch(/'full-compaction\.delta-commits'/)
    expect(sql).not.toMatch(/'write-buffer-size'/)
    expect(sql).not.toMatch(/'snapshot\.num-retained\.min'/)
    expect(sql).not.toMatch(/'snapshot\.num-retained\.max'/)
  })

  it("renders the full-coverage CDC config used by the pg-fluss-paimon template", () => {
    const { sql } = generateSql(
      makePipeline({
        mergeEngine: "deduplicate",
        changelogProducer: "input",
        bucket: 8,
        bucketKey: ["order_id"],
        fullCompactionDeltaCommits: 10,
      }),
    )
    expect(sql).toMatchSnapshot()
  })
})

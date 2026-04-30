import { beforeEach, describe, expect, it } from "vitest"
import { resolveConnectors } from "@/codegen/connector-resolver.js"
import { generateSql } from "@/codegen/sql-generator.js"
import {
  FlussCatalog,
  IcebergCatalog,
  PaimonCatalog,
} from "@/components/catalogs.js"
import { Pipeline } from "@/components/pipeline.js"
import { FlussSink, IcebergSink, PaimonSink } from "@/components/sinks.js"
import { type FlussScanStartupMode, FlussSource } from "@/components/sources.js"
import { validateConnectorProperties } from "@/core/connector-validation.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"

const FLINK_VERSION: FlinkMajorVersion = "2.0"

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    user_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    order_ts: Field.TIMESTAMP(3),
  },
})

beforeEach(() => {
  resetNodeIdCounter()
})

const STARTUP_MODES: readonly FlussScanStartupMode[] = [
  "initial",
  "earliest",
  "latest",
  "timestamp",
]

// ── 6.1 + 6.3: snapshot the rendered SQL across (mode × table-type) ─

describe("FlussSource startup-mode × table-type matrix", () => {
  for (const mode of STARTUP_MODES) {
    for (const flavor of ["log", "primary-key"] as const) {
      it(`renders ${flavor} table read with scanStartupMode='${mode}'`, () => {
        const cat = FlussCatalog({
          name: "fluss_cat",
          bootstrapServers: "fluss-coordinator:9123",
        })

        const source = FlussSource({
          catalog: cat.handle,
          database: "shop",
          table: "orders",
          schema: OrderSchema,
          scanStartupMode: mode,
          ...(mode === "timestamp"
            ? { scanStartupTimestampMs: 1735689600000 }
            : {}),
          ...(flavor === "primary-key" ? { primaryKey: ["order_id"] } : {}),
        })

        // Sink to a generic Paimon target so the pipeline has a terminus.
        // Append-only flavors get an append-only Paimon target (no mergeEngine);
        // primary-key flavors get a merge-engine Paimon target.
        const paimon = PaimonCatalog({
          name: "lake",
          warehouse: "s3://bucket/lake",
        })
        const sink = PaimonSink({
          catalog: paimon.handle,
          database: "warehouse",
          table: "orders_view",
          ...(flavor === "primary-key"
            ? { primaryKey: ["order_id"], mergeEngine: "deduplicate" }
            : {}),
          children: [source],
        })

        const pipeline = Pipeline({
          name: "fluss-pg-paimon",
          children: [cat.node, paimon.node, sink],
        })

        const { sql } = generateSql(pipeline, { flinkVersion: FLINK_VERSION })
        expect(sql).toMatchSnapshot()
      })
    }
  }

  it("renders 'scan.startup.timestamp' WITH key only when mode is 'timestamp'", () => {
    const cat = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })

    const tsSource = FlussSource({
      catalog: cat.handle,
      database: "shop",
      table: "orders",
      schema: OrderSchema,
      scanStartupMode: "timestamp",
      scanStartupTimestampMs: 1735689600000,
    })

    const initialSource = FlussSource({
      catalog: cat.handle,
      database: "shop",
      table: "orders",
      schema: OrderSchema,
      scanStartupMode: "initial",
    })

    const paimon = PaimonCatalog({
      name: "lake",
      warehouse: "s3://bucket/lake",
    })

    const tsSink = PaimonSink({
      catalog: paimon.handle,
      database: "warehouse",
      table: "ts_view",
      children: [tsSource],
    })
    const initialSink = PaimonSink({
      catalog: paimon.handle,
      database: "warehouse",
      table: "initial_view",
      children: [initialSource],
    })

    const tsPipeline = Pipeline({
      name: "fluss-ts",
      children: [cat.node, paimon.node, tsSink],
    })
    const initialPipeline = Pipeline({
      name: "fluss-initial",
      children: [cat.node, paimon.node, initialSink],
    })

    const tsSql = generateSql(tsPipeline, { flinkVersion: FLINK_VERSION }).sql
    const initialSql = generateSql(initialPipeline, {
      flinkVersion: FLINK_VERSION,
    }).sql

    expect(tsSql).toContain("'scan.startup.timestamp' = '1735689600000'")
    expect(initialSql).not.toContain("scan.startup.timestamp")
  })

  it("includes PRIMARY KEY (...) NOT ENFORCED in DDL only when primaryKey is set", () => {
    const cat = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })

    const pkSource = FlussSource({
      catalog: cat.handle,
      database: "shop",
      table: "orders",
      schema: OrderSchema,
      primaryKey: ["order_id"],
    })

    const logSource = FlussSource({
      catalog: cat.handle,
      database: "shop",
      table: "events",
      schema: OrderSchema,
    })

    const paimon = PaimonCatalog({
      name: "lake",
      warehouse: "s3://bucket/lake",
    })

    const pkSink = PaimonSink({
      catalog: paimon.handle,
      database: "warehouse",
      table: "pk_view",
      primaryKey: ["order_id"],
      mergeEngine: "deduplicate",
      children: [pkSource],
    })
    const logSink = PaimonSink({
      catalog: paimon.handle,
      database: "warehouse",
      table: "log_view",
      children: [logSource],
    })

    const pkSql = generateSql(
      Pipeline({
        name: "fluss-pk",
        children: [cat.node, paimon.node, pkSink],
      }),
      { flinkVersion: FLINK_VERSION },
    ).sql

    const logSql = generateSql(
      Pipeline({
        name: "fluss-log",
        children: [cat.node, paimon.node, logSink],
      }),
      { flinkVersion: FLINK_VERSION },
    ).sql

    expect(pkSql).toContain("PRIMARY KEY (`order_id`) NOT ENFORCED")
    expect(logSql).not.toContain("PRIMARY KEY")
  })
})

// ── 6.2: ChangelogMode inference ─────────────────────────────────────

describe("FlussSource changelog mode inference", () => {
  it("emits 'append-only' when primaryKey is omitted (Log table read)", () => {
    const cat = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })
    const node = FlussSource({
      catalog: cat.handle,
      database: "shop",
      table: "orders",
      schema: OrderSchema,
    })
    expect(node.props.changelogMode).toBe("append-only")
  })

  it("emits 'retract' when primaryKey is set (PrimaryKey table read)", () => {
    const cat = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })
    const node = FlussSource({
      catalog: cat.handle,
      database: "shop",
      table: "orders",
      schema: OrderSchema,
      primaryKey: ["order_id"],
    })
    expect(node.props.changelogMode).toBe("retract")
  })
})

// ── 6.4: connector-jar resolution ────────────────────────────────────

describe("FlussSource connector resolution", () => {
  it("includes the Fluss connector artifact when a FlussSource is present", () => {
    const cat = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })

    const source = FlussSource({
      catalog: cat.handle,
      database: "shop",
      table: "orders",
      schema: OrderSchema,
    })

    const paimon = PaimonCatalog({
      name: "lake",
      warehouse: "s3://bucket/lake",
    })
    const sink = PaimonSink({
      catalog: paimon.handle,
      database: "warehouse",
      table: "orders_view",
      children: [source],
    })

    const pipeline = Pipeline({
      name: "p",
      children: [cat.node, paimon.node, sink],
    })

    const { jars } = resolveConnectors(pipeline, {
      flinkVersion: FLINK_VERSION,
    })
    const fluss = jars.filter(
      (j) => j.artifact.artifactId === "fluss-connector-flink",
    )
    expect(fluss).toHaveLength(1)
  })

  it("includes the Fluss connector artifact exactly once when source and sink coexist", () => {
    const cat = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })

    const source = FlussSource({
      catalog: cat.handle,
      database: "shop",
      table: "orders",
      schema: OrderSchema,
      primaryKey: ["order_id"],
    })

    const sink = FlussSink({
      catalog: cat.handle,
      database: "warehouse",
      table: "orders_archive",
      primaryKey: ["order_id"],
      children: [source],
    })

    const pipeline = Pipeline({
      name: "fluss-roundtrip",
      children: [cat.node, sink],
    })

    const { jars } = resolveConnectors(pipeline, {
      flinkVersion: FLINK_VERSION,
    })
    const fluss = jars.filter(
      (j) => j.artifact.artifactId === "fluss-connector-flink",
    )
    expect(fluss).toHaveLength(1)
  })
})

// ── 6.5: FlussSource → IcebergSink upsert happy path ─────────────────

describe("FlussSource → IcebergSink (upsertEnabled)", () => {
  it("synthesizes the retract→Iceberg-MoR pipeline cleanly", () => {
    const fluss = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })
    const iceberg = IcebergCatalog({
      name: "iceberg_cat",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })

    const source = FlussSource({
      catalog: fluss.handle,
      database: "shop",
      table: "orders",
      schema: OrderSchema,
      primaryKey: ["order_id"],
    })

    const sink = IcebergSink({
      catalog: iceberg.handle,
      database: "lakehouse",
      table: "orders_mor",
      primaryKey: ["order_id"],
      formatVersion: 2,
      upsertEnabled: true,
      equalityFieldColumns: ["order_id"],
      children: [source],
    })

    const pipeline = Pipeline({
      name: "fluss-iceberg-mor",
      children: [fluss.node, iceberg.node, sink],
    })

    const { sql } = generateSql(pipeline, { flinkVersion: FLINK_VERSION })
    expect(sql).toMatchSnapshot()

    const diags = validateConnectorProperties(pipeline)
    expect(diags.filter((d) => d.severity === "error")).toEqual([])
  })
})

// ── 6.6: FlussSource → PaimonSink mergeEngine happy path ─────────────

describe("FlussSource → PaimonSink (mergeEngine='deduplicate')", () => {
  it("synthesizes the Stage-B serve topology used by pg-fluss-paimon", () => {
    const fluss = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })
    const paimon = PaimonCatalog({
      name: "paimon_cat",
      warehouse: "s3://benchmark/paimon",
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
      database: "benchmark",
      table: "orders",
      primaryKey: ["order_id"],
      mergeEngine: "deduplicate",
      children: [source],
    })

    const pipeline = Pipeline({
      name: "pg-fluss-paimon-serve",
      children: [fluss.node, paimon.node, sink],
    })

    const { sql } = generateSql(pipeline, { flinkVersion: FLINK_VERSION })
    expect(sql).toMatchSnapshot()

    const diags = validateConnectorProperties(pipeline)
    expect(diags.filter((d) => d.severity === "error")).toEqual([])
  })
})

// ── 6.7: validation rejection for retract-required sink ──────────────

describe("FlussSource retract-required sink validation", () => {
  it("rejects FlussSource without primaryKey → PaimonSink with mergeEngine", () => {
    const fluss = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })
    const paimon = PaimonCatalog({
      name: "paimon_cat",
      warehouse: "s3://benchmark/paimon",
    })

    const source = FlussSource({
      catalog: fluss.handle,
      database: "shop",
      table: "events",
      schema: OrderSchema,
    })

    const sink = PaimonSink({
      catalog: paimon.handle,
      database: "benchmark",
      table: "events",
      primaryKey: ["order_id"],
      mergeEngine: "deduplicate",
      children: [source],
    })

    const pipeline = Pipeline({
      name: "fluss-log-paimon-merge",
      children: [fluss.node, paimon.node, sink],
    })

    const diags = validateConnectorProperties(pipeline)
    const errors = diags.filter((d) => d.severity === "error")
    expect(errors.length).toBeGreaterThan(0)
    const msg = errors.map((d) => d.message).join("\n")
    expect(msg).toContain("FlussSource")
    expect(msg).toContain("PaimonSink")
    expect(msg).toContain("requires a retract upstream")
  })

  it("rejects FlussSource without primaryKey → IcebergSink upsertEnabled", () => {
    const fluss = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })
    const iceberg = IcebergCatalog({
      name: "iceberg_cat",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })

    const source = FlussSource({
      catalog: fluss.handle,
      database: "shop",
      table: "events",
      schema: OrderSchema,
    })

    const sink = IcebergSink({
      catalog: iceberg.handle,
      database: "lakehouse",
      table: "events",
      primaryKey: ["order_id"],
      formatVersion: 2,
      upsertEnabled: true,
      equalityFieldColumns: ["order_id"],
      children: [source],
    })

    const pipeline = Pipeline({
      name: "fluss-log-iceberg-upsert",
      children: [fluss.node, iceberg.node, sink],
    })

    const diags = validateConnectorProperties(pipeline)
    const errors = diags.filter((d) => d.severity === "error")
    expect(errors.some((e) => e.message.includes("retract upstream"))).toBe(
      true,
    )
  })

  it("rejects tap:true on FlussSource", () => {
    const cat = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })

    const source = FlussSource({
      catalog: cat.handle,
      database: "shop",
      table: "orders",
      schema: OrderSchema,
      tap: true,
    })

    const paimon = PaimonCatalog({
      name: "lake",
      warehouse: "s3://bucket/lake",
    })
    const sink = PaimonSink({
      catalog: paimon.handle,
      database: "warehouse",
      table: "orders_view",
      children: [source],
    })

    const pipeline = Pipeline({
      name: "fluss-tap",
      children: [cat.node, paimon.node, sink],
    })

    const diags = validateConnectorProperties(pipeline)
    expect(
      diags.some(
        (d) =>
          d.severity === "error" &&
          d.message.includes("FlussSource") &&
          d.message.includes("tap"),
      ),
    ).toBe(true)
  })

  it("rejects scanStartupTimestampMs without scanStartupMode='timestamp'", () => {
    const cat = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })

    const source = FlussSource({
      catalog: cat.handle,
      database: "shop",
      table: "orders",
      schema: OrderSchema,
      scanStartupMode: "earliest",
      scanStartupTimestampMs: 12345,
    })

    const paimon = PaimonCatalog({
      name: "lake",
      warehouse: "s3://bucket/lake",
    })
    const sink = PaimonSink({
      catalog: paimon.handle,
      database: "warehouse",
      table: "orders_view",
      children: [source],
    })

    const pipeline = Pipeline({
      name: "fluss-bad-ts",
      children: [cat.node, paimon.node, sink],
    })

    const diags = validateConnectorProperties(pipeline)
    expect(
      diags.some(
        (d) =>
          d.severity === "error" &&
          d.message.includes("scanStartupTimestampMs"),
      ),
    ).toBe(true)
  })

  it("throws synchronously when scanStartupMode='timestamp' is used without timestamp", () => {
    const cat = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })
    expect(() =>
      FlussSource({
        catalog: cat.handle,
        database: "shop",
        table: "orders",
        schema: OrderSchema,
        scanStartupMode: "timestamp",
      }),
    ).toThrow(/scanStartupTimestampMs/)
  })
})

// ── connector-resolver registry self-check (task 3.1) ────────────────

describe("FlussSource connector registry self-check", () => {
  it("resolves the existing Fluss artifact without a duplicate registry entry", () => {
    const cat = FlussCatalog({
      name: "fluss_cat",
      bootstrapServers: "fluss-coordinator:9123",
    })

    const source = FlussSource({
      catalog: cat.handle,
      database: "shop",
      table: "orders",
      schema: OrderSchema,
    })

    const node: ConstructNode = Pipeline({
      name: "p",
      children: [cat.node, source],
    })

    const { jars } = resolveConnectors(node, { flinkVersion: FLINK_VERSION })
    const seen = jars
      .map((j) => `${j.artifact.groupId}:${j.artifact.artifactId}`)
      .sort()
    expect(seen).toEqual(["org.apache.fluss:fluss-connector-flink"])
  })
})

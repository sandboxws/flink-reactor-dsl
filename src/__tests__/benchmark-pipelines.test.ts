import { beforeEach, describe, expect, it } from "vitest"
import { resolveConnectors } from "@/codegen/connector-resolver.js"
import { generateCrd } from "@/codegen/crd-generator.js"
import { generatePipelineYaml } from "@/codegen/pipeline-yaml-generator.js"
import { generateSql } from "@/codegen/sql-generator.js"
import type { PostgresCdcSnapshotMode } from "@/components/sources.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"
import f1Pipeline, {
  type F1CommitMode,
  type F1WireFormat,
} from "../../pipelines/pg-cdc-iceberg-f1/index.js"
import f2Pipeline, {
  type F2CommitMode,
} from "../../pipelines/pg-cdc-iceberg-f2/index.js"

const FLINK_VERSION: FlinkMajorVersion = "2.0"

function findIcebergSink(n: ConstructNode): ConstructNode {
  if (n.component === "IcebergSink") return n
  for (const c of n.children) {
    if (c.component === "IcebergSink") return c
    try {
      return findIcebergSink(c)
    } catch {
      // keep searching siblings
    }
  }
  throw new Error("IcebergSink not found")
}

function jarCoordinates(
  jars: ReturnType<typeof resolveConnectors>["jars"],
): readonly string[] {
  return jars
    .map(
      (j) =>
        `${j.artifact.groupId}:${j.artifact.artifactId}:${j.artifact.version}`,
    )
    .sort()
}

beforeEach(() => {
  resetNodeIdCounter()
})

// ── F1 (Kafka-hop) ───────────────────────────────────────────────────

const F1_WIRE_FORMATS: readonly F1WireFormat[] = ["json", "avro", "protobuf"]
const F1_COMMIT_MODES: readonly F1CommitMode[] = ["throughput", "latency"]

describe("pg-cdc-iceberg-f1 (Kafka-hop)", () => {
  for (const wireFormat of F1_WIRE_FORMATS) {
    for (const commitMode of F1_COMMIT_MODES) {
      const combo = `${wireFormat} + ${commitMode}`
      it(`synthesizes a stable artifact bundle for ${combo}`, () => {
        const node = f1Pipeline({
          wireFormat,
          commitMode,
          schemaRegistryUrl: "http://schema-registry:8081",
        })

        const { sql } = generateSql(node, { flinkVersion: FLINK_VERSION })
        const crd = generateCrd(node, { flinkVersion: FLINK_VERSION })
        const { jars } = resolveConnectors(node, {
          flinkVersion: FLINK_VERSION,
        })

        expect({
          sql,
          crd,
          jars: jarCoordinates(jars),
        }).toMatchSnapshot()
      })
    }
  }

  it("threads the schema-registry URL through the Kafka DDL for avro", () => {
    const node = f1Pipeline({
      wireFormat: "avro",
      commitMode: "throughput",
      schemaRegistryUrl: "http://sr.internal:8081",
    })

    const { sql } = generateSql(node, { flinkVersion: FLINK_VERSION })
    expect(sql).toContain("'debezium-avro.schema-registry.url'")
    expect(sql).toContain("http://sr.internal:8081")
  })

  it("threads the schema-registry URL through the Kafka DDL for protobuf", () => {
    const node = f1Pipeline({
      wireFormat: "protobuf",
      commitMode: "latency",
      schemaRegistryUrl: "http://sr.internal:8081",
    })

    const { sql } = generateSql(node, { flinkVersion: FLINK_VERSION })
    expect(sql).toContain("'debezium-protobuf.schema-registry.url'")
    expect(sql).toContain("http://sr.internal:8081")
  })

  it("threads commitIntervalSeconds onto the IcebergSink node per commitMode", () => {
    const latency = f1Pipeline({
      wireFormat: "protobuf",
      commitMode: "latency",
      schemaRegistryUrl: "http://sr.internal:8081",
    })
    const throughput = f1Pipeline({
      wireFormat: "json",
      commitMode: "throughput",
    })

    expect(findIcebergSink(latency).props.commitIntervalSeconds).toBe(2)
    expect(findIcebergSink(throughput).props.commitIntervalSeconds).toBe(10)
  })

  it("resolves the protobuf-confluent-registry jar when wireFormat=protobuf", () => {
    const node = f1Pipeline({
      wireFormat: "protobuf",
      commitMode: "throughput",
      schemaRegistryUrl: "http://sr.internal:8081",
    })

    const { jars } = resolveConnectors(node, { flinkVersion: FLINK_VERSION })
    const ids = jars.map((j) => j.artifact.artifactId)
    expect(ids).toContain("flink-sql-protobuf-confluent-registry")
    expect(ids).toContain("iceberg-flink-runtime-2.0")
  })

  it("produces byte-identical output across two synthesis runs", () => {
    const first = (() => {
      resetNodeIdCounter()
      const node = f1Pipeline({
        wireFormat: "json",
        commitMode: "throughput",
      })
      return {
        sql: generateSql(node, { flinkVersion: FLINK_VERSION }).sql,
        crd: generateCrd(node, { flinkVersion: FLINK_VERSION }),
      }
    })()

    const second = (() => {
      resetNodeIdCounter()
      const node = f1Pipeline({
        wireFormat: "json",
        commitMode: "throughput",
      })
      return {
        sql: generateSql(node, { flinkVersion: FLINK_VERSION }).sql,
        crd: generateCrd(node, { flinkVersion: FLINK_VERSION }),
      }
    })()

    expect(second.sql).toBe(first.sql)
    expect(second.crd).toEqual(first.crd)
  })
})

// ── F2 (Pipeline Connector) ──────────────────────────────────────────

const F2_SNAPSHOT_MODES: readonly PostgresCdcSnapshotMode[] = [
  "initial",
  "never",
  "initial_only",
]
const F2_COMMIT_MODES: readonly F2CommitMode[] = ["throughput", "latency"]

describe("pg-cdc-iceberg-f2 (Pipeline Connector)", () => {
  for (const snapshotMode of F2_SNAPSHOT_MODES) {
    for (const commitMode of F2_COMMIT_MODES) {
      const combo = `${snapshotMode} + ${commitMode}`
      it(`synthesizes a stable artifact bundle for ${combo}`, () => {
        const node = f2Pipeline({ snapshotMode, commitMode })

        const pipelineYaml = generatePipelineYaml(node)
        const crd = generateCrd(node, { flinkVersion: FLINK_VERSION })
        const { jars } = resolveConnectors(node, {
          flinkVersion: FLINK_VERSION,
        })

        expect({
          pipelineYaml,
          crd,
          jars: jarCoordinates(jars),
        }).toMatchSnapshot()
      })
    }
  }

  it("pins the Flink CDC Postgres pipeline connector at 3.6.0", () => {
    const node = f2Pipeline({
      snapshotMode: "initial",
      commitMode: "latency",
    })

    const { jars } = resolveConnectors(node, { flinkVersion: FLINK_VERSION })
    const postgresCdcJar = jars.find(
      (j) => j.artifact.artifactId === "flink-cdc-pipeline-connector-postgres",
    )
    expect(postgresCdcJar?.artifact.version).toBe("3.6.0")
  })

  it("includes the iceberg-flink runtime matching the target Flink version", () => {
    const node = f2Pipeline({
      snapshotMode: "initial",
      commitMode: "throughput",
    })

    const resolved2x = resolveConnectors(node, { flinkVersion: "2.0" })
    expect(resolved2x.jars.map((j) => j.artifact.artifactId)).toContain(
      "iceberg-flink-runtime-2.0",
    )

    const resolved1x = resolveConnectors(node, { flinkVersion: "1.20" })
    expect(resolved1x.jars.map((j) => j.artifact.artifactId)).toContain(
      "iceberg-flink-runtime-1.20",
    )
  })

  it("produces a bounded job for snapshotMode=initial_only", () => {
    const node = f2Pipeline({
      snapshotMode: "initial_only",
      commitMode: "throughput",
    })

    const pipelineYaml = generatePipelineYaml(node)
    expect(pipelineYaml).toContain("scan.startup.mode: snapshot")
    expect(pipelineYaml).toContain("slot.drop.on.stop: true")
  })

  it("disables snapshot when snapshotMode=never", () => {
    // Flink CDC 3.6 routes "no snapshot" through scan.startup.mode rather
    // than the legacy scan.snapshot.enabled toggle.
    const node = f2Pipeline({
      snapshotMode: "never",
      commitMode: "throughput",
    })

    const pipelineYaml = generatePipelineYaml(node)
    expect(pipelineYaml).toContain("scan.startup.mode: latest-offset")
    expect(pipelineYaml).not.toContain("scan.snapshot.enabled")
  })

  it("emits commit-interval-ms=2000 for commitMode=latency", () => {
    const node = f2Pipeline({
      snapshotMode: "initial",
      commitMode: "latency",
    })

    const pipelineYaml = generatePipelineYaml(node)
    expect(pipelineYaml).toContain(
      "table.properties.commit-interval-ms: '2000'",
    )
  })

  it("produces byte-identical output across two synthesis runs", () => {
    const render = () => {
      resetNodeIdCounter()
      const node = f2Pipeline({
        snapshotMode: "initial",
        commitMode: "latency",
      })
      return {
        yaml: generatePipelineYaml(node),
        crd: generateCrd(node, { flinkVersion: FLINK_VERSION }),
      }
    }

    const first = render()
    const second = render()
    expect(second.yaml).toBe(first.yaml)
    expect(second.crd).toEqual(first.crd)
  })
})

// ── Cross-pipeline invariants ────────────────────────────────────────

describe("F1 vs F2 cross-pipeline invariants", () => {
  it("both pipelines target the same Iceberg catalog.database.table", () => {
    const f1 = f1Pipeline({ wireFormat: "json", commitMode: "throughput" })
    const f2 = f2Pipeline({ snapshotMode: "initial", commitMode: "throughput" })

    const f1Sql = generateSql(f1, { flinkVersion: FLINK_VERSION }).sql
    const f2Yaml = generatePipelineYaml(f2)

    expect(f1Sql).toContain("`lakekeeper`.`tpch`.`orders`")
    expect(f2Yaml).toContain("catalog.name: lakekeeper")
    expect(f2Yaml).toContain("database: tpch")
    expect(f2Yaml).toContain("table: orders")
  })

  it("both pipelines carry the same MoR knobs on the Iceberg sink node", () => {
    const f1 = f1Pipeline({ wireFormat: "json", commitMode: "throughput" })
    const f2 = f2Pipeline({ snapshotMode: "initial", commitMode: "throughput" })

    const f1Sink = findIcebergSink(f1).props
    const f2Sink = findIcebergSink(f2).props

    for (const key of [
      "upsertEnabled",
      "formatVersion",
      "writeDistributionMode",
      "targetFileSizeMB",
      "writeParquetCompression",
      "equalityFieldColumns",
    ]) {
      expect(f1Sink[key]).toEqual(f2Sink[key])
    }

    // F2 (pipeline.yaml path) actually renders the MoR knobs into the emitted
    // artifact. F1 (Flink SQL path, catalog-managed IcebergSink) leaves them
    // on the node for downstream consumers but does not emit them into DDL —
    // Flink's Iceberg integration derives them from the catalog at runtime.
    const f2Yaml = generatePipelineYaml(f2)?.toLowerCase() ?? ""
    for (const fragment of [
      "write.upsert.enabled",
      "format-version",
      "zstd",
      "hash",
    ]) {
      expect(f2Yaml).toContain(fragment)
    }
  })
})

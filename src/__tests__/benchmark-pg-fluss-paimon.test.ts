import { beforeEach, describe, expect, it } from "vitest"
import { resolveConnectors } from "@/codegen/connector-resolver.js"
import { generateCrd } from "@/codegen/crd-generator.js"
import { generatePipelineYaml } from "@/codegen/pipeline-yaml-generator.js"
import { generateSql } from "@/codegen/sql-generator.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import type { FlinkMajorVersion } from "@/core/types.js"
import ingestPipeline, {
  type IngestCommitMode,
  type IngestSnapshotMode,
} from "../../pipelines/pg-fluss-paimon/ingest.js"
import servePipeline, {
  type ServeCommitMode,
  type ServeMergeEngine,
} from "../../pipelines/pg-fluss-paimon/serve.js"

const FLINK_VERSION: FlinkMajorVersion = "2.2"

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

// ── Ingest (PostgresCdcPipelineSource → FlussSink, Pipeline-YAML branch) ──

const INGEST_SNAPSHOT_MODES: readonly IngestSnapshotMode[] = [
  "initial",
  "never",
]
const INGEST_COMMIT_MODES: readonly IngestCommitMode[] = [
  "throughput",
  "latency",
]

describe("pg-fluss-paimon-ingest (Pipeline-YAML branch)", () => {
  for (const snapshotMode of INGEST_SNAPSHOT_MODES) {
    for (const commitMode of INGEST_COMMIT_MODES) {
      const combo = `${snapshotMode} + ${commitMode}`
      it(`synthesizes a stable artifact bundle for ${combo}`, () => {
        const node = ingestPipeline({ snapshotMode, commitMode })

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

  it("emits the Pipeline-Connector Fluss artifact and NOT the Flink-SQL Fluss artifact", () => {
    const node = ingestPipeline({
      snapshotMode: "initial",
      commitMode: "throughput",
    })
    const { jars } = resolveConnectors(node, { flinkVersion: FLINK_VERSION })
    const ids = jars.map((j) => j.artifact.artifactId)
    expect(ids).toContain("flink-cdc-pipeline-connector-postgres")
    expect(ids).toContain("flink-cdc-pipeline-connector-fluss")
    expect(ids).not.toContain("fluss-flink-2.2")
  })

  it("emits schema.evolution.behavior: lenient on the Fluss sink stanza", () => {
    const node = ingestPipeline({
      snapshotMode: "initial",
      commitMode: "throughput",
    })
    const yaml = generatePipelineYaml(node)
    expect(yaml).toContain("schema.evolution.behavior: lenient")
  })

  it("threads commitMode onto execution.checkpointing.interval (latency=2000ms)", () => {
    const node = ingestPipeline({
      snapshotMode: "initial",
      commitMode: "latency",
    })
    const crd = generateCrd(node, { flinkVersion: FLINK_VERSION })
    expect(
      crd.spec.flinkConfiguration["execution.checkpointing.interval"],
    ).toBe("2000")
  })

  it("emits scan.snapshot.enabled: false for snapshotMode='never'", () => {
    const node = ingestPipeline({
      snapshotMode: "never",
      commitMode: "throughput",
    })
    const yaml = generatePipelineYaml(node)
    expect(yaml).toContain("scan.snapshot.enabled: false")
  })
})

// ── Serve (FlussSource → Filter → PaimonSink, Flink-SQL branch) ──────

const SERVE_MERGE_ENGINES: readonly ServeMergeEngine[] = [
  "deduplicate",
  "partial-update",
]
const SERVE_COMMIT_MODES: readonly ServeCommitMode[] = ["throughput", "latency"]

describe("pg-fluss-paimon-serve (Flink-SQL branch)", () => {
  for (const paimonMergeEngine of SERVE_MERGE_ENGINES) {
    for (const commitMode of SERVE_COMMIT_MODES) {
      const combo = `${paimonMergeEngine} + ${commitMode}`
      it(`synthesizes a stable artifact bundle for ${combo}`, () => {
        const node = servePipeline({ paimonMergeEngine, commitMode })

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

  it("emits the Flink-SQL Fluss artifact at 0.9.0-incubating", () => {
    const node = servePipeline({
      paimonMergeEngine: "deduplicate",
      commitMode: "throughput",
    })
    const { jars } = resolveConnectors(node, { flinkVersion: FLINK_VERSION })
    const fluss = jars.find((j) => j.artifact.artifactId === "fluss-flink-2.2")
    expect(fluss?.artifact.groupId).toBe("org.apache.fluss")
    expect(fluss?.artifact.version).toBe("0.9.0-incubating")
  })

  it("emits the Paimon Flink connector matching the active Flink major", () => {
    const node = servePipeline({
      paimonMergeEngine: "deduplicate",
      commitMode: "throughput",
    })

    const flink22 = resolveConnectors(node, { flinkVersion: "2.2" })
    expect(flink22.jars.map((j) => j.artifact.artifactId)).toContain(
      "paimon-flink-2.2",
    )

    const flink120 = resolveConnectors(node, { flinkVersion: "1.20" })
    expect(flink120.jars.map((j) => j.artifact.artifactId)).toContain(
      "paimon-flink-1.20",
    )
  })

  it("threads paimonMergeEngine onto the PaimonSink WITH clause", () => {
    const dedupeNode = servePipeline({
      paimonMergeEngine: "deduplicate",
      commitMode: "throughput",
    })
    const partialNode = servePipeline({
      paimonMergeEngine: "partial-update",
      commitMode: "throughput",
    })

    const dedupeSql = generateSql(dedupeNode, {
      flinkVersion: FLINK_VERSION,
    }).sql
    const partialSql = generateSql(partialNode, {
      flinkVersion: FLINK_VERSION,
    }).sql

    expect(dedupeSql).toContain("'merge-engine' = 'deduplicate'")
    expect(partialSql).toContain("'merge-engine' = 'partial-update'")
  })

  it("threads commitMode onto execution.checkpointing.interval (latency=2000ms)", () => {
    const node = servePipeline({
      paimonMergeEngine: "deduplicate",
      commitMode: "latency",
    })
    const { sql } = generateSql(node, { flinkVersion: FLINK_VERSION })
    expect(sql).toContain("'execution.checkpointing.interval' = '2000'")
  })
})

// ── Cross-pipeline contract ──────────────────────────────────────────

describe("ingest ↔ serve cross-pipeline contract", () => {
  it("both pipelines reference the same FlussCatalog bootstrap.servers", () => {
    const ingest = ingestPipeline({
      snapshotMode: "initial",
      commitMode: "throughput",
    })
    const serve = servePipeline({
      paimonMergeEngine: "deduplicate",
      commitMode: "throughput",
    })

    const ingestYaml = generatePipelineYaml(ingest)
    const serveSql = generateSql(serve, { flinkVersion: FLINK_VERSION }).sql

    expect(ingestYaml).toContain("bootstrap.servers: fluss-coordinator:9123")
    expect(serveSql).toContain("'bootstrap.servers' = 'fluss-coordinator:9123'")
  })

  it("serve pipeline reads from the shared Fluss database.table the ingest writes to", () => {
    const serve = servePipeline({
      paimonMergeEngine: "deduplicate",
      commitMode: "throughput",
    })
    const serveSql = generateSql(serve, { flinkVersion: FLINK_VERSION }).sql

    // The Flink CDC Pipeline Connector for Fluss auto-derives the target
    // Fluss table from upstream Postgres tables, so the ingest YAML stanza
    // does not emit `database:` / `table:` keys — the cross-pipeline contract
    // is `bootstrap.servers` (asserted in the previous test). The serve side
    // declares its FlussSource read target via a `CREATE TABLE … LIKE
    // <catalog>.<database>.<table>` clause (Fluss 0.9.0-incubating is
    // catalog-only — no SQL `connector='fluss'` factory exists, so the
    // database/table coordinates live in the LIKE path, not in WITH).
    expect(serveSql).toContain("`benchmark`.`orders`")
  })

  it("produces byte-identical output across two synthesis runs (ingest)", () => {
    const render = () => {
      resetNodeIdCounter()
      const node = ingestPipeline({
        snapshotMode: "initial",
        commitMode: "throughput",
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

  it("produces byte-identical output across two synthesis runs (serve)", () => {
    const render = () => {
      resetNodeIdCounter()
      const node = servePipeline({
        paimonMergeEngine: "deduplicate",
        commitMode: "throughput",
      })
      return {
        sql: generateSql(node, { flinkVersion: FLINK_VERSION }).sql,
        crd: generateCrd(node, { flinkVersion: FLINK_VERSION }),
      }
    }

    const first = render()
    const second = render()
    expect(second.sql).toBe(first.sql)
    expect(second.crd).toEqual(first.crd)
  })
})

import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { afterEach, beforeEach, describe, expect, it } from "vitest"
import { runSynth } from "@/cli/commands/synth.js"

// The project root — we use absolute imports to the real jsx-runtime
const projectRoot = resolve(__dirname, "../../../../")

describe("synth command", () => {
  let tempDir: string

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "flink-reactor-synth-"))
  })

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true })
  })

  function writePipeline(name: string, content: string): void {
    const dir = join(tempDir, "pipelines", name)
    mkdirSync(dir, { recursive: true })
    writeFileSync(join(dir, "index.tsx"), content, "utf-8")
  }

  // Use absolute path to the real jsx-runtime so dynamic import resolves
  function makeSimplePipeline(): string {
    const jsxPath = join(projectRoot, "src/core/jsx-runtime.js")
    return `
import { createElement } from '${jsxPath}';

const pipeline = createElement('Pipeline', { name: 'test-pipeline', parallelism: 1 },
  createElement('KafkaSink', { topic: 'output', format: 'json', bootstrapServers: 'localhost:9092' },
    createElement('Filter', { condition: 'amount > 100' },
      createElement('KafkaSource', {
        topic: 'input',
        format: 'json',
        bootstrapServers: 'localhost:9092',
        schema: {
          fields: { amount: 'BIGINT', name: 'STRING' },
          metadataColumns: [],
        },
      })
    )
  )
);

export default pipeline;
`
  }

  it("discovers and synthesizes a pipeline to dist/", async () => {
    writePipeline("orders", makeSimplePipeline())

    const artifacts = await runSynth({
      outdir: "dist",
      projectDir: tempDir,
    })

    expect(artifacts.length).toBeGreaterThanOrEqual(1)

    const artifact = artifacts[0]
    expect(artifact.name).toBe("test-pipeline")

    // Check output files
    expect(
      existsSync(join(tempDir, "dist", "test-pipeline", "pipeline.sql")),
    ).toBe(true)
    expect(
      existsSync(join(tempDir, "dist", "test-pipeline", "deployment.yaml")),
    ).toBe(true)
    expect(
      existsSync(join(tempDir, "dist", "test-pipeline", "configmap.yaml")),
    ).toBe(true)

    // Validate SQL content
    const sql = readFileSync(
      join(tempDir, "dist", "test-pipeline", "pipeline.sql"),
      "utf-8",
    )
    expect(sql).toContain("CREATE TABLE")
    expect(sql).toContain("INSERT INTO")

    // Validate YAML content
    const yaml = readFileSync(
      join(tempDir, "dist", "test-pipeline", "deployment.yaml"),
      "utf-8",
    )
    expect(yaml).toContain("FlinkDeployment")
    expect(yaml).toContain("flink.apache.org/v1beta1")

    // Validate ConfigMap
    const configMap = readFileSync(
      join(tempDir, "dist", "test-pipeline", "configmap.yaml"),
      "utf-8",
    )
    expect(configMap).toContain("ConfigMap")
    expect(configMap).toContain("test-pipeline-sql")
  })

  it("returns empty array when no pipelines found", async () => {
    const artifacts = await runSynth({
      outdir: "dist",
      projectDir: tempDir,
    })

    expect(artifacts).toEqual([])
  })

  it("supports --pipeline flag to target specific pipeline", async () => {
    writePipeline("orders", makeSimplePipeline())
    writePipeline("analytics", makeSimplePipeline())

    const artifacts = await runSynth({
      pipeline: "orders",
      outdir: "dist",
      projectDir: tempDir,
    })

    // Should only synth the targeted pipeline
    expect(artifacts.length).toBeGreaterThanOrEqual(1)
  })

  it("supports --outdir flag for custom output directory", async () => {
    writePipeline("orders", makeSimplePipeline())

    await runSynth({
      outdir: "build/output",
      projectDir: tempDir,
    })

    expect(
      existsSync(
        join(tempDir, "build", "output", "test-pipeline", "pipeline.sql"),
      ),
    ).toBe(true)
  })
})

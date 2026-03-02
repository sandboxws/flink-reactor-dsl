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
import { generateAscii, generateDot } from "@/cli/commands/graph.js"
import { runSynth } from "@/cli/commands/synth.js"
import { runValidate } from "@/cli/commands/validate.js"
import { createElement } from "@/core/jsx-runtime.js"
import { SynthContext } from "@/core/synth-context.js"

const projectRoot = resolve(__dirname, "../../../../")

describe("end-to-end: new -> write pipeline -> synth -> validate -> graph", () => {
  let tempDir: string

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "flink-reactor-e2e-"))
  })

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true })
  })

  it("full CLI workflow produces correct output", async () => {
    // Step 1: "Write" a pipeline (simulating what `new` scaffolds)
    const pipelineDir = join(tempDir, "pipelines", "orders")
    mkdirSync(pipelineDir, { recursive: true })

    const jsxPath = join(projectRoot, "src/core/jsx-runtime.js")
    writeFileSync(
      join(pipelineDir, "index.tsx"),
      `
import { createElement } from '${jsxPath}';

const pipeline = createElement('Pipeline', { name: 'orders', parallelism: 2 },
  createElement('KafkaSink', {
    topic: 'processed-orders',
    format: 'json',
    bootstrapServers: 'kafka:9092',
  },
    createElement('Filter', { condition: 'total > 0' },
      createElement('KafkaSource', {
        topic: 'raw-orders',
        format: 'json',
        bootstrapServers: 'kafka:9092',
        schema: {
          fields: { order_id: 'BIGINT', total: 'DOUBLE', customer: 'STRING' },
          metadataColumns: [],
        },
      })
    )
  )
);

export default pipeline;
`,
      "utf-8",
    )

    // Step 2: Synth
    const artifacts = await runSynth({
      outdir: "dist",
      projectDir: tempDir,
    })

    expect(artifacts.length).toBeGreaterThanOrEqual(1)

    // Verify files
    expect(existsSync(join(tempDir, "dist", "orders", "pipeline.sql"))).toBe(
      true,
    )
    expect(existsSync(join(tempDir, "dist", "orders", "deployment.yaml"))).toBe(
      true,
    )
    expect(existsSync(join(tempDir, "dist", "orders", "configmap.yaml"))).toBe(
      true,
    )

    const sql = readFileSync(
      join(tempDir, "dist", "orders", "pipeline.sql"),
      "utf-8",
    )
    expect(sql).toContain("raw-orders")
    expect(sql).toContain("processed-orders")
    expect(sql).toContain("total > 0")
    expect(sql).toContain("INSERT INTO")

    const yaml = readFileSync(
      join(tempDir, "dist", "orders", "deployment.yaml"),
      "utf-8",
    )
    expect(yaml).toContain("FlinkDeployment")

    // Step 3: Validate
    const valid = await runValidate({
      projectDir: tempDir,
    })
    expect(valid).toBe(true)

    // Step 4: Graph (test the pure rendering function, since runGraph
    //          uses loadPipeline which requires the same setup)
    const pipelineNode = createElement(
      "Pipeline",
      { name: "orders", parallelism: 2 },
      createElement(
        "KafkaSink",
        { topic: "processed-orders" },
        createElement(
          "Filter",
          { condition: "total > 0" },
          createElement("KafkaSource", {
            topic: "raw-orders",
            schema: {
              fields: { order_id: "BIGINT", total: "DOUBLE" },
              metadataColumns: [],
            },
          }),
        ),
      ),
    )

    const ctx = new SynthContext()
    ctx.buildFromTree(pipelineNode)

    const ascii = generateAscii("orders", ctx.getAllNodes(), ctx.getAllEdges())
    expect(ascii).toContain("Pipeline: orders")
    expect(ascii).toContain("KafkaSource")
    expect(ascii).toContain("Filter")
    expect(ascii).toContain("KafkaSink")

    const dot = generateDot("orders", ctx.getAllNodes(), ctx.getAllEdges())
    expect(dot).toContain('digraph "orders"')
    expect(dot).toContain("->")
  })
})

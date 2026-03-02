import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { afterEach, beforeEach, describe, expect, it } from "vitest"
import { runSchema } from "../schema.js"

const projectRoot = resolve(__dirname, "../../../../")

describe("schema command", () => {
  let tempDir: string

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "flink-reactor-schema-"))
  })

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true })
  })

  function writePipeline(name: string, content: string): void {
    const dir = join(tempDir, "pipelines", name)
    mkdirSync(dir, { recursive: true })
    writeFileSync(join(dir, "index.tsx"), content, "utf-8")
  }

  function writeFile(relPath: string, content: string): void {
    const absPath = join(tempDir, relPath)
    mkdirSync(join(absPath, ".."), { recursive: true })
    writeFileSync(absPath, content, "utf-8")
  }

  const jsxPath = join(projectRoot, "src/core/jsx-runtime.js")

  function makeSimplePipeline(): string {
    return `
import { createElement } from '${jsxPath}';

const pipeline = createElement('Pipeline', { name: 'test-pipeline', parallelism: 1 },
  createElement('KafkaSink', { topic: 'output', format: 'json', _nameHint: 'output' },
    createElement('Filter', { condition: 'amount > 100' },
      createElement('KafkaSource', {
        topic: 'input',
        format: 'json',
        _nameHint: 'input',
        schema: {
          fields: { amount: 'BIGINT', name: 'STRING', event_time: 'TIMESTAMP(3)' },
          watermark: { column: 'event_time', expression: "event_time - INTERVAL '5' SECOND" },
          primaryKey: { columns: ['name'] },
          metadataColumns: [],
        },
      })
    )
  )
);

export default pipeline;
`
  }

  it("introspects schemas from a pipeline file path", async () => {
    writePipeline("orders", makeSimplePipeline())

    const schemas = await runSchema(
      join(tempDir, "pipelines/orders/index.tsx"),
      { projectDir: tempDir },
    )

    expect(schemas.length).toBeGreaterThanOrEqual(2)

    const sources = schemas.filter((s) => s.kind === "source")
    const sinks = schemas.filter((s) => s.kind === "sink")

    expect(sources).toHaveLength(1)
    expect(sinks).toHaveLength(1)

    // Source has PK and WM constraints
    const src = sources[0]
    expect(src.columns).toHaveLength(3)

    const nameField = src.columns.find((c) => c.name === "name")!
    expect(nameField.constraints).toContain("PK")

    const eventTimeField = src.columns.find((c) => c.name === "event_time")!
    expect(eventTimeField.constraints).toContain("WM")

    // Sink has resolved schema (same 3 fields, no constraints)
    expect(sinks[0].columns).toHaveLength(3)
  })

  it("resolves pipeline name to pipelines/<name>/index.tsx", async () => {
    writePipeline("my-pipeline", makeSimplePipeline())

    const schemas = await runSchema("my-pipeline", { projectDir: tempDir })

    expect(schemas.length).toBeGreaterThanOrEqual(2)
  })

  it("returns empty array and sets exit code for missing file", async () => {
    const schemas = await runSchema("nonexistent.tsx", { projectDir: tempDir })

    expect(schemas).toEqual([])
    expect(process.exitCode).toBe(1)

    // Reset for other tests
    process.exitCode = 0
  })

  it("produces JSON output with --json flag", async () => {
    writePipeline("json-test", makeSimplePipeline())

    const schemas = await runSchema(
      join(tempDir, "pipelines/json-test/index.tsx"),
      { json: true, projectDir: tempDir },
    )

    // JSON output goes to stdout — we just verify the return value is valid
    expect(schemas.length).toBeGreaterThanOrEqual(2)
    for (const schema of schemas) {
      expect(schema).toHaveProperty("nodeId")
      expect(schema).toHaveProperty("component")
      expect(schema).toHaveProperty("kind")
      expect(schema).toHaveProperty("nameHint")
      expect(schema).toHaveProperty("columns")
    }
  })

  it("resolves a standalone .tsx file outside pipelines/", async () => {
    writeFile("standalone.tsx", makeSimplePipeline())

    const schemas = await runSchema("standalone.tsx", { projectDir: tempDir })

    expect(schemas.length).toBeGreaterThanOrEqual(2)
  })
})

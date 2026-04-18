import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { afterEach, beforeEach, describe, expect, it } from "vitest"
import { runValidate } from "@/cli/commands/validate.js"

const projectRoot = resolve(__dirname, "../../../../")

describe("validate command", () => {
  let tempDir: string

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "flink-reactor-validate-"))
  })

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true })
  })

  function writePipeline(name: string, content: string): void {
    const dir = join(tempDir, "pipelines", name)
    mkdirSync(dir, { recursive: true })
    writeFileSync(join(dir, "index.tsx"), content, "utf-8")
  }

  function jsxPath(): string {
    return join(projectRoot, "src/core/jsx-runtime.js")
  }

  it("passes for a valid pipeline", { timeout: 15_000 }, async () => {
    writePipeline(
      "orders",
      `
import { createElement } from '${jsxPath()}';

const pipeline = createElement('Pipeline', { name: 'valid-pipeline' },
  createElement('KafkaSink', { topic: 'output', format: 'json', bootstrapServers: 'localhost:9092' },
    createElement('Filter', { condition: 'amount > 100' },
      createElement('KafkaSource', {
        topic: 'input',
        format: 'json',
        bootstrapServers: 'localhost:9092',
        schema: {
          fields: { amount: 'BIGINT' },
          metadataColumns: [],
        },
      })
    )
  )
);

export default pipeline;
`,
    )

    const result = await runValidate({
      projectDir: tempDir,
    })

    expect(result).toBe(true)
  })

  it(
    "reports errors for an invalid pipeline with orphan source",
    { timeout: 15_000 },
    async () => {
      writePipeline(
        "invalid",
        `
import { createElement } from '${jsxPath()}';

const pipeline = createElement('Pipeline', { name: 'invalid-pipeline' },
  createElement('KafkaSource', {
    topic: 'input',
    format: 'json',
    bootstrapServers: 'localhost:9092',
    schema: {
      fields: { amount: 'BIGINT' },
      metadataColumns: [],
    },
  })
);

export default pipeline;
`,
      )

      const result = await runValidate({
        projectDir: tempDir,
      })

      // Orphan source should cause validation failure
      expect(result).toBe(false)
    },
  )

  it("returns true when no pipelines found", async () => {
    const result = await runValidate({
      projectDir: tempDir,
    })

    expect(result).toBe(true)
  })
})

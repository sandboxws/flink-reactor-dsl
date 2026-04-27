import { existsSync, readFileSync, rmSync } from "node:fs"
import { join } from "node:path"
import { afterEach, describe, expect, it } from "vitest"
import type { TemplateName } from "@/cli/commands/new.js"
import { runValidate } from "@/cli/commands/validate.js"
import {
  EXPECTED_PIPELINES,
  scaffoldAndSynth,
} from "./helpers/scaffold-and-synth.js"

describe("scaffold → synth", () => {
  let tempRoot: string | null = null

  afterEach(() => {
    if (tempRoot) {
      rmSync(tempRoot, { recursive: true, force: true })
      tempRoot = null
    }
  })

  for (const [template, pipelines] of Object.entries(EXPECTED_PIPELINES) as [
    TemplateName,
    readonly string[],
  ][]) {
    it(
      `${template}: scaffolds and synthesizes ${pipelines.length} pipeline(s)`,
      { timeout: 30000 },
      async () => {
        const result = await scaffoldAndSynth(template)
        tempRoot = result.tempRoot
        const { artifacts } = result

        if (pipelines.length === 0) {
          expect(artifacts).toEqual([])
          return
        }

        expect(artifacts.map((a) => a.name).sort()).toEqual([...pipelines])

        for (const artifact of artifacts) {
          expect(artifact.sql.sql).toMatch(/CREATE TABLE/i)
          expect(artifact.sql.sql).toMatch(/INSERT INTO/i)
          expect(artifact.sql.statements.length).toBeGreaterThan(0)

          const projectDir = join(tempRoot, "app")
          for (const file of [
            "pipeline.sql",
            "deployment.yaml",
            "configmap.yaml",
          ]) {
            expect(
              existsSync(join(projectDir, "dist", artifact.name, file)),
            ).toBe(true)
          }

          const diskSql = readFileSync(
            join(projectDir, "dist", artifact.name, "pipeline.sql"),
            "utf-8",
          )
          expect(diskSql).toContain("CREATE TABLE")
        }

        // If codegen succeeded, `fr dev`'s validator must also pass — otherwise
        // users hit "Validation failed." on pipelines that synthesize just fine
        // (the exact mismatch that motivated this test).
        const projectDir = join(tempRoot, "app")
        const valid = await runValidate({ projectDir })
        expect(valid).toBe(true)
      },
    )
  }
})

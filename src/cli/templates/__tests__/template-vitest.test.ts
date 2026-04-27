import { rmSync } from "node:fs"
import { describe, expect, it } from "vitest"
import type { TemplateName } from "@/cli/commands/new.js"
import { scaffoldAndRunVitest } from "./helpers/scaffold-and-vitest.js"

// Regression coverage for the scaffold → `pnpm test` user journey. The
// in-repo `template-scaffold-synth.test.ts` only exercises `runSynth`
// (via jiti); a real scaffolded project goes through Vite/esbuild,
// which has different module-resolution rules and a different DSL
// surface (the published package, not src/). This test catches bugs
// that only surface in that path — the trio fixed in this branch
// (jsx-dev-runtime export, vitest.config @/ alias, synthesizeApp stub
// field name) all shipped because nothing exercised the real run.
//
// Two templates: starter (minimal — its only test is `it.todo()`, so we
// only assert vitest exits clean) and stock-basics (4 pipelines, exercises
// the @/ alias path; assert ≥4 tests passed). Adding more templates is
// one entry; budget ~5–10s per template.

interface TemplateCase {
  readonly template: TemplateName
  readonly minPassed: number
}

const TEMPLATES_TO_RUN: readonly TemplateCase[] = [
  { template: "starter", minPassed: 0 },
  { template: "stock-basics", minPassed: 4 },
]

describe("Scaffolded vitest run", () => {
  for (const { template, minPassed } of TEMPLATES_TO_RUN) {
    it(
      `${template} — vitest run passes in scaffolded project`,
      { timeout: 60_000 },
      async () => {
        const result = await scaffoldAndRunVitest(template)
        try {
          expect(
            result.exitCode,
            `vitest exited ${result.exitCode}\n` +
              `--- stdout ---\n${result.stdout}\n` +
              `--- stderr ---\n${result.stderr}`,
          ).toBe(0)

          if (minPassed > 0) {
            const match = result.stdout.match(/Test Files\s+(\d+)\s+passed/)
            expect(
              match,
              `expected "Test Files N passed" in stdout:\n${result.stdout}`,
            ).not.toBeNull()
            expect(Number(match?.[1])).toBeGreaterThanOrEqual(minPassed)
          }
        } finally {
          rmSync(result.tempRoot, { recursive: true, force: true })
        }
      },
    )
  }
})

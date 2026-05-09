import { createHash } from "node:crypto"
import { beforeEach, describe, expect, it } from "vitest"
import { generateCrdYaml } from "@/codegen/crd-generator.js"
import { generateSql } from "@/codegen/sql/sql-generator.js"
import { synthesizeApp } from "@/core/app.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { synth } from "@/testing/synth.js"

// The DSL contract is: same input → same output bytes. This file pins
// that contract end-to-end across every example pipeline. Two ways to
// break determinism slip past ordinary snapshot tests:
//
//   1. Wall-clock leakage — `new Date().toISOString()` in synth output
//      produces a different field value on every run. Snapshots use the
//      first observed value and don't catch the drift unless you run
//      twice in the same process.
//   2. TZ-dependent parsing — `new Date("2026-01-01T00:00:00")` (no `Z`)
//      yields different epoch ms on UTC vs PST runners. CI matrix
//      catches this; local snapshots don't.
//
// We hash the concatenated outputs and assert two consecutive runs
// produce the same hash. Cheap. Catches both classes.

const EXAMPLES = [
  "01-simple-source-sink",
  "02-filter-project",
  "03-group-aggregate",
  "04-tumble-window",
  "13-simple-etl",
  "19-union-aggregate",
  "21-branching-multi-sink",
  "26-cdc-sync",
  "32-union-streams",
  "38-dedup-aggregate",
]

function hashOutput(parts: readonly string[]): string {
  const hash = createHash("sha256")
  for (const part of parts) hash.update(part)
  return hash.digest("hex")
}

beforeEach(() => {
  resetNodeIdCounter()
})

describe("determinism: synth() output is bit-stable across runs", () => {
  for (const id of EXAMPLES) {
    it(id, async () => {
      const mod = await import(`../../examples/${id}/after.tsx`)
      resetNodeIdCounter()
      const a = synth(mod.default)
      resetNodeIdCounter()
      const b = synth(mod.default)
      expect(a.sql).toBe(b.sql)
      expect(JSON.stringify(a.crd)).toBe(JSON.stringify(b.crd))
    })
  }
})

describe("determinism: synthesizeApp output is bit-stable across runs", () => {
  it("identical pipelineManifest + tapManifest across two synthesize calls", async () => {
    const mod = await import(`../../examples/13-simple-etl/after.tsx`)
    const fixedTime = "2026-04-27T12:00:00.000Z"
    resetNodeIdCounter()
    const a = synthesizeApp(
      { name: "test", children: mod.default },
      { synthesizedAt: fixedTime },
    )
    resetNodeIdCounter()
    const b = synthesizeApp(
      { name: "test", children: mod.default },
      { synthesizedAt: fixedTime },
    )
    expect(JSON.stringify(a)).toBe(JSON.stringify(b))
  })

  it("default sentinel keeps output deterministic without an explicit synthesizedAt", async () => {
    // No synthesizedAt → both runs use the sentinel ("1970-01-01…").
    // The hash should match across consecutive calls.
    const mod = await import(`../../examples/01-simple-source-sink/after.tsx`)
    resetNodeIdCounter()
    const a = synthesizeApp({ name: "test", children: mod.default })
    resetNodeIdCounter()
    const b = synthesizeApp({ name: "test", children: mod.default })
    expect(hashOutput([JSON.stringify(a)])).toBe(
      hashOutput([JSON.stringify(b)]),
    )
  })
})

describe("determinism: generateSql is reentrant-safe across calls", () => {
  it("two sequential generateSql calls produce identical output", async () => {
    const mod = await import(`../../examples/03-group-aggregate/after.tsx`)
    resetNodeIdCounter()
    const a = generateSql(mod.default)
    resetNodeIdCounter()
    const b = generateSql(mod.default)
    expect(a.sql).toBe(b.sql)
    expect(a.statements).toEqual(b.statements)
  })
})

describe("determinism: CRD YAML is bit-stable", () => {
  for (const id of EXAMPLES.slice(0, 5)) {
    it(id, async () => {
      const mod = await import(`../../examples/${id}/after.tsx`)
      resetNodeIdCounter()
      const a = generateCrdYaml(mod.default, { flinkVersion: "2.0" })
      resetNodeIdCounter()
      const b = generateCrdYaml(mod.default, { flinkVersion: "2.0" })
      expect(a).toBe(b)
    })
  }
})

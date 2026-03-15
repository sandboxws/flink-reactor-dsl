import { describe, expect, it } from "vitest"
import { createRulesRegistry } from "../src/component-rules"
import { checkRuleParity, formatParityReport } from "../src/parity-check"

describe("checkRuleParity", () => {
  it("returns no mismatches for the default registry", () => {
    const registry = createRulesRegistry()
    const mismatches = checkRuleParity(registry)
    expect(mismatches).toEqual([])
  })

  it("detects unknown parent in rules", () => {
    const registry = createRulesRegistry({
      FakeComponent: ["Filter"],
    })
    const mismatches = checkRuleParity(registry)
    const unknownParent = mismatches.find(
      (m) =>
        m.type === "unknown_in_rules" && m.components.includes("FakeComponent"),
    )
    expect(unknownParent).toBeDefined()
    expect(unknownParent?.fix).toContain("component-inventory.ts")
  })

  it("detects unknown child in rules", () => {
    const registry = createRulesRegistry({
      Pipeline: [
        ...((createRulesRegistry().getAllowedChildren(
          "Pipeline",
        ) as string[]) ?? []),
        "NonExistentWidget",
      ],
    })
    const mismatches = checkRuleParity(registry)
    const unknownChild = mismatches.find(
      (m) =>
        m.type === "unknown_in_rules" &&
        m.components.includes("NonExistentWidget"),
    )
    expect(unknownChild).toBeDefined()
  })
})

describe("formatParityReport", () => {
  it("reports success when no mismatches", () => {
    const report = formatParityReport([])
    expect(report).toContain("passed")
  })

  it("formats mismatches with fix instructions", () => {
    const report = formatParityReport([
      {
        type: "missing_from_rules",
        components: ["NewSource"],
        message: "Source component(s) not in Pipeline rules: NewSource",
        fix: 'Add "NewSource" to Pipeline\'s allowed children',
      },
    ])
    expect(report).toContain("1 issue(s)")
    expect(report).toContain("NewSource")
    expect(report).toContain("Fix:")
  })
})

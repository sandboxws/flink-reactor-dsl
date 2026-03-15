/**
 * Parity tests: verify that plugin rule coverage matches the DSL
 * component inventory. Failures here mean a component was added to
 * the DSL or inventory without updating the plugin rules.
 */
import { describe, expect, it } from "vitest"
import {
  DSL_COMPONENTS,
  getComponentsByKind,
  HIERARCHY_ONLY_COMPONENTS,
} from "../src/component-inventory"
import { createRulesRegistry } from "../src/component-rules"

describe("rule parity with DSL component inventory", () => {
  const registry = createRulesRegistry()
  const allReferenced = registry.getAllReferencedComponents()
  const allRegisteredParents = registry.getRegisteredParents()

  it("every DSL source component appears in Pipeline's allowed children", () => {
    const sources = getComponentsByKind("Source")
    const pipelineChildren = registry.getAllowedChildren("Pipeline")
    expect(Array.isArray(pipelineChildren)).toBe(true)

    const missing = sources.filter(
      (s) => !(pipelineChildren as string[]).includes(s),
    )
    expect(
      missing,
      `Source components missing from Pipeline rules: ${missing.join(", ")}`,
    ).toEqual([])
  })

  it("every DSL sink component appears in Pipeline's allowed children", () => {
    const sinks = getComponentsByKind("Sink")
    const pipelineChildren = registry.getAllowedChildren("Pipeline")
    expect(Array.isArray(pipelineChildren)).toBe(true)

    const missing = sinks.filter(
      (s) => !(pipelineChildren as string[]).includes(s),
    )
    expect(
      missing,
      `Sink components missing from Pipeline rules: ${missing.join(", ")}`,
    ).toEqual([])
  })

  it("every DSL transform component appears in Pipeline's allowed children", () => {
    const transforms = getComponentsByKind("Transform")
    // Sub-components like SideOutput.Sink are not direct Pipeline children
    const topLevel = transforms.filter((t) => !t.includes("."))
    const pipelineChildren = registry.getAllowedChildren("Pipeline")
    expect(Array.isArray(pipelineChildren)).toBe(true)

    const missing = topLevel.filter(
      (t) => !(pipelineChildren as string[]).includes(t),
    )
    expect(
      missing,
      `Transform components missing from Pipeline rules: ${missing.join(", ")}`,
    ).toEqual([])
  })

  it("every DSL join component appears in Pipeline's allowed children", () => {
    const joins = getComponentsByKind("Join")
    const pipelineChildren = registry.getAllowedChildren("Pipeline")
    expect(Array.isArray(pipelineChildren)).toBe(true)

    const missing = joins.filter(
      (j) => !(pipelineChildren as string[]).includes(j),
    )
    expect(
      missing,
      `Join components missing from Pipeline rules: ${missing.join(", ")}`,
    ).toEqual([])
  })

  it("every DSL window component appears in Pipeline's allowed children", () => {
    const windows = getComponentsByKind("Window")
    const pipelineChildren = registry.getAllowedChildren("Pipeline")
    expect(Array.isArray(pipelineChildren)).toBe(true)

    const missing = windows.filter(
      (w) => !(pipelineChildren as string[]).includes(w),
    )
    expect(
      missing,
      `Window components missing from Pipeline rules: ${missing.join(", ")}`,
    ).toEqual([])
  })

  it("every hierarchy-only sub-component is referenced in rules", () => {
    const missing = HIERARCHY_ONLY_COMPONENTS.filter(
      (c) => !allReferenced.includes(c),
    )
    expect(
      missing,
      `Hierarchy-only components missing from rules: ${missing.join(", ")}`,
    ).toEqual([])
  })

  it("every parent with explicit rules is a known component", () => {
    const allKnown = new Set([
      ...DSL_COMPONENTS.keys(),
      ...HIERARCHY_ONLY_COMPONENTS,
    ])
    const unknown = allRegisteredParents.filter((p) => !allKnown.has(p))
    expect(
      unknown,
      `Parents in rules not found in inventory: ${unknown.join(", ")}`,
    ).toEqual([])
  })

  it("every child referenced in rules is a known component", () => {
    const allKnown = new Set([
      ...DSL_COMPONENTS.keys(),
      ...HIERARCHY_ONLY_COMPONENTS,
    ])
    const unknownChildren: string[] = []
    for (const parent of allRegisteredParents) {
      const children = registry.getAllowedChildren(parent)
      if (Array.isArray(children)) {
        for (const child of children) {
          if (!allKnown.has(child)) {
            unknownChildren.push(`${parent} → ${child}`)
          }
        }
      }
    }
    expect(
      unknownChildren,
      `Children in rules not found in inventory: ${unknownChildren.join(", ")}`,
    ).toEqual([])
  })

  it("DSL_COMPONENTS inventory matches expected count", () => {
    // This test serves as a tripwire: if someone adds a component to the
    // DSL inventory, this count changes and forces a review of rule coverage.
    expect(DSL_COMPONENTS.size).toBe(48)
  })
})

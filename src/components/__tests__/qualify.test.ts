import { beforeEach, describe, expect, it } from "vitest"
import { Qualify } from "@/components/qualify.js"
import { createElement, resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { SynthContext } from "@/core/synth-context.js"

beforeEach(() => {
  resetNodeIdCounter()
})

function makeSource(name: string) {
  return createElement("KafkaSource", { topic: name })
}

describe("Qualify", () => {
  it("creates a Qualify node with condition prop", () => {
    const source = makeSource("events")
    const node = Qualify({
      condition: "rn = 1",
      children: source,
    })

    expect(node.kind).toBe("Qualify")
    expect(node.component).toBe("Qualify")
    expect(node.props.condition).toBe("rn = 1")
    expect(node.children).toHaveLength(1)
  })

  it("creates a Qualify node with window prop", () => {
    const source = makeSource("events")
    const node = Qualify({
      condition: "rn <= 3",
      window:
        "ROW_NUMBER() OVER (PARTITION BY `region` ORDER BY `revenue` DESC) AS rn",
      children: source,
    })

    expect(node.props.condition).toBe("rn <= 3")
    expect(node.props.window).toBe(
      "ROW_NUMBER() OVER (PARTITION BY `region` ORDER BY `revenue` DESC) AS rn",
    )
  })

  it("throws when condition is missing", () => {
    expect(() => {
      Qualify({ condition: "" })
    }).toThrow("Qualify requires a 'condition' prop")
  })

  it("participates in DAG validation", () => {
    const source = makeSource("events")
    const qualify = Qualify({
      condition: "rn = 1",
      children: source,
    })
    const sink = createElement("KafkaSink", { topic: "output" }, qualify)
    const pipeline = createElement("Pipeline", { name: "test" }, sink)

    const ctx = new SynthContext()
    ctx.buildFromTree(pipeline)
    const diagnostics = ctx.validate(pipeline)

    // Should have no Qualify-specific errors (orphan sources may appear due to test wiring)
    const qualifyErrors = diagnostics.filter(
      (d) => d.severity === "error" && d.component === "Qualify",
    )
    expect(qualifyErrors).toHaveLength(0)
  })

  it("validation catches missing condition on Qualify node", () => {
    // Directly create a Qualify node without condition via createElement
    const source = makeSource("events")
    const qualifyNode = createElement("Qualify", {}, source)
    const sink = createElement("KafkaSink", { topic: "output" }, qualifyNode)
    const pipeline = createElement("Pipeline", { name: "test" }, sink)

    const ctx = new SynthContext()
    ctx.buildFromTree(pipeline)
    const diagnostics = ctx.validate(pipeline)

    const qualifyErrors = diagnostics.filter(
      (d) => d.component === "Qualify" && d.severity === "error",
    )
    expect(qualifyErrors).toHaveLength(1)
    expect(qualifyErrors[0].message).toContain("condition")
  })
})

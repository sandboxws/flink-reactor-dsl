import { beforeEach, describe, expect, it } from "vitest"
import { createElement, resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { SynthContext } from "@/core/synth-context.js"
import type { ConstructNode } from "@/core/types.js"

beforeEach(() => {
  resetNodeIdCounter()
})

function makeNode(
  component: string,
  kind: ConstructNode["kind"],
): ConstructNode {
  return { id: component, kind, component, props: {}, children: [] }
}

describe("SynthContext graph model", () => {
  it("registers nodes and edges", () => {
    const ctx = new SynthContext()
    const source = makeNode("src", "Source")
    const sink = makeNode("snk", "Sink")

    ctx.addNode(source)
    ctx.addNode(sink)
    ctx.addEdge("src", "snk")

    expect(ctx.getNode("src")).toBe(source)
    expect(ctx.getNode("snk")).toBe(sink)
    expect([...ctx.getOutgoing("src")]).toEqual(["snk"])
    expect([...ctx.getIncoming("snk")]).toEqual(["src"])
  })

  it("collects nodes by kind", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("s1", "Source"))
    ctx.addNode(makeNode("s2", "Source"))
    ctx.addNode(makeNode("f1", "Transform"))
    ctx.addNode(makeNode("k1", "Sink"))

    expect(ctx.getNodesByKind("Source")).toHaveLength(2)
    expect(ctx.getNodesByKind("Transform")).toHaveLength(1)
    expect(ctx.getNodesByKind("Source", "Sink")).toHaveLength(3)
  })

  it("returns all edges", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("a", "Source"))
    ctx.addNode(makeNode("b", "Transform"))
    ctx.addNode(makeNode("c", "Sink"))
    ctx.addEdge("a", "b")
    ctx.addEdge("b", "c")

    const edges = ctx.getAllEdges()
    expect(edges).toHaveLength(2)
    expect(edges).toContainEqual({ from: "a", to: "b" })
    expect(edges).toContainEqual({ from: "b", to: "c" })
  })
})

describe("SynthContext.buildFromTree", () => {
  it("registers nodes and edges from a construct tree", () => {
    const sink = createElement("KafkaSink", { topic: "out" })
    const filter = createElement("Filter", { predicate: "x > 0" }, sink)
    const source = createElement("KafkaSource", { topic: "in" }, filter)

    const ctx = new SynthContext()
    ctx.buildFromTree(source)

    expect(ctx.getAllNodes()).toHaveLength(3)
    const edges = ctx.getAllEdges()
    expect(edges).toHaveLength(2)
  })
})

describe("topological sort", () => {
  it("returns nodes in dependency order", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("source", "Source"))
    ctx.addNode(makeNode("filter", "Transform"))
    ctx.addNode(makeNode("sink", "Sink"))
    ctx.addEdge("source", "filter")
    ctx.addEdge("filter", "sink")

    const sorted = ctx.topologicalSort()
    const ids = sorted.map((n) => n.id)

    expect(ids.indexOf("source")).toBeLessThan(ids.indexOf("filter"))
    expect(ids.indexOf("filter")).toBeLessThan(ids.indexOf("sink"))
  })

  it("handles fan-out topology", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("source", "Source"))
    ctx.addNode(makeNode("sink_a", "Sink"))
    ctx.addNode(makeNode("sink_b", "Sink"))
    ctx.addEdge("source", "sink_a")
    ctx.addEdge("source", "sink_b")

    const sorted = ctx.topologicalSort()
    const ids = sorted.map((n) => n.id)

    expect(ids.indexOf("source")).toBeLessThan(ids.indexOf("sink_a"))
    expect(ids.indexOf("source")).toBeLessThan(ids.indexOf("sink_b"))
  })

  it("throws on cycle", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("a", "Transform"))
    ctx.addNode(makeNode("b", "Transform"))
    ctx.addEdge("a", "b")
    ctx.addEdge("b", "a")

    expect(() => ctx.topologicalSort()).toThrow("Cycle detected")
  })
})

describe("orphan source detection", () => {
  it("detects source with no outgoing edges", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("orphan", "Source"))

    const diagnostics = ctx.detectOrphanSources()
    expect(diagnostics).toHaveLength(1)
    expect(diagnostics[0].severity).toBe("error")
    expect(diagnostics[0].message).toContain("Orphan source")
    expect(diagnostics[0].nodeId).toBe("orphan")
  })

  it("does not flag connected sources", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("src", "Source"))
    ctx.addNode(makeNode("snk", "Sink"))
    ctx.addEdge("src", "snk")

    expect(ctx.detectOrphanSources()).toHaveLength(0)
  })
})

describe("dangling sink detection", () => {
  it("detects sink with no incoming edges", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("dangling", "Sink"))

    const diagnostics = ctx.detectDanglingSinks()
    expect(diagnostics).toHaveLength(1)
    expect(diagnostics[0].severity).toBe("error")
    expect(diagnostics[0].message).toContain("Dangling sink")
    expect(diagnostics[0].nodeId).toBe("dangling")
  })

  it("does not flag connected sinks", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("src", "Source"))
    ctx.addNode(makeNode("snk", "Sink"))
    ctx.addEdge("src", "snk")

    expect(ctx.detectDanglingSinks()).toHaveLength(0)
  })
})

describe("cycle detection", () => {
  it("detects a simple two-node cycle", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("a", "Transform"))
    ctx.addNode(makeNode("b", "Transform"))
    ctx.addEdge("a", "b")
    ctx.addEdge("b", "a")

    const diagnostics = ctx.detectCycles()
    expect(diagnostics).toHaveLength(1)
    expect(diagnostics[0].severity).toBe("error")
    expect(diagnostics[0].message).toContain("Cycle detected")
  })

  it("detects a three-node cycle", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("a", "Transform"))
    ctx.addNode(makeNode("b", "Transform"))
    ctx.addNode(makeNode("c", "Transform"))
    ctx.addEdge("a", "b")
    ctx.addEdge("b", "c")
    ctx.addEdge("c", "a")

    const diagnostics = ctx.detectCycles()
    expect(diagnostics).toHaveLength(1)
  })

  it("returns empty for acyclic graph", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("a", "Source"))
    ctx.addNode(makeNode("b", "Transform"))
    ctx.addNode(makeNode("c", "Sink"))
    ctx.addEdge("a", "b")
    ctx.addEdge("b", "c")

    expect(ctx.detectCycles()).toHaveLength(0)
  })
})

describe("validate (combined)", () => {
  it("returns zero diagnostics for a valid pipeline", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("src", "Source"))
    ctx.addNode(makeNode("xform", "Transform"))
    ctx.addNode(makeNode("snk", "Sink"))
    ctx.addEdge("src", "xform")
    ctx.addEdge("xform", "snk")

    expect(ctx.validate()).toHaveLength(0)
  })

  it("catches multiple issues in one pass", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("orphan_src", "Source"))
    ctx.addNode(makeNode("dangling_snk", "Sink"))

    const diagnostics = ctx.validate()
    expect(diagnostics.length).toBeGreaterThanOrEqual(2)
    expect(diagnostics.some((d) => d.message.includes("Orphan"))).toBe(true)
    expect(diagnostics.some((d) => d.message.includes("Dangling"))).toBe(true)
  })
})

import { beforeEach, describe, expect, it } from "vitest"
import {
  computeChangelogModes,
  validateChangelogModes,
} from "@/core/changelog-propagation.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { SynthContext } from "@/core/synth-context.js"
import type { ConstructNode, NodeKind } from "@/core/types.js"

beforeEach(() => {
  resetNodeIdCounter()
})

// ── Helpers ─────────────────────────────────────────────────────────

function makeNode(
  overrides: Partial<ConstructNode> & { id: string; component: string },
): ConstructNode {
  return {
    kind: "Transform" as NodeKind,
    props: {},
    children: [],
    ...overrides,
  }
}

function makeSource(
  id: string,
  changelogMode: "append-only" | "retract" = "append-only",
): ConstructNode {
  return makeNode({
    id,
    kind: "Source",
    component: "KafkaSource",
    props: { changelogMode },
  })
}

function makeSink(
  id: string,
  component = "KafkaSink",
  props: Record<string, unknown> = {},
): ConstructNode {
  return makeNode({
    id,
    kind: "Sink",
    component,
    props,
  })
}

// ── Computation tests ───────────────────────────────────────────────

describe("computeChangelogModes", () => {
  it("produces retract for unbounded Aggregate", () => {
    const ctx = new SynthContext()
    const source = makeSource("src", "append-only")
    const agg = makeNode({
      id: "agg",
      kind: "Transform",
      component: "Aggregate",
      props: { groupBy: ["user_id"], select: { cnt: "COUNT(*)" } },
    })
    const sink = makeSink("sink")

    ctx.addNode(source)
    ctx.addNode(agg)
    ctx.addNode(sink)
    ctx.addEdge(source.id, agg.id)
    ctx.addEdge(agg.id, sink.id)

    const modes = computeChangelogModes(ctx)
    expect(modes.get("src")).toBe("append-only")
    expect(modes.get("agg")).toBe("retract")
    expect(modes.get("sink")).toBe("retract")
  })

  it("produces append-only for windowed Aggregate", () => {
    const ctx = new SynthContext()
    const source = makeSource("src", "append-only")
    const window = makeNode({
      id: "win",
      kind: "Window",
      component: "TumbleWindow",
      props: { size: "1 hour", on: "event_time" },
    })
    const agg = makeNode({
      id: "agg",
      kind: "Transform",
      component: "Aggregate",
      props: { groupBy: ["user_id"], select: { cnt: "COUNT(*)" } },
    })
    const sink = makeSink("sink")

    ctx.addNode(source)
    ctx.addNode(window)
    ctx.addNode(agg)
    ctx.addNode(sink)
    ctx.addEdge(source.id, window.id)
    ctx.addEdge(window.id, agg.id)
    ctx.addEdge(agg.id, sink.id)

    const modes = computeChangelogModes(ctx)
    expect(modes.get("agg")).toBe("append-only")
    expect(modes.get("sink")).toBe("append-only")
  })

  it("produces append-only for Deduplicate", () => {
    const ctx = new SynthContext()
    const source = makeSource("src", "retract")
    const dedup = makeNode({
      id: "dedup",
      kind: "Transform",
      component: "Deduplicate",
      props: { key: ["user_id"], order: "event_time", keep: "first" },
    })
    const sink = makeSink("sink", "PaimonSink")

    ctx.addNode(source)
    ctx.addNode(dedup)
    ctx.addNode(sink)
    ctx.addEdge(source.id, dedup.id)
    ctx.addEdge(dedup.id, sink.id)

    const modes = computeChangelogModes(ctx)
    expect(modes.get("dedup")).toBe("append-only")
    expect(modes.get("sink")).toBe("append-only")
  })

  it("passes through changelog mode for Filter", () => {
    const ctx = new SynthContext()
    const source = makeSource("src", "retract")
    const filter = makeNode({
      id: "filter",
      component: "Filter",
      props: { condition: "amount > 100" },
    })
    const sink = makeSink("sink", "PaimonSink")

    ctx.addNode(source)
    ctx.addNode(filter)
    ctx.addNode(sink)
    ctx.addEdge(source.id, filter.id)
    ctx.addEdge(filter.id, sink.id)

    const modes = computeChangelogModes(ctx)
    expect(modes.get("filter")).toBe("retract")
  })

  it("passes through changelog mode for Map", () => {
    const ctx = new SynthContext()
    const source = makeSource("src", "retract")
    const map = makeNode({
      id: "map",
      component: "Map",
      props: { select: { total: "amount * quantity" } },
    })

    ctx.addNode(source)
    ctx.addNode(map)
    ctx.addEdge(source.id, map.id)

    const modes = computeChangelogModes(ctx)
    expect(modes.get("map")).toBe("retract")
  })

  it("produces retract for Union with mixed modes", () => {
    const ctx = new SynthContext()
    const src1 = makeSource("src1", "append-only")
    const src2 = makeSource("src2", "retract")
    const union = makeNode({
      id: "union",
      component: "Union",
    })
    const sink = makeSink("sink", "PaimonSink")

    ctx.addNode(src1)
    ctx.addNode(src2)
    ctx.addNode(union)
    ctx.addNode(sink)
    ctx.addEdge(src1.id, union.id)
    ctx.addEdge(src2.id, union.id)
    ctx.addEdge(union.id, sink.id)

    const modes = computeChangelogModes(ctx)
    expect(modes.get("union")).toBe("retract")
  })

  it("produces append-only for Union with all append-only inputs", () => {
    const ctx = new SynthContext()
    const src1 = makeSource("src1", "append-only")
    const src2 = makeSource("src2", "append-only")
    const union = makeNode({
      id: "union",
      component: "Union",
    })

    ctx.addNode(src1)
    ctx.addNode(src2)
    ctx.addNode(union)
    ctx.addEdge(src1.id, union.id)
    ctx.addEdge(src2.id, union.id)

    const modes = computeChangelogModes(ctx)
    expect(modes.get("union")).toBe("append-only")
  })

  describe("Join changelog mode", () => {
    it("produces append-only for join with all append-only inputs", () => {
      const ctx = new SynthContext()
      const left = makeSource("left", "append-only")
      const right = makeSource("right", "append-only")
      const join = makeNode({
        id: "join",
        kind: "Join",
        component: "Join",
        props: { on: "a.id = b.id", type: "inner" },
      })

      ctx.addNode(left)
      ctx.addNode(right)
      ctx.addNode(join)
      ctx.addEdge(left.id, join.id)
      ctx.addEdge(right.id, join.id)

      const modes = computeChangelogModes(ctx)
      expect(modes.get("join")).toBe("append-only")
    })

    it("produces retract for join with retract input", () => {
      const ctx = new SynthContext()
      const left = makeSource("left", "retract")
      const right = makeSource("right", "append-only")
      const join = makeNode({
        id: "join",
        kind: "Join",
        component: "Join",
        props: { on: "a.id = b.id", type: "inner" },
      })

      ctx.addNode(left)
      ctx.addNode(right)
      ctx.addNode(join)
      ctx.addEdge(left.id, join.id)
      ctx.addEdge(right.id, join.id)

      const modes = computeChangelogModes(ctx)
      expect(modes.get("join")).toBe("retract")
    })

    it("produces retract for join with both retract inputs", () => {
      const ctx = new SynthContext()
      const left = makeSource("left", "retract")
      const right = makeSource("right", "retract")
      const join = makeNode({
        id: "join",
        kind: "Join",
        component: "Join",
        props: { on: "a.id = b.id", type: "full" },
      })

      ctx.addNode(left)
      ctx.addNode(right)
      ctx.addNode(join)
      ctx.addEdge(left.id, join.id)
      ctx.addEdge(right.id, join.id)

      const modes = computeChangelogModes(ctx)
      expect(modes.get("join")).toBe("retract")
    })
  })
})

// ── Validation tests ────────────────────────────────────────────────

describe("validateChangelogModes", () => {
  it("errors when append-only sink receives retract stream", () => {
    const ctx = new SynthContext()
    const source = makeSource("src", "retract")
    const sink = makeSink("sink", "KafkaSink")

    ctx.addNode(source)
    ctx.addNode(sink)
    ctx.addEdge(source.id, sink.id)

    const diagnostics = validateChangelogModes(ctx)
    expect(diagnostics).toHaveLength(1)
    expect(diagnostics[0].severity).toBe("error")
    expect(diagnostics[0].message).toContain("retract")
    expect(diagnostics[0].message).toContain("KafkaSink")
    expect(diagnostics[0].nodeId).toBe("sink")
    expect(diagnostics[0].category).toBe("changelog")
  })

  it("accepts retract stream at upsert-capable sink", () => {
    const ctx = new SynthContext()
    const source = makeSource("src", "retract")
    const sink = makeSink("sink", "PaimonSink")

    ctx.addNode(source)
    ctx.addNode(sink)
    ctx.addEdge(source.id, sink.id)

    const diagnostics = validateChangelogModes(ctx)
    expect(diagnostics).toHaveLength(0)
  })

  it("detects intermediate changelog mismatch (not just sinks)", () => {
    const ctx = new SynthContext()
    const source = makeSource("src", "retract")
    const dedup = makeNode({
      id: "dedup",
      kind: "Transform",
      component: "Deduplicate",
      props: { key: ["user_id"], order: "event_time", keep: "first" },
    })
    const sink = makeSink("sink", "KafkaSink")

    ctx.addNode(source)
    ctx.addNode(dedup)
    ctx.addNode(sink)
    ctx.addEdge(source.id, dedup.id)
    ctx.addEdge(dedup.id, sink.id)

    const diagnostics = validateChangelogModes(ctx)
    // Should warn about Deduplicate receiving retract input
    const deduplicateWarning = diagnostics.find(
      (d) => d.nodeId === "dedup" && d.category === "changelog",
    )
    expect(deduplicateWarning).toBeDefined()
    expect(deduplicateWarning?.severity).toBe("warning")
    expect(deduplicateWarning?.message).toContain("retract")
  })

  it("propagates retract through Filter to append-only sink", () => {
    const ctx = new SynthContext()
    const source = makeSource("src", "retract")
    const filter = makeNode({
      id: "filter",
      component: "Filter",
      props: { condition: "amount > 100" },
    })
    const sink = makeSink("sink", "KafkaSink")

    ctx.addNode(source)
    ctx.addNode(filter)
    ctx.addNode(sink)
    ctx.addEdge(source.id, filter.id)
    ctx.addEdge(filter.id, sink.id)

    const diagnostics = validateChangelogModes(ctx)
    const sinkError = diagnostics.find(
      (d) => d.nodeId === "sink" && d.severity === "error",
    )
    expect(sinkError).toBeDefined()
    expect(sinkError?.message).toContain("retract")
  })

  it("no errors for all append-only pipeline", () => {
    const ctx = new SynthContext()
    const source = makeSource("src", "append-only")
    const filter = makeNode({
      id: "filter",
      component: "Filter",
      props: { condition: "x > 1" },
    })
    const sink = makeSink("sink", "KafkaSink")

    ctx.addNode(source)
    ctx.addNode(filter)
    ctx.addNode(sink)
    ctx.addEdge(source.id, filter.id)
    ctx.addEdge(filter.id, sink.id)

    const diagnostics = validateChangelogModes(ctx)
    expect(diagnostics).toHaveLength(0)
  })
})

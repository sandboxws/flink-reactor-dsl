import { beforeEach, describe, expect, it } from "vitest"
import { Route } from "@/components/route.js"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { SynthContext } from "@/core/synth-context.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const LogSchema = Schema({
  fields: {
    message: Field.STRING(),
    level: Field.STRING(),
    timestamp: Field.TIMESTAMP(3),
  },
})

// ── 4.6 Route splits to multiple downstream sinks ──────────────────

describe("Route", () => {
  it("splits a stream into multiple downstream paths", () => {
    const errorSink = KafkaSink({ topic: "errors" })
    const warnSink = KafkaSink({ topic: "warnings" })
    const otherSink = KafkaSink({ topic: "other" })

    const errorBranch = Route.Branch({
      condition: "level = 'ERROR'",
      children: errorSink,
    })
    const warnBranch = Route.Branch({
      condition: "level = 'WARN'",
      children: warnSink,
    })
    const defaultBranch = Route.Default({ children: otherSink })

    const route = Route({
      children: [errorBranch, warnBranch, defaultBranch],
    })

    expect(route.kind).toBe("Transform")
    expect(route.component).toBe("Route")
    expect(route.children).toHaveLength(3)
    expect(route.children[0].component).toBe("Route.Branch")
    expect(route.children[0].props.condition).toBe("level = 'ERROR'")
    expect(route.children[1].component).toBe("Route.Branch")
    expect(route.children[2].component).toBe("Route.Default")
  })

  it("builds a valid DAG with source → route → sinks", () => {
    const errorSink = KafkaSink({ topic: "errors" })
    const warnSink = KafkaSink({ topic: "warnings" })

    const errorBranch = Route.Branch({
      condition: "level = 'ERROR'",
      children: errorSink,
    })
    const warnBranch = Route.Branch({
      condition: "level = 'WARN'",
      children: warnSink,
    })

    const route = Route({ children: [errorBranch, warnBranch] })
    const source = KafkaSource({
      topic: "logs",
      schema: LogSchema,
      children: route,
    })

    const ctx = new SynthContext()
    ctx.buildFromTree(source)

    // source → route → branch1 → sink1, branch2 → sink2
    expect(ctx.getAllNodes()).toHaveLength(6)
    expect(ctx.getAllEdges()).toHaveLength(5)
  })

  it("throws when Route has no Branch children", () => {
    const sink = KafkaSink({ topic: "out" })

    expect(() => Route({ children: sink })).toThrow(
      "Route requires at least one Route.Branch child",
    )
  })

  it("throws when Route has only a Default child", () => {
    const sink = KafkaSink({ topic: "out" })
    const defaultBranch = Route.Default({ children: sink })

    expect(() => Route({ children: defaultBranch })).toThrow(
      "Route requires at least one Route.Branch child",
    )
  })
})

describe("Route.Branch", () => {
  it("stores condition and wraps downstream children", () => {
    const sink = KafkaSink({ topic: "errors" })
    const branch = Route.Branch({
      condition: "level = 'ERROR'",
      children: sink,
    })

    expect(branch.component).toBe("Route.Branch")
    expect(branch.props.condition).toBe("level = 'ERROR'")
    expect(branch.children).toHaveLength(1)
    expect(branch.children[0].component).toBe("KafkaSink")
  })
})

describe("Route.Default", () => {
  it("wraps downstream children without a condition", () => {
    const sink = KafkaSink({ topic: "other" })
    const def = Route.Default({ children: sink })

    expect(def.component).toBe("Route.Default")
    expect(def.children).toHaveLength(1)
    expect(def.props).not.toHaveProperty("condition")
  })
})

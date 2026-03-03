import { beforeEach, describe, expect, it } from "vitest"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { Aggregate } from "@/components/transforms.js"
import {
  SessionWindow,
  SlideWindow,
  TumbleWindow,
} from "@/components/windows.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { SynthContext } from "@/core/synth-context.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const EventSchema = Schema({
  fields: {
    userId: Field.STRING(),
    pageUrl: Field.STRING(),
    viewTimestamp: Field.TIMESTAMP(3),
  },
})

// ── 4.7 TumbleWindow wraps Aggregate child ──────────────────────────

describe("TumbleWindow", () => {
  it("wraps an Aggregate child for windowed aggregation", () => {
    const aggregate = Aggregate({
      groupBy: ["userId"],
      select: {
        userId: "userId",
        viewCount: "COUNT(*)",
        windowStart: "WINDOW_START",
        windowEnd: "WINDOW_END",
      },
    })

    const window = TumbleWindow({
      size: "1 hour",
      on: "viewTimestamp",
      children: aggregate,
    })

    expect(window.kind).toBe("Window")
    expect(window.component).toBe("TumbleWindow")
    expect(window.props.size).toBe("1 hour")
    expect(window.props.on).toBe("viewTimestamp")
    expect(window.children).toHaveLength(1)
    expect(window.children[0].component).toBe("Aggregate")
  })

  it("builds a valid DAG with source → window → aggregate → sink", () => {
    const sink = KafkaSink({ topic: "hourly-stats" })
    const aggregate = Aggregate({
      groupBy: ["userId"],
      select: { viewCount: "COUNT(*)" },
      children: sink,
    })
    const window = TumbleWindow({
      size: "1 hour",
      on: "viewTimestamp",
      children: aggregate,
    })
    const source = KafkaSource({
      topic: "page-views",
      schema: EventSchema,
      children: window,
    })

    const ctx = new SynthContext()
    ctx.buildFromTree(source)

    expect(ctx.getAllNodes()).toHaveLength(4)
    expect(ctx.getAllEdges()).toHaveLength(3)

    const sorted = ctx.topologicalSort()
    expect(sorted.map((n) => n.component)).toEqual([
      "KafkaSource",
      "TumbleWindow",
      "Aggregate",
      "KafkaSink",
    ])
    expect(ctx.validate()).toHaveLength(0)
  })

  it("stores parallelism on window node", () => {
    const window = TumbleWindow({
      size: "5 minutes",
      on: "ts",
      parallelism: 16,
    })

    expect(window.props.parallelism).toBe(16)
  })
})

// ── 4.8 SlideWindow with size and slide ─────────────────────────────

describe("SlideWindow", () => {
  it("stores size and slide on construct node", () => {
    const window = SlideWindow({
      size: "1 hour",
      slide: "15 minutes",
      on: "viewTimestamp",
    })

    expect(window.kind).toBe("Window")
    expect(window.component).toBe("SlideWindow")
    expect(window.props.size).toBe("1 hour")
    expect(window.props.slide).toBe("15 minutes")
    expect(window.props.on).toBe("viewTimestamp")
  })

  it("wraps Aggregate child for sliding window aggregation", () => {
    const aggregate = Aggregate({
      groupBy: ["pageUrl"],
      select: {
        pageUrl: "pageUrl",
        hitCount: "COUNT(*)",
      },
    })

    const window = SlideWindow({
      size: "1 hour",
      slide: "5 minutes",
      on: "viewTimestamp",
      children: aggregate,
    })

    expect(window.children).toHaveLength(1)
    expect(window.children[0].component).toBe("Aggregate")
  })
})

// ── 4.9 SessionWindow with gap ──────────────────────────────────────

describe("SessionWindow", () => {
  it("stores gap duration on construct node", () => {
    const window = SessionWindow({
      gap: "30 minutes",
      on: "viewTimestamp",
    })

    expect(window.kind).toBe("Window")
    expect(window.component).toBe("SessionWindow")
    expect(window.props.gap).toBe("30 minutes")
    expect(window.props.on).toBe("viewTimestamp")
  })

  it("wraps Aggregate child for session window aggregation", () => {
    const aggregate = Aggregate({
      groupBy: ["userId"],
      select: {
        userId: "userId",
        sessionPages: "COUNT(*)",
        sessionDuration:
          "TIMESTAMPDIFF(SECOND, MIN(viewTimestamp), MAX(viewTimestamp))",
      },
    })

    const window = SessionWindow({
      gap: "10 minutes",
      on: "viewTimestamp",
      children: aggregate,
    })

    expect(window.children).toHaveLength(1)
    expect(window.children[0].props.select).toHaveProperty("sessionDuration")
  })
})

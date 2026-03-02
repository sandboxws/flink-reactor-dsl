import { beforeEach, describe, expect, it } from "vitest"
import { createElement, resetNodeIdCounter } from "../../core/jsx-runtime.js"
import { SynthContext } from "../../core/synth-context.js"
import { MatchRecognize } from "../cep.js"

beforeEach(() => {
  resetNodeIdCounter()
})

function makeSource(name: string) {
  return createElement("KafkaSource", { topic: name })
}

describe("MatchRecognize", () => {
  it("creates a CEP node with pattern metadata", () => {
    const events = makeSource("events")

    const cep = MatchRecognize({
      input: events,
      pattern: "A B+ C",
      define: {
        A: "A.event_type = 'start'",
        B: "B.amount > 100",
        C: "C.event_type = 'end'",
      },
      measures: {
        start_time: "A.event_time",
        end_time: "C.event_time",
        total: "SUM(B.amount)",
      },
      partitionBy: ["user_id"],
      orderBy: "event_time",
      after: "MATCH_RECOGNIZED",
    })

    expect(cep.kind).toBe("CEP")
    expect(cep.component).toBe("MatchRecognize")
    expect(cep.props.pattern).toBe("A B+ C")
    expect(cep.props.define).toEqual({
      A: "A.event_type = 'start'",
      B: "B.amount > 100",
      C: "C.event_type = 'end'",
    })
    expect(cep.props.measures).toEqual({
      start_time: "A.event_time",
      end_time: "C.event_time",
      total: "SUM(B.amount)",
    })
    expect(cep.props.partitionBy).toEqual(["user_id"])
    expect(cep.props.orderBy).toBe("event_time")
    expect(cep.props.after).toBe("MATCH_RECOGNIZED")
    expect(cep.props.input).toBe(events.id)
    expect(cep.children).toHaveLength(1)
  })

  it("wires correctly into the DAG", () => {
    const source = makeSource("transactions")

    const cep = MatchRecognize({
      input: source,
      pattern: "A B",
      define: { A: "A.amount > 1000", B: "B.amount > 1000" },
      measures: { first_time: "A.ts", second_time: "B.ts" },
    })

    const ctx = new SynthContext()
    ctx.buildFromTree(cep)
    expect(ctx.getAllNodes()).toHaveLength(2)
    expect(ctx.getAllEdges()).toHaveLength(1)
  })

  it("throws when input is missing", () => {
    expect(() =>
      MatchRecognize({
        input: undefined as unknown as ReturnType<typeof makeSource>,
        pattern: "A B",
        define: { A: "true" },
        measures: { x: "A.x" },
      }),
    ).toThrow("MatchRecognize requires an input stream")
  })

  it("throws when pattern is missing", () => {
    const source = makeSource("events")
    expect(() =>
      MatchRecognize({
        input: source,
        pattern: "",
        define: { A: "true" },
        measures: { x: "A.x" },
      }),
    ).toThrow("MatchRecognize requires a pattern")
  })

  it("throws when define is empty", () => {
    const source = makeSource("events")
    expect(() =>
      MatchRecognize({
        input: source,
        pattern: "A B",
        define: {},
        measures: { x: "A.x" },
      }),
    ).toThrow("MatchRecognize requires at least one DEFINE clause")
  })

  it("throws when measures is empty", () => {
    const source = makeSource("events")
    expect(() =>
      MatchRecognize({
        input: source,
        pattern: "A B",
        define: { A: "true" },
        measures: {},
      }),
    ).toThrow("MatchRecognize requires at least one MEASURES expression")
  })
})

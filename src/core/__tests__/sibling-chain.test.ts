import { beforeEach, describe, expect, it } from "vitest"
import { generateSql } from "@/codegen/sql-generator.js"
import { Pipeline } from "@/components/pipeline.js"
import { Route } from "@/components/route.js"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { StatementSet } from "@/components/statement-set.js"
import { Filter } from "@/components/transforms.js"
import { createElement, resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { resolveSiblingChains } from "@/core/sibling-chain.js"
import type { ConstructNode } from "@/core/types.js"

const TestSchema = Schema({
  fields: {
    id: Field.BIGINT(),
    tier: Field.STRING(),
  },
})

beforeEach(() => {
  resetNodeIdCounter()
})

// These tests pin the contract between two pieces of code that walk
// "preceding-sibling" topology: `resolveSiblingChains` (used by the
// validator to decide whether a sink is wired up) and the codegen-side
// `buildSiblingChainQuery` (which actually emits the SQL). If the rules
// drift apart, validation says "all sinks bound" while codegen falls back
// to `SELECT * FROM unknown`. The comment in `sibling-chain.ts:17–18`
// promises the two agree; this test enforces it.

function pipeline(...children: ConstructNode[]): ConstructNode {
  return createElement(Pipeline, { name: "test-pipeline" }, ...children)
}

function expectSinkCodegenIsBound(root: ConstructNode, sinkId: string): void {
  const sql = generateSql(root).sql
  expect(
    sql,
    `sink ${sinkId} should not fall back to SELECT * FROM unknown`,
  ).not.toMatch(/FROM\s+unknown/i)
}

describe("sibling-chain resolution agrees with codegen", () => {
  it("Source → Sink: resolution and codegen both bind", () => {
    const root = pipeline(
      KafkaSource({ topic: "in", schema: TestSchema }),
      KafkaSink({ topic: "out" }),
    )
    const { sinkToSource } = resolveSiblingChains(root)
    const [source, sink] = root.children
    expect(sinkToSource.get(sink.id)).toBe(source.id)
    expectSinkCodegenIsBound(root, sink.id)
  })

  it("Source → Filter → Sink: chain skips through Transforms", () => {
    const root = pipeline(
      KafkaSource({ topic: "in", schema: TestSchema }),
      Filter({ condition: "id > 0" }),
      KafkaSink({ topic: "out" }),
    )
    const { sinkToSource } = resolveSiblingChains(root)
    const source = root.children[0]
    const sink = root.children[2]
    expect(sinkToSource.get(sink.id)).toBe(source.id)
    expectSinkCodegenIsBound(root, sink.id)
  })

  it("StatementSet pairs each sink with its nearest preceding Source", () => {
    const root = createElement(
      StatementSet,
      {},
      KafkaSource({ topic: "a", schema: TestSchema }),
      KafkaSink({ topic: "a-out" }),
      KafkaSource({ topic: "b", schema: TestSchema }),
      KafkaSink({ topic: "b-out" }),
    )
    const wrapped = pipeline(root)
    const { sinkToSource } = resolveSiblingChains(wrapped)

    // The two sinks should bind to their respective preceding sources,
    // not both to the first one.
    const ss = wrapped.children[0]
    const [srcA, sinkA, srcB, sinkB] = ss.children
    expect(sinkToSource.get(sinkA.id)).toBe(srcA.id)
    expect(sinkToSource.get(sinkB.id)).toBe(srcB.id)
    expectSinkCodegenIsBound(wrapped, sinkA.id)
    expectSinkCodegenIsBound(wrapped, sinkB.id)
  })

  it("Route binds every branch sink to the preceding source", () => {
    const root = pipeline(
      KafkaSource({ topic: "in", schema: TestSchema }),
      createElement(
        Route,
        {},
        createElement(
          Route.Branch,
          { when: "tier = 'gold'" },
          KafkaSink({ topic: "gold-out" }),
        ),
        createElement(Route.Default, {}, KafkaSink({ topic: "default-out" })),
      ),
    )
    const { sinkToSource, sourceToSinks } = resolveSiblingChains(root)
    const source = root.children[0]
    const route = root.children[1]
    const goldSink = route.children[0].children[0]
    const defaultSink = route.children[1].children[0]
    expect(sinkToSource.get(goldSink.id)).toBe(source.id)
    expect(sinkToSource.get(defaultSink.id)).toBe(source.id)
    expect(sourceToSinks.get(source.id)?.size).toBe(2)
  })

  it("orphan sink (no preceding source) is not bound by either side", () => {
    const root = pipeline(KafkaSink({ topic: "orphan-out" }))
    const { sinkToSource } = resolveSiblingChains(root)
    const sink = root.children[0]
    expect(sinkToSource.has(sink.id)).toBe(false)
    // codegen should fall back; this test only asserts the resolver
    // doesn't claim it's bound when codegen can't actually wire it up.
  })
})

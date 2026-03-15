import { Effect, Either } from "effect"
import { beforeEach, describe, expect, it } from "vitest"
import { generateCrdEither } from "@/codegen/crd-generator.js"
import { generateSqlEither } from "@/codegen/sql-generator.js"
import { Pipeline } from "@/components/pipeline.js"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { Filter } from "@/components/transforms.js"
import { synthesizeAppEffect } from "@/core/app.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { invokeHookEither } from "@/core/plugin-registry.js"
import { Field, Schema } from "@/core/schema.js"
import { SynthContext } from "@/core/synth-context.js"
import type { ConstructNode } from "@/core/types.js"

beforeEach(() => {
  resetNodeIdCounter()
})

function makeNode(
  component: string,
  kind: ConstructNode["kind"],
  props: Record<string, unknown> = {},
  children: ConstructNode[] = [],
): ConstructNode {
  return { id: component, kind, component, props, children }
}

const TestSchema = Schema({
  fields: {
    id: Field.BIGINT(),
    value: Field.STRING(),
  },
})

function buildPipeline() {
  const sink = KafkaSink({
    topic: "output",
    bootstrapServers: "kafka:9092",
    format: "json",
    schema: TestSchema,
  })
  const filter = Filter({ condition: "`id` > 0" }, sink)
  const source = KafkaSource(
    {
      topic: "input",
      bootstrapServers: "kafka:9092",
      format: "json",
      schema: TestSchema,
    },
    filter,
  )
  return Pipeline({ name: "test-pipeline" }, source)
}

// ── Task 8: topologicalSort Either path ──────────────────────────────

describe("topologicalSortEither", () => {
  it("returns Right for a valid DAG", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("source", "Source"))
    ctx.addNode(makeNode("filter", "Transform"))
    ctx.addNode(makeNode("sink", "Sink"))
    ctx.addEdge("source", "filter")
    ctx.addEdge("filter", "sink")

    const result = ctx.topologicalSortEither()
    expect(Either.isRight(result)).toBe(true)

    if (Either.isRight(result)) {
      const ids = result.right.map((n) => n.id)
      expect(ids.indexOf("source")).toBeLessThan(ids.indexOf("filter"))
      expect(ids.indexOf("filter")).toBeLessThan(ids.indexOf("sink"))
    }
  })

  it("returns Left(CycleDetectedError) for a cycle", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("a", "Transform"))
    ctx.addNode(makeNode("b", "Transform"))
    ctx.addEdge("a", "b")
    ctx.addEdge("b", "a")

    const result = ctx.topologicalSortEither()
    expect(Either.isLeft(result)).toBe(true)

    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("CycleDetectedError")
      expect(result.left.message).toContain("Cycle detected")
    }
  })

  it("returns Left with nodeIds for cycle participants", () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("x", "Transform"))
    ctx.addNode(makeNode("y", "Transform"))
    ctx.addNode(makeNode("z", "Transform"))
    ctx.addEdge("x", "y")
    ctx.addEdge("y", "z")
    ctx.addEdge("z", "x")

    const result = ctx.topologicalSortEither()
    expect(Either.isLeft(result)).toBe(true)

    if (Either.isLeft(result)) {
      expect(result.left.nodeIds.length).toBeGreaterThan(0)
    }
  })
})

// ── Task 9: validate Either path ─────────────────────────────────────

describe("validateEither", () => {
  it("returns warnings for a valid tree", async () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("src", "Source"))
    ctx.addNode(makeNode("snk", "Sink"))
    ctx.addEdge("src", "snk")

    const warnings = await Effect.runPromise(ctx.validateEither())
    expect(warnings).toHaveLength(0)
  })

  it("fails with ValidationError for orphan source", async () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("orphan", "Source"))

    const exit = await Effect.runPromiseExit(ctx.validateEither())
    expect(exit._tag).toBe("Failure")

    if (exit._tag === "Failure") {
      const cause = exit.cause
      // Extract the error from the cause
      const error = (
        cause as {
          _tag: string
          error?: { _tag: string; diagnostics: Array<{ message: string }> }
        }
      ).error
      expect(error?._tag).toBe("ValidationError")
      expect(error?.diagnostics.length).toBeGreaterThan(0)
      expect(
        error?.diagnostics.some((d: { message: string }) =>
          d.message.includes("Orphan"),
        ),
      ).toBe(true)
    }
  })

  it("fails with ValidationError for dangling sink", async () => {
    const ctx = new SynthContext()
    ctx.addNode(makeNode("dangling", "Sink"))

    const exit = await Effect.runPromiseExit(ctx.validateEither())
    expect(exit._tag).toBe("Failure")

    if (exit._tag === "Failure") {
      const cause = exit.cause
      const error = (
        cause as {
          _tag: string
          error?: { _tag: string; diagnostics: Array<{ message: string }> }
        }
      ).error
      expect(error?._tag).toBe("ValidationError")
      expect(
        error?.diagnostics.some((d: { message: string }) =>
          d.message.includes("Dangling"),
        ),
      ).toBe(true)
    }
  })
})

// ── Task 10: generateSql Either path ─────────────────────────────────

describe("generateSqlEither", () => {
  it("returns Right for valid codegen", () => {
    const pipeline = buildPipeline()

    const result = generateSqlEither(pipeline)
    expect(Either.isRight(result)).toBe(true)

    if (Either.isRight(result)) {
      expect(result.right.sql).toBeTruthy()
      expect(result.right.statements.length).toBeGreaterThan(0)
    }
  })

  it("captures errors as Left(SqlGenerationError)", () => {
    // A pipeline node with null children to trigger an error
    const badNode: ConstructNode = {
      id: "bad",
      kind: "Pipeline",
      component: "Pipeline",
      props: { name: "bad-pipeline" },
      children: null as unknown as ConstructNode[],
    }

    const result = generateSqlEither(badNode)
    expect(Either.isLeft(result)).toBe(true)

    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("SqlGenerationError")
      expect(result.left.component).toBe("Pipeline")
    }
  })
})

// ── Task 11: generateCrd Either path ─────────────────────────────────

describe("generateCrdEither", () => {
  it("returns Right for valid CRD", () => {
    const pipeline = buildPipeline()

    const result = generateCrdEither(pipeline, { flinkVersion: "2.0" })
    expect(Either.isRight(result)).toBe(true)

    if (Either.isRight(result)) {
      expect(result.right.kind).toBe("FlinkDeployment")
      expect(result.right.metadata.name).toBe("test-pipeline")
    }
  })

  it("returns Right even with minimal props (CRD has defaults)", () => {
    const pipeline: ConstructNode = {
      id: "p1",
      kind: "Pipeline",
      component: "Pipeline",
      props: { name: "minimal" },
      children: [],
    }

    const result = generateCrdEither(pipeline, { flinkVersion: "2.0" })
    expect(Either.isRight(result)).toBe(true)

    if (Either.isRight(result)) {
      expect(result.right.metadata.name).toBe("minimal")
    }
  })
})

// ── Task 12: synthesizeAppEffect ─────────────────────────────────────

describe("synthesizeAppEffect", () => {
  it("full success: produces AppSynthResult", async () => {
    const pipeline = buildPipeline()

    const effect = synthesizeAppEffect(
      { name: "test-app", children: pipeline },
      { flinkVersion: "2.0" },
    )

    const result = await Effect.runPromise(effect)
    expect(result.appName).toBe("test-app")
    expect(result.pipelines).toHaveLength(1)
    expect(result.pipelines[0].name).toBe("test-pipeline")
    expect(result.pipelines[0].sql.sql).toBeTruthy()
    expect(result.pipelines[0].crd.kind).toBe("FlinkDeployment")
  })

  it("plugin hook failure: surfaces PluginError", async () => {
    const pipeline = buildPipeline()
    const failingPlugin = {
      name: "failing-plugin",
      beforeSynth: () => {
        throw new Error("plugin exploded")
      },
    }

    const effect = synthesizeAppEffect(
      { name: "test-app", children: pipeline },
      { flinkVersion: "2.0", plugins: [failingPlugin] },
    )

    const exit = await Effect.runPromiseExit(effect)
    expect(exit._tag).toBe("Failure")
  })

  it("empty app: succeeds with no pipelines", async () => {
    const effect = synthesizeAppEffect(
      { name: "empty-app" },
      { flinkVersion: "2.0" },
    )

    const result = await Effect.runPromise(effect)
    expect(result.appName).toBe("empty-app")
    expect(result.pipelines).toHaveLength(0)
  })
})

// ── invokeHookEither ─────────────────────────────────────────────────

describe("invokeHookEither", () => {
  it("returns Right for successful hook", () => {
    const result = invokeHookEither("my-plugin", "beforeSynth", () => 42)
    expect(Either.isRight(result)).toBe(true)

    if (Either.isRight(result)) {
      expect(result.right).toBe(42)
    }
  })

  it("returns Left(PluginError) for throwing hook", () => {
    const result = invokeHookEither("my-plugin", "beforeSynth", () => {
      throw new Error("hook crashed")
    })
    expect(Either.isLeft(result)).toBe(true)

    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("PluginError")
      expect(result.left.reason).toBe("hook_failure")
      expect(result.left.pluginName).toBe("my-plugin")
      expect(result.left.hookName).toBe("beforeSynth")
      expect(result.left.message).toContain("hook crashed")
    }
  })
})

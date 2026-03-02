import { beforeEach, describe, expect, it } from "vitest"
import { synthesizeApp } from "@/core/app.js"
import { defineConfig } from "@/core/config.js"
import {
  createElement,
  resetComponentKinds,
  resetNodeIdCounter,
} from "@/core/jsx-runtime.js"
import type { FlinkReactorPlugin } from "@/core/plugin.js"
import { Field, Schema } from "@/core/schema.js"
import type { ConstructNode } from "@/core/types.js"
import { synth, validate } from "@/testing/index.js"

beforeEach(() => {
  resetNodeIdCounter()
  resetComponentKinds()
})

const testSchema = Schema({
  fields: {
    id: Field.BIGINT(),
    name: Field.STRING(),
    amount: Field.DOUBLE(),
  },
})

function buildTestPipeline(): ConstructNode {
  return createElement(
    "Pipeline",
    { name: "test-pipeline" },
    createElement(
      "KafkaSink",
      { topic: "output", bootstrapServers: "kafka:9092" },
      createElement(
        "Filter",
        { condition: "amount > 0" },
        createElement("KafkaSource", {
          topic: "input",
          bootstrapServers: "kafka:9092",
          schema: testSchema,
        }),
      ),
    ),
  )
}

describe("plugin integration with synthesizeApp", () => {
  it("runs synthesis without plugins (baseline)", () => {
    const pipeline = buildTestPipeline()
    const result = synthesizeApp({ name: "test-app", children: pipeline })

    expect(result.pipelines).toHaveLength(1)
    expect(result.pipelines[0].sql.sql).toContain("CREATE TABLE")
    expect(result.pipelines[0].sql.sql).toContain("INSERT INTO")
  })

  it("applies tree transformer plugin", () => {
    const addParallelismPlugin: FlinkReactorPlugin = {
      name: "add-parallelism",
      transformTree: (tree) => ({
        ...tree,
        props: { ...tree.props, parallelism: 4 },
      }),
    }

    const pipeline = buildTestPipeline()
    const result = synthesizeApp(
      { name: "test-app", children: pipeline },
      { plugins: [addParallelismPlugin] },
    )

    expect(result.pipelines[0].crd.spec.job.parallelism).toBe(4)
  })

  it("applies CRD transformer plugin", () => {
    const labelsPlugin: FlinkReactorPlugin = {
      name: "labels",
      transformCrd: (crd) => ({
        ...crd,
        metadata: {
          ...crd.metadata,
          labels: {
            ...crd.metadata.labels,
            "app.kubernetes.io/managed-by": "flink-reactor",
          },
        },
      }),
    }

    const pipeline = buildTestPipeline()
    const result = synthesizeApp(
      { name: "test-app", children: pipeline },
      { plugins: [labelsPlugin] },
    )

    expect(result.pipelines[0].crd.metadata.labels).toEqual({
      "app.kubernetes.io/managed-by": "flink-reactor",
    })
  })

  it("calls beforeSynth and afterSynth hooks", () => {
    const hookLog: string[] = []

    const lifecyclePlugin: FlinkReactorPlugin = {
      name: "lifecycle",
      beforeSynth: (ctx) => {
        hookLog.push(`before:${ctx.appName}`)
      },
      afterSynth: (ctx) => {
        hookLog.push(`after:${ctx.appName}:${ctx.results.length}`)
      },
    }

    const pipeline = buildTestPipeline()
    synthesizeApp(
      { name: "my-app", children: pipeline },
      { plugins: [lifecyclePlugin] },
    )

    expect(hookLog).toEqual(["before:my-app", "after:my-app:1"])
  })

  it("respects plugin ordering in tree transformers", () => {
    const log: string[] = []

    const pluginA: FlinkReactorPlugin = {
      name: "alpha",
      transformTree: (tree) => {
        log.push("alpha")
        return tree
      },
    }

    const pluginB: FlinkReactorPlugin = {
      name: "beta",
      ordering: { after: ["alpha"] },
      transformTree: (tree) => {
        log.push("beta")
        return tree
      },
    }

    const pipeline = buildTestPipeline()
    synthesizeApp(
      { name: "test-app", children: pipeline },
      { plugins: [pluginB, pluginA] }, // Intentionally reversed
    )

    expect(log).toEqual(["alpha", "beta"])
  })

  it("merges plugins from config and options", () => {
    const log: string[] = []

    const configPlugin: FlinkReactorPlugin = {
      name: "from-config",
      beforeSynth: () => {
        log.push("config")
      },
    }

    const optionsPlugin: FlinkReactorPlugin = {
      name: "from-options",
      beforeSynth: () => {
        log.push("options")
      },
    }

    const config = defineConfig({
      flink: { version: "2.0" },
      plugins: [configPlugin],
    })

    const pipeline = buildTestPipeline()
    synthesizeApp(
      { name: "test-app", children: pipeline },
      { config, plugins: [optionsPlugin] },
    )

    expect(log).toEqual(["config", "options"])
  })

  it("registers and uses custom component kinds", () => {
    const customPlugin: FlinkReactorPlugin = {
      name: "custom-source",
      components: new Map([["RedisSource", "Source"]]),
      sqlGenerators: new Map([
        ["RedisSource", (node) => `SELECT * FROM redis_table_${node.id}`],
      ]),
      ddlGenerators: new Map([
        [
          "RedisSource",
          (node) =>
            `CREATE TABLE \`${node.id}\` WITH (\n  'connector' = 'redis'\n);`,
        ],
      ]),
    }

    // Build pipeline using the custom component
    const pipeline = createElement(
      "Pipeline",
      { name: "redis-pipeline" },
      createElement(
        "KafkaSink",
        { topic: "output", bootstrapServers: "kafka:9092" },
        createElement("RedisSource", { key: "my-key" }),
      ),
    )

    const result = synthesizeApp(
      { name: "test-app", children: pipeline },
      { plugins: [customPlugin] },
    )

    expect(result.pipelines[0].sql.sql).toContain("'connector' = 'redis'")
    expect(result.pipelines[0].sql.sql).toContain("SELECT * FROM redis_table_")
  })

  it("produces deterministic output across multiple runs", () => {
    const plugin: FlinkReactorPlugin = {
      name: "determinism-test",
      transformCrd: (crd) => ({
        ...crd,
        metadata: { ...crd.metadata, labels: { test: "determinism" } },
      }),
    }

    // Run twice with fresh ID counters
    resetNodeIdCounter()
    const pipeline1 = buildTestPipeline()
    const result1 = synthesizeApp(
      { name: "test-app", children: pipeline1 },
      { plugins: [plugin] },
    )

    resetNodeIdCounter()
    const pipeline2 = buildTestPipeline()
    const result2 = synthesizeApp(
      { name: "test-app", children: pipeline2 },
      { plugins: [plugin] },
    )

    expect(result1.pipelines[0].sql.sql).toBe(result2.pipelines[0].sql.sql)
    expect(result1.pipelines[0].crd).toEqual(result2.pipelines[0].crd)
  })
})

describe("plugin integration with synth() test helper", () => {
  it("applies plugins through synth()", () => {
    const plugin: FlinkReactorPlugin = {
      name: "synth-test",
      transformCrd: (crd) => ({
        ...crd,
        metadata: { ...crd.metadata, labels: { via: "synth-helper" } },
      }),
    }

    const pipeline = buildTestPipeline()
    const result = synth(pipeline, { plugins: [plugin] })

    expect(result.crd.metadata.labels).toEqual({ via: "synth-helper" })
    expect(result.sql).toContain("INSERT INTO")
  })

  it("synth() works without plugins (backward compatible)", () => {
    const pipeline = buildTestPipeline()
    const result = synth(pipeline)

    expect(result.sql).toContain("CREATE TABLE")
    expect(result.crd.metadata.name).toBe("test-pipeline")
  })
})

describe("plugin integration with validate() test helper", () => {
  it("applies plugin validators through validate()", () => {
    const warnPlugin: FlinkReactorPlugin = {
      name: "warn-plugin",
      validate: (_tree, _existing) => {
        return [
          {
            severity: "warning",
            message: "Plugin warning: consider adding error handling",
          },
        ]
      },
    }

    const pipeline = buildTestPipeline()
    const result = validate(pipeline, { plugins: [warnPlugin] })

    expect(result.warnings).toHaveLength(1)
    expect(result.warnings[0].message).toContain("Plugin warning")
  })

  it("validate() works without plugins (backward compatible)", () => {
    // Build tree in source→sink order (matching SynthContext graph direction)
    const pipeline = createElement(
      "Pipeline",
      { name: "valid" },
      createElement(
        "KafkaSource",
        { topic: "in", schema: testSchema },
        createElement(
          "Filter",
          { condition: "amount > 0" },
          createElement("KafkaSink", {
            topic: "out",
            bootstrapServers: "kafka:9092",
          }),
        ),
      ),
    )
    const result = validate(pipeline)

    expect(result.errors).toHaveLength(0)
  })
})

describe("plugin error handling", () => {
  it("cleans up component registrations even when synthesis throws", () => {
    const failPlugin: FlinkReactorPlugin = {
      name: "fail-plugin",
      components: new Map([["FailSource", "Source"]]),
      transformTree: () => {
        throw new Error("intentional failure")
      },
    }

    const pipeline = buildTestPipeline()

    expect(() =>
      synthesizeApp(
        { name: "test-app", children: pipeline },
        { plugins: [failPlugin] },
      ),
    ).toThrow("intentional failure")

    // After failure, component kinds should be reset
    // Verify by building a new pipeline — FailSource should default to 'Transform'
    const node = createElement("FailSource", { key: "test" })
    expect(node.kind).toBe("Transform") // Not 'Source' — registration was cleaned up
  })
})

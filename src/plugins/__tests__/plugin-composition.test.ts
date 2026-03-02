import { beforeEach, describe, expect, it } from "vitest"
import { synthesizeApp } from "@/core/app.js"
import {
  createElement,
  resetComponentKinds,
  resetNodeIdCounter,
} from "@/core/jsx-runtime.js"
import { resolvePlugins } from "@/core/plugin-registry.js"
import { Field, Schema } from "@/core/schema.js"
import type { ConstructNode } from "@/core/types.js"
import { errorHandlingPlugin } from "@/plugins/error-handling-plugin.js"
import { loggingPlugin } from "@/plugins/logging-plugin.js"
import { metricsPlugin } from "@/plugins/metrics-plugin.js"
import { synth } from "@/testing/index.js"

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

function buildPipeline(): ConstructNode {
  return createElement(
    "Pipeline",
    { name: "composed-test" },
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

describe("all three built-in plugins composed", () => {
  const allPlugins = [
    loggingPlugin({ level: "DEBUG" }),
    metricsPlugin({ reporters: [{ type: "prometheus" }] }),
    errorHandlingPlugin({
      restartStrategy: { type: "fixed-delay", attempts: 5 },
    }),
  ]

  it("resolves plugins in deterministic order", () => {
    const chain = resolvePlugins(allPlugins)

    // Logging declares before:metrics, error-handling declares after:[logging, metrics]
    expect(chain.order).toEqual([
      "flink-reactor:logging",
      "flink-reactor:metrics",
      "flink-reactor:error-handling",
    ])
  })

  it("resolves in same order regardless of input order", () => {
    const reversed = [...allPlugins].reverse()
    const chain = resolvePlugins(reversed)

    expect(chain.order).toEqual([
      "flink-reactor:logging",
      "flink-reactor:metrics",
      "flink-reactor:error-handling",
    ])
  })

  it("combines all CRD configuration keys", () => {
    const pipeline = buildPipeline()
    const result = synth(pipeline, { plugins: allPlugins })
    const config = result.crd.spec.flinkConfiguration

    // From logging plugin
    expect(config["rootLogger.level"]).toBe("DEBUG")

    // From metrics plugin
    expect(config["metrics.reporter.prometheus.factory.class"]).toBe(
      "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
    )
    expect(config["metrics.reporter.prometheus.port"]).toBe("9249")

    // From error-handling plugin
    expect(config["restart-strategy.type"]).toBe("fixed-delay")
    expect(config["restart-strategy.fixed-delay.attempts"]).toBe("5")
  })

  it("combines all metadata annotations", () => {
    const pipeline = buildPipeline()
    const result = synth(pipeline, { plugins: allPlugins })
    const annotations = result.crd.metadata.annotations!

    // From logging plugin
    expect(annotations["flink-reactor.io/logging-tag"]).toBe("pipeline-log")

    // From metrics plugin
    expect(annotations["flink-reactor.io/metric-reporters"]).toBe("prometheus")

    // From error-handling plugin
    expect(annotations["flink-reactor.io/restart-strategy"]).toBe("fixed-delay")
  })

  it("preserves SQL output (no SQL modifications from any plugin)", () => {
    const pipeline = buildPipeline()
    const withPlugins = synth(pipeline, { plugins: allPlugins })

    resetNodeIdCounter()
    const pipeline2 = buildPipeline()
    const withoutPlugins = synth(pipeline2)

    expect(withPlugins.sql).toBe(withoutPlugins.sql)
  })

  it("works through synthesizeApp", () => {
    const pipeline = buildPipeline()
    const result = synthesizeApp(
      { name: "test-app", children: pipeline },
      { plugins: allPlugins },
    )

    expect(result.pipelines).toHaveLength(1)
    const config = result.pipelines[0].crd.spec.flinkConfiguration

    // Verify all three plugins contributed
    expect(config["rootLogger.level"]).toBe("DEBUG")
    expect(config["metrics.reporter.prometheus.factory.class"]).toBeDefined()
    expect(config["restart-strategy.type"]).toBe("fixed-delay")
  })

  it("produces deterministic output across runs", () => {
    resetNodeIdCounter()
    const p1 = buildPipeline()
    const r1 = synth(p1, { plugins: allPlugins })

    resetNodeIdCounter()
    const p2 = buildPipeline()
    const r2 = synth(p2, { plugins: allPlugins })

    expect(r1.sql).toBe(r2.sql)
    expect(r1.crd).toEqual(r2.crd)
  })
})

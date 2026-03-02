import { beforeEach, describe, expect, it } from "vitest"
import { synthesizeApp } from "../../core/app.js"
import {
  createElement,
  resetComponentKinds,
  resetNodeIdCounter,
} from "../../core/jsx-runtime.js"
import { Field, Schema } from "../../core/schema.js"
import type { ConstructNode } from "../../core/types.js"
import { synth, validate } from "../../testing/index.js"
import { errorHandlingPlugin } from "../error-handling-plugin.js"

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

// Pipeline with NO error handling
function buildUnprotectedPipeline(): ConstructNode {
  return createElement(
    "Pipeline",
    { name: "unprotected" },
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

// Pipeline WITH Validate error handling
function buildProtectedPipeline(): ConstructNode {
  return createElement(
    "Pipeline",
    { name: "protected" },
    createElement(
      "KafkaSink",
      { topic: "output", bootstrapServers: "kafka:9092" },
      createElement(
        "Validate",
        {
          rules: { notNull: ["id", "name"] },
        },
        createElement("KafkaSource", {
          topic: "input",
          bootstrapServers: "kafka:9092",
          schema: testSchema,
        }),
      ),
    ),
  )
}

describe("errorHandlingPlugin", () => {
  it("has the correct plugin name", () => {
    const plugin = errorHandlingPlugin()
    expect(plugin.name).toBe("flink-reactor:error-handling")
    expect(plugin.version).toBe("0.1.0")
  })

  describe("validation", () => {
    it("warns when sinks have no upstream error handling", () => {
      const plugin = errorHandlingPlugin({ warnOnUnprotectedSinks: true })
      const pipeline = buildUnprotectedPipeline()

      const result = validate(pipeline, { plugins: [plugin] })

      const pluginWarnings = result.warnings.filter((w) =>
        w.message.includes("no upstream error handling"),
      )
      expect(pluginWarnings.length).toBeGreaterThanOrEqual(1)
      expect(pluginWarnings[0].component).toBe("KafkaSink")
    })

    it("does not warn when sinks have upstream Validate", () => {
      const plugin = errorHandlingPlugin({ warnOnUnprotectedSinks: true })
      const pipeline = buildProtectedPipeline()

      const result = validate(pipeline, { plugins: [plugin] })

      const pluginWarnings = result.warnings.filter((w) =>
        w.message.includes("no upstream error handling"),
      )
      expect(pluginWarnings).toHaveLength(0)
    })

    it("can be disabled", () => {
      const plugin = errorHandlingPlugin({ warnOnUnprotectedSinks: false })
      const pipeline = buildUnprotectedPipeline()

      const result = validate(pipeline, { plugins: [plugin] })

      const pluginWarnings = result.warnings.filter((w) =>
        w.message.includes("no upstream error handling"),
      )
      expect(pluginWarnings).toHaveLength(0)
    })

    it("supports custom guard components", () => {
      const plugin = errorHandlingPlugin({
        guardComponents: ["Filter"], // Treat Filter as a guard
      })
      const pipeline = buildUnprotectedPipeline() // Has a Filter

      const result = validate(pipeline, { plugins: [plugin] })

      // Filter is treated as a guard, so no warning
      const pluginWarnings = result.warnings.filter((w) =>
        w.message.includes("no upstream error handling"),
      )
      expect(pluginWarnings).toHaveLength(0)
    })
  })

  describe("CRD transformer — fixed-delay", () => {
    it("configures default fixed-delay strategy", () => {
      const plugin = errorHandlingPlugin()
      const pipeline = buildUnprotectedPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      const config = result.crd.spec.flinkConfiguration

      expect(config["restart-strategy.type"]).toBe("fixed-delay")
      expect(config["restart-strategy.fixed-delay.attempts"]).toBe("3")
      expect(config["restart-strategy.fixed-delay.delay"]).toBe("10s")
    })

    it("configures custom fixed-delay strategy", () => {
      const plugin = errorHandlingPlugin({
        restartStrategy: {
          type: "fixed-delay",
          attempts: 10,
          delay: "30s",
        },
      })
      const pipeline = buildUnprotectedPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      const config = result.crd.spec.flinkConfiguration

      expect(config["restart-strategy.fixed-delay.attempts"]).toBe("10")
      expect(config["restart-strategy.fixed-delay.delay"]).toBe("30s")
    })
  })

  describe("CRD transformer — failure-rate", () => {
    it("configures failure-rate strategy with defaults", () => {
      const plugin = errorHandlingPlugin({
        restartStrategy: { type: "failure-rate" },
      })
      const pipeline = buildUnprotectedPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      const config = result.crd.spec.flinkConfiguration

      expect(config["restart-strategy.type"]).toBe("failure-rate")
      expect(
        config["restart-strategy.failure-rate.max-failures-per-interval"],
      ).toBe("3")
      expect(
        config["restart-strategy.failure-rate.failure-rate-interval"],
      ).toBe("5m")
      expect(config["restart-strategy.failure-rate.delay"]).toBe("10s")
    })

    it("configures failure-rate strategy with custom values", () => {
      const plugin = errorHandlingPlugin({
        restartStrategy: {
          type: "failure-rate",
          maxFailuresPerInterval: 5,
          failureRateInterval: "10m",
          delay: "15s",
        },
      })
      const pipeline = buildUnprotectedPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      const config = result.crd.spec.flinkConfiguration

      expect(
        config["restart-strategy.failure-rate.max-failures-per-interval"],
      ).toBe("5")
      expect(
        config["restart-strategy.failure-rate.failure-rate-interval"],
      ).toBe("10m")
      expect(config["restart-strategy.failure-rate.delay"]).toBe("15s")
    })
  })

  describe("CRD transformer — exponential-delay", () => {
    it("configures exponential-delay strategy with defaults", () => {
      const plugin = errorHandlingPlugin({
        restartStrategy: { type: "exponential-delay" },
      })
      const pipeline = buildUnprotectedPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      const config = result.crd.spec.flinkConfiguration

      expect(config["restart-strategy.type"]).toBe("exponential-delay")
      expect(config["restart-strategy.exponential-delay.initial-backoff"]).toBe(
        "1s",
      )
      expect(config["restart-strategy.exponential-delay.max-backoff"]).toBe(
        "5m",
      )
      expect(
        config["restart-strategy.exponential-delay.backoff-multiplier"],
      ).toBe("2")
      expect(
        config["restart-strategy.exponential-delay.reset-backoff-threshold"],
      ).toBe("10m")
      expect(config["restart-strategy.exponential-delay.jitter-factor"]).toBe(
        "0.1",
      )
    })

    it("configures exponential-delay with custom values", () => {
      const plugin = errorHandlingPlugin({
        restartStrategy: {
          type: "exponential-delay",
          initialBackoff: "2s",
          maxBackoff: "10m",
          backoffMultiplier: 3.0,
          resetBackoffThreshold: "30m",
          jitter: 0.2,
        },
      })
      const pipeline = buildUnprotectedPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      const config = result.crd.spec.flinkConfiguration

      expect(config["restart-strategy.exponential-delay.initial-backoff"]).toBe(
        "2s",
      )
      expect(config["restart-strategy.exponential-delay.max-backoff"]).toBe(
        "10m",
      )
      expect(
        config["restart-strategy.exponential-delay.backoff-multiplier"],
      ).toBe("3")
      expect(
        config["restart-strategy.exponential-delay.reset-backoff-threshold"],
      ).toBe("30m")
      expect(config["restart-strategy.exponential-delay.jitter-factor"]).toBe(
        "0.2",
      )
    })
  })

  describe("metadata annotations", () => {
    it("adds restart strategy annotation", () => {
      const plugin = errorHandlingPlugin({
        restartStrategy: { type: "failure-rate" },
      })
      const pipeline = buildUnprotectedPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      expect(
        result.crd.metadata.annotations?.["flink-reactor.io/restart-strategy"],
      ).toBe("failure-rate")
    })
  })

  describe("integration", () => {
    it("works through synthesizeApp", () => {
      const plugin = errorHandlingPlugin({
        restartStrategy: { type: "fixed-delay", attempts: 5 },
      })
      const pipeline = buildUnprotectedPipeline()

      const result = synthesizeApp(
        { name: "test-app", children: pipeline },
        { plugins: [plugin] },
      )

      expect(result.pipelines).toHaveLength(1)
      expect(
        result.pipelines[0].crd.spec.flinkConfiguration[
          "restart-strategy.type"
        ],
      ).toBe("fixed-delay")
    })

    it("produces deterministic output across runs", () => {
      const plugin = errorHandlingPlugin({
        restartStrategy: { type: "failure-rate", maxFailuresPerInterval: 5 },
      })

      resetNodeIdCounter()
      const p1 = buildUnprotectedPipeline()
      const r1 = synth(p1, { plugins: [plugin] })

      resetNodeIdCounter()
      const p2 = buildUnprotectedPipeline()
      const r2 = synth(p2, { plugins: [plugin] })

      expect(r1.sql).toBe(r2.sql)
      expect(r1.crd).toEqual(r2.crd)
    })

    it("does not modify SQL output", () => {
      const plugin = errorHandlingPlugin()
      const pipeline = buildUnprotectedPipeline()

      const withPlugin = synth(pipeline, { plugins: [plugin] })

      resetNodeIdCounter()
      const pipeline2 = buildUnprotectedPipeline()
      const withoutPlugin = synth(pipeline2)

      // SQL should be identical — error handling plugin only modifies CRD
      expect(withPlugin.sql).toBe(withoutPlugin.sql)
    })
  })
})

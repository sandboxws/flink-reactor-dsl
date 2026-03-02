import { beforeEach, describe, expect, it } from "vitest"
import { synthesizeApp } from "../../core/app.js"
import {
  createElement,
  resetComponentKinds,
  resetNodeIdCounter,
} from "../../core/jsx-runtime.js"
import { Field, Schema } from "../../core/schema.js"
import type { ConstructNode } from "../../core/types.js"
import { synth } from "../../testing/index.js"
import { metricsPlugin } from "../metrics-plugin.js"

beforeEach(() => {
  resetNodeIdCounter()
  resetComponentKinds()
})

const testSchema = Schema({
  fields: {
    id: Field.BIGINT(),
    value: Field.DOUBLE(),
  },
})

function buildPipeline(): ConstructNode {
  return createElement(
    "Pipeline",
    { name: "metrics-test" },
    createElement(
      "KafkaSink",
      { topic: "output", bootstrapServers: "kafka:9092" },
      createElement("KafkaSource", {
        topic: "input",
        bootstrapServers: "kafka:9092",
        schema: testSchema,
      }),
    ),
  )
}

describe("metricsPlugin", () => {
  it("has the correct plugin name", () => {
    const plugin = metricsPlugin({ reporters: [{ type: "prometheus" }] })
    expect(plugin.name).toBe("flink-reactor:metrics")
    expect(plugin.version).toBe("0.1.0")
  })

  describe("Prometheus reporter", () => {
    it("configures Prometheus with default port", () => {
      const plugin = metricsPlugin({ reporters: [{ type: "prometheus" }] })
      const pipeline = buildPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      const config = result.crd.spec.flinkConfiguration

      expect(config["metrics.reporter.prometheus.factory.class"]).toBe(
        "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
      )
      expect(config["metrics.reporter.prometheus.port"]).toBe("9249")
    })

    it("configures Prometheus with custom port", () => {
      const plugin = metricsPlugin({
        reporters: [{ type: "prometheus", port: 9999 }],
      })
      const pipeline = buildPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      expect(
        result.crd.spec.flinkConfiguration["metrics.reporter.prometheus.port"],
      ).toBe("9999")
    })
  })

  describe("SLF4J reporter", () => {
    it("configures SLF4J with default interval", () => {
      const plugin = metricsPlugin({ reporters: [{ type: "slf4j" }] })
      const pipeline = buildPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      const config = result.crd.spec.flinkConfiguration

      expect(config["metrics.reporter.slf4j.factory.class"]).toBe(
        "org.apache.flink.metrics.slf4j.Slf4jReporterFactory",
      )
      expect(config["metrics.reporter.slf4j.interval"]).toBe("60s")
    })

    it("configures SLF4J with custom interval", () => {
      const plugin = metricsPlugin({
        reporters: [{ type: "slf4j", interval: "30s" }],
      })
      const pipeline = buildPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      expect(
        result.crd.spec.flinkConfiguration["metrics.reporter.slf4j.interval"],
      ).toBe("30s")
    })
  })

  describe("JMX reporter", () => {
    it("configures JMX with default port", () => {
      const plugin = metricsPlugin({ reporters: [{ type: "jmx" }] })
      const pipeline = buildPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      const config = result.crd.spec.flinkConfiguration

      expect(config["metrics.reporter.jmx.factory.class"]).toBe(
        "org.apache.flink.metrics.jmx.JMXReporterFactory",
      )
      expect(config["metrics.reporter.jmx.port"]).toBe("8789")
    })
  })

  describe("multiple reporters", () => {
    it("configures multiple reporters simultaneously", () => {
      const plugin = metricsPlugin({
        reporters: [
          { type: "prometheus", port: 9249 },
          { type: "slf4j", interval: "10s" },
          { type: "jmx" },
        ],
      })
      const pipeline = buildPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      const config = result.crd.spec.flinkConfiguration

      // All three reporters should be configured
      expect(config["metrics.reporter.prometheus.factory.class"]).toBeDefined()
      expect(config["metrics.reporter.slf4j.factory.class"]).toBeDefined()
      expect(config["metrics.reporter.jmx.factory.class"]).toBeDefined()

      // Annotation should list all reporters
      expect(
        result.crd.metadata.annotations?.["flink-reactor.io/metric-reporters"],
      ).toBe("prometheus,slf4j,jmx")
    })
  })

  describe("optional settings", () => {
    it("configures scope delimiter", () => {
      const plugin = metricsPlugin({
        reporters: [{ type: "prometheus" }],
        scopeDelimiter: "_",
      })
      const pipeline = buildPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      expect(
        result.crd.spec.flinkConfiguration["metrics.scope.delimiter"],
      ).toBe("_")
    })

    it("configures latency tracking interval", () => {
      const plugin = metricsPlugin({
        reporters: [{ type: "prometheus" }],
        latencyTrackingInterval: 5000,
      })
      const pipeline = buildPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      expect(
        result.crd.spec.flinkConfiguration["metrics.latency.interval"],
      ).toBe("5000")
    })

    it("disables latency tracking when set to 0", () => {
      const plugin = metricsPlugin({
        reporters: [{ type: "prometheus" }],
        latencyTrackingInterval: 0,
      })
      const pipeline = buildPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      expect(
        result.crd.spec.flinkConfiguration["metrics.latency.interval"],
      ).toBe("0")
    })
  })

  describe("integration", () => {
    it("works through synthesizeApp", () => {
      const plugin = metricsPlugin({
        reporters: [{ type: "prometheus", port: 9249 }],
      })
      const pipeline = buildPipeline()

      const result = synthesizeApp(
        { name: "test-app", children: pipeline },
        { plugins: [plugin] },
      )

      expect(result.pipelines).toHaveLength(1)
      const config = result.pipelines[0].crd.spec.flinkConfiguration
      expect(config["metrics.reporter.prometheus.factory.class"]).toBeDefined()
    })

    it("produces deterministic output across runs", () => {
      const plugin = metricsPlugin({
        reporters: [{ type: "prometheus" }, { type: "slf4j" }],
      })

      resetNodeIdCounter()
      const p1 = buildPipeline()
      const r1 = synth(p1, { plugins: [plugin] })

      resetNodeIdCounter()
      const p2 = buildPipeline()
      const r2 = synth(p2, { plugins: [plugin] })

      expect(r1.sql).toBe(r2.sql)
      expect(r1.crd).toEqual(r2.crd)
    })

    it("does not modify SQL output", () => {
      const plugin = metricsPlugin({
        reporters: [{ type: "prometheus" }],
      })
      const pipeline = buildPipeline()

      const withPlugin = synth(pipeline, { plugins: [plugin] })

      resetNodeIdCounter()
      const pipeline2 = buildPipeline()
      const withoutPlugin = synth(pipeline2)

      // SQL should be identical — metrics plugin only modifies CRD
      expect(withPlugin.sql).toBe(withoutPlugin.sql)
    })
  })
})

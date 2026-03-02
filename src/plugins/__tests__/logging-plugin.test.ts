import { beforeEach, describe, expect, it } from "vitest"
import { synthesizeApp } from "../../core/app.js"
import {
  createElement,
  resetComponentKinds,
  resetNodeIdCounter,
} from "../../core/jsx-runtime.js"
import { Field, Schema } from "../../core/schema.js"
import { findNodes } from "../../core/tree-utils.js"
import type { ConstructNode } from "../../core/types.js"
import { synth } from "../../testing/index.js"
import { loggingPlugin } from "../logging-plugin.js"

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
    { name: "log-test" },
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

describe("loggingPlugin", () => {
  it("has the correct plugin name", () => {
    const plugin = loggingPlugin()
    expect(plugin.name).toBe("flink-reactor:logging")
    expect(plugin.version).toBe("0.1.0")
  })

  describe("tree transformer", () => {
    it("annotates Source, Sink, and Transform nodes by default", () => {
      const plugin = loggingPlugin()
      const pipeline = buildPipeline()

      const transformed = plugin.transformTree?.(pipeline)

      // Pipeline itself should NOT be annotated
      expect(transformed.props._logging).toBeUndefined()

      // Find all annotated nodes
      const annotated = findNodes(transformed, (n) => n.props._logging != null)
      expect(annotated.length).toBeGreaterThanOrEqual(3)

      // Check annotation shape
      for (const node of annotated) {
        const logging = node.props._logging as { tag: string; level: string }
        expect(logging.tag).toBe("pipeline-log")
        expect(logging.level).toBe("INFO")
      }
    })

    it("respects custom targets option", () => {
      const plugin = loggingPlugin({ targets: ["Source"] })
      const pipeline = buildPipeline()

      const transformed = plugin.transformTree?.(pipeline)

      const annotated = findNodes(transformed, (n) => n.props._logging != null)
      // Only Source nodes should be annotated
      for (const node of annotated) {
        expect(node.kind).toBe("Source")
      }
    })

    it("uses custom tag and level", () => {
      const plugin = loggingPlugin({ tag: "audit", level: "DEBUG" })
      const pipeline = buildPipeline()

      const transformed = plugin.transformTree?.(pipeline)

      const annotated = findNodes(transformed, (n) => n.props._logging != null)
      expect(annotated.length).toBeGreaterThan(0)

      for (const node of annotated) {
        const logging = node.props._logging as { tag: string; level: string }
        expect(logging.tag).toBe("audit")
        expect(logging.level).toBe("DEBUG")
      }
    })
  })

  describe("CRD transformer", () => {
    it("adds root logger level to flinkConfiguration", () => {
      const plugin = loggingPlugin({ level: "WARN" })
      const pipeline = buildPipeline()

      const result = synth(pipeline, { plugins: [plugin] })

      expect(result.crd.spec.flinkConfiguration["rootLogger.level"]).toBe(
        "WARN",
      )
    })

    it("configures additional loggers", () => {
      const plugin = loggingPlugin({
        loggers: {
          "org.apache.flink.streaming": "DEBUG",
          "org.apache.kafka": "WARN",
        },
      })
      const pipeline = buildPipeline()

      const result = synth(pipeline, { plugins: [plugin] })
      const config = result.crd.spec.flinkConfiguration

      expect(config["logger.org_apache_flink_streaming.name"]).toBe(
        "org.apache.flink.streaming",
      )
      expect(config["logger.org_apache_flink_streaming.level"]).toBe("DEBUG")
      expect(config["logger.org_apache_kafka.name"]).toBe("org.apache.kafka")
      expect(config["logger.org_apache_kafka.level"]).toBe("WARN")
    })

    it("adds logging metadata annotations", () => {
      const plugin = loggingPlugin({ tag: "my-tag" })
      const pipeline = buildPipeline()

      const result = synth(pipeline, { plugins: [plugin] })

      expect(
        result.crd.metadata.annotations?.["flink-reactor.io/logging-tag"],
      ).toBe("my-tag")
      expect(
        result.crd.metadata.annotations?.["flink-reactor.io/logging-targets"],
      ).toBeDefined()
    })
  })

  describe("integration", () => {
    it("works through synthesizeApp", () => {
      const plugin = loggingPlugin({ level: "DEBUG" })
      const pipeline = buildPipeline()

      const result = synthesizeApp(
        { name: "test-app", children: pipeline },
        { plugins: [plugin] },
      )

      expect(result.pipelines).toHaveLength(1)
      expect(
        result.pipelines[0].crd.spec.flinkConfiguration["rootLogger.level"],
      ).toBe("DEBUG")
    })

    it("produces deterministic output across runs", () => {
      const plugin = loggingPlugin({ level: "INFO", tag: "determ-test" })

      resetNodeIdCounter()
      const p1 = buildPipeline()
      const r1 = synth(p1, { plugins: [plugin] })

      resetNodeIdCounter()
      const p2 = buildPipeline()
      const r2 = synth(p2, { plugins: [plugin] })

      expect(r1.sql).toBe(r2.sql)
      expect(r1.crd).toEqual(r2.crd)
    })
  })
})

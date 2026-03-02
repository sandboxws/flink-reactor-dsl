import { describe, expect, it } from "vitest"
import { createElement } from "../../../core/jsx-runtime.js"
import { SynthContext } from "../../../core/synth-context.js"
import type { ConstructNode } from "../../../core/types.js"
import { generateAscii, generateDot } from "../graph.js"

function buildGraphData(pipelineNode: ConstructNode) {
  const ctx = new SynthContext()
  ctx.buildFromTree(pipelineNode)
  return {
    nodes: ctx.getAllNodes(),
    edges: ctx.getAllEdges(),
  }
}

describe("graph command", () => {
  describe("ASCII output", () => {
    it("produces ASCII output for a simple pipeline", () => {
      const pipeline = createElement(
        "Pipeline",
        { name: "test-pipeline" },
        createElement(
          "KafkaSink",
          { topic: "output", name: "output-sink" },
          createElement(
            "Filter",
            { condition: "amount > 100", name: "filter" },
            createElement("KafkaSource", {
              topic: "input",
              name: "input-source",
              schema: { fields: { amount: "BIGINT" }, metadataColumns: [] },
            }),
          ),
        ),
      )

      const { nodes, edges } = buildGraphData(pipeline)
      const ascii = generateAscii("test-pipeline", nodes, edges)

      expect(ascii).toContain("Pipeline: test-pipeline")
      expect(ascii).toContain("[Pipeline]")
      expect(ascii).toContain("[Sink]")
      expect(ascii).toContain("[Transform]")
      expect(ascii).toContain("[Source]")
      expect(ascii).toContain("KafkaSource")
      expect(ascii).toContain("KafkaSink")
      expect(ascii).toContain("Filter")
    })

    it("includes node types and names in visualization", () => {
      const pipeline = createElement(
        "Pipeline",
        { name: "orders" },
        createElement("KafkaSource", {
          topic: "orders",
          name: "order-events",
          schema: { fields: { id: "BIGINT" }, metadataColumns: [] },
        }),
      )

      const { nodes, edges } = buildGraphData(pipeline)
      const ascii = generateAscii("orders", nodes, edges)

      expect(ascii).toContain("[Source]")
      expect(ascii).toContain("KafkaSource")
    })

    it("handles empty pipeline", () => {
      const ascii = generateAscii("empty", [], [])
      expect(ascii).toContain("(empty pipeline)")
    })
  })

  describe("DOT output", () => {
    it("produces valid DOT format", () => {
      const pipeline = createElement(
        "Pipeline",
        { name: "dot-test" },
        createElement(
          "KafkaSink",
          { topic: "output" },
          createElement("KafkaSource", {
            topic: "input",
            schema: { fields: { id: "BIGINT" }, metadataColumns: [] },
          }),
        ),
      )

      const { nodes, edges } = buildGraphData(pipeline)
      const dot = generateDot("dot-test", nodes, edges)

      expect(dot).toContain('digraph "dot-test"')
      expect(dot).toContain("rankdir=TB")
      expect(dot).toContain("->")
      expect(dot).toContain("}")
    })

    it("assigns colors by node kind", () => {
      const pipeline = createElement(
        "Pipeline",
        { name: "colored" },
        createElement(
          "KafkaSink",
          { topic: "output" },
          createElement("KafkaSource", {
            topic: "input",
            schema: { fields: { id: "BIGINT" }, metadataColumns: [] },
          }),
        ),
      )

      const { nodes, edges } = buildGraphData(pipeline)
      const dot = generateDot("colored", nodes, edges)

      // Source should be green, Sink should be red
      expect(dot).toContain("#4CAF50") // Source green
      expect(dot).toContain("#F44336") // Sink red
    })
  })
})

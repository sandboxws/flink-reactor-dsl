import { beforeEach, describe, expect, it } from "vitest"
import {
  createElement,
  Field,
  Fragment,
  Schema,
  synthesizeApp,
} from "@/browser.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const testSchema = Schema({
  fields: {
    id: Field.BIGINT(),
    name: Field.STRING(),
    amount: Field.DOUBLE(),
  },
})

describe("browser entry compatibility", () => {
  it("synthesizeApp produces SQL from a minimal pipeline", () => {
    const pipeline = createElement(
      "Pipeline",
      { name: "browser-test" },
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

    const result = synthesizeApp({ name: "test-app", children: pipeline })

    expect(result.pipelines).toHaveLength(1)
    expect(result.pipelines[0].sql.sql).toEqual(expect.any(String))
    expect(result.pipelines[0].sql.sql.length).toBeGreaterThan(0)
    expect(result.pipelines[0].sql.sql).toContain("CREATE TABLE")
  })

  it("createElement and Fragment are importable and functional", () => {
    const node = createElement("KafkaSource", {
      topic: "test",
      schema: testSchema,
    })
    expect(node.kind).toBe("Source")
    expect(node.component).toBe("KafkaSource")

    // Fragment returns children as-is for flattening by parent createElement
    expect(typeof Fragment).toBe("function")
    const children = Fragment({ children: [node] })
    expect(children).toContain(node)
  })
})

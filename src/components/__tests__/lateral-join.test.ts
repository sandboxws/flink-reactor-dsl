import { beforeEach, describe, expect, it } from "vitest"
import { LateralJoin } from "@/components/lateral-join.js"
import { KafkaSource } from "@/components/sources.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { SynthContext } from "@/core/synth-context.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    shipping_address: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
  },
})

describe("LateralJoin", () => {
  it("creates a LateralJoin node with input and function", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })

    const joined = LateralJoin({
      input: source,
      function: "parse_address",
      args: ["shipping_address"],
      as: { street: "STRING", city: "STRING", zip: "STRING" },
      type: "left",
    })

    expect(joined.kind).toBe("Join")
    expect(joined.component).toBe("LateralJoin")
    expect(joined.props.function).toBe("parse_address")
    expect(joined.props.args).toEqual(["shipping_address"])
    expect(joined.props.as).toEqual({
      street: "STRING",
      city: "STRING",
      zip: "STRING",
    })
    expect(joined.props.type).toBe("left")
    expect(joined.props.input).toBe(source.id)
    // Input stream is wired as child for DAG edges
    expect(joined.children).toHaveLength(1)
  })

  it("builds a valid DAG", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })

    const joined = LateralJoin({
      input: source,
      function: "parse_address",
      args: ["shipping_address"],
      as: { street: "STRING", city: "STRING" },
    })

    const ctx = new SynthContext()
    ctx.buildFromTree(joined)

    // LateralJoin → KafkaSource
    expect(ctx.getAllNodes()).toHaveLength(2)
    expect(ctx.getAllEdges()).toHaveLength(1)
  })

  it("defaults to cross join type", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })

    const joined = LateralJoin({
      input: source,
      function: "split_tags",
      args: ["tags"],
      as: { tag: "STRING" },
    })

    expect(joined.props.type).toBeUndefined()
  })

  it("throws when input is missing", () => {
    expect(() =>
      LateralJoin({
        // biome-ignore lint/suspicious/noExplicitAny: intentional invalid input for error-path test
        input: undefined as unknown as any,
        function: "parse_address",
        args: ["addr"],
        as: { street: "STRING" },
      }),
    ).toThrow("LateralJoin requires an input")
  })

  it("throws when function is missing", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })

    expect(() =>
      LateralJoin({
        input: source,
        function: "",
        args: ["addr"],
        as: { street: "STRING" },
      }),
    ).toThrow("LateralJoin requires a function name")
  })

  it("throws when args is empty", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })

    expect(() =>
      LateralJoin({
        input: source,
        function: "parse_address",
        args: [],
        as: { street: "STRING" },
      }),
    ).toThrow("LateralJoin requires at least one argument")
  })

  it("throws when as is empty", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })

    expect(() =>
      LateralJoin({
        input: source,
        function: "parse_address",
        args: ["addr"],
        as: {},
      }),
    ).toThrow("LateralJoin requires output column definitions")
  })
})

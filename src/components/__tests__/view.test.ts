import { beforeEach, describe, expect, it } from "vitest"
import { resetNodeIdCounter } from "../../core/jsx-runtime.js"
import { Field, Schema } from "../../core/schema.js"
import { SynthContext } from "../../core/synth-context.js"
import { KafkaSource } from "../sources.js"
import { Map } from "../transforms.js"
import { View } from "../view.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    user_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    quantity: Field.INT(),
  },
})

describe("View", () => {
  it("creates a View node with name and upstream children", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })
    const mapped = Map({
      select: { order_id: "order_id", total: "amount * quantity" },
      children: [source],
    })

    const view = View({
      name: "enriched_orders",
      children: [mapped],
    })

    expect(view.kind).toBe("View")
    expect(view.component).toBe("View")
    expect(view.props.name).toBe("enriched_orders")
    expect(view.children).toHaveLength(1)
  })

  it("builds a valid DAG", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })
    const mapped = Map({
      select: { total: "amount * quantity" },
      children: [source],
    })

    const view = View({
      name: "totals",
      children: [mapped],
    })

    const ctx = new SynthContext()
    ctx.buildFromTree(view)

    // View → Map → KafkaSource
    expect(ctx.getAllNodes()).toHaveLength(3)
    expect(ctx.getAllEdges()).toHaveLength(2)
  })

  it("throws when name is missing", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })

    expect(() =>
      View({
        name: "",
        children: [source],
      }),
    ).toThrow("View requires a name")
  })

  it("supports a direct source as child (no transforms)", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })

    const view = View({
      name: "raw_orders",
      children: [source],
    })

    expect(view.kind).toBe("View")
    expect(view.children).toHaveLength(1)
    expect(view.children[0].component).toBe("KafkaSource")
  })
})

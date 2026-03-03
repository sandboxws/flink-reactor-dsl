import { beforeEach, describe, expect, it } from "vitest"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { Validate } from "@/components/validate.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { SynthContext } from "@/core/synth-context.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    user_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    email: Field.STRING(),
  },
})

describe("Validate", () => {
  it("creates a Validate node with rules and children", () => {
    const source = KafkaSource({ topic: "raw-orders", schema: OrderSchema })
    const rejectSink = KafkaSink({ topic: "invalid-orders" })
    const rejectWrapper = Validate.Reject({ children: rejectSink })

    const validate = Validate({
      rules: {
        notNull: ["order_id", "user_id", "amount"],
        range: { amount: [0, 1_000_000] },
        expression: { valid_email: "email LIKE '%@%.%'" },
      },
      children: [rejectWrapper, source],
    })

    expect(validate.kind).toBe("Transform")
    expect(validate.component).toBe("Validate")
    expect(validate.props.rules).toEqual({
      notNull: ["order_id", "user_id", "amount"],
      range: { amount: [0, 1_000_000] },
      expression: { valid_email: "email LIKE '%@%.%'" },
    })
    expect(validate.children).toHaveLength(2)
  })

  it("builds a valid DAG", () => {
    const source = KafkaSource({ topic: "raw-orders", schema: OrderSchema })
    const rejectSink = KafkaSink({ topic: "invalid-orders" })
    const rejectWrapper = Validate.Reject({ children: rejectSink })

    const validate = Validate({
      rules: { notNull: ["order_id"] },
      children: [rejectWrapper, source],
    })

    const ctx = new SynthContext()
    ctx.buildFromTree(validate)

    // Validate → Validate.Reject → KafkaSink, Validate → KafkaSource
    expect(ctx.getAllNodes()).toHaveLength(4)
    expect(ctx.getAllEdges()).toHaveLength(3)
  })

  it("throws when rules are empty", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })
    const rejectSink = KafkaSink({ topic: "invalid" })
    const wrapper = Validate.Reject({ children: rejectSink })

    expect(() =>
      Validate({
        rules: {},
        children: [wrapper, source],
      }),
    ).toThrow("Validate requires at least one validation rule")
  })

  it("throws when Validate.Reject is missing", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })

    expect(() =>
      Validate({
        rules: { notNull: ["order_id"] },
        children: [source],
      }),
    ).toThrow("Validate requires a Validate.Reject child")
  })

  it("supports notNull-only rules", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })
    const rejectSink = KafkaSink({ topic: "invalid" })
    const wrapper = Validate.Reject({ children: rejectSink })

    const validate = Validate({
      rules: { notNull: ["order_id", "user_id"] },
      children: [wrapper, source],
    })

    expect(validate.component).toBe("Validate")
  })

  it("supports range-only rules", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })
    const rejectSink = KafkaSink({ topic: "invalid" })
    const wrapper = Validate.Reject({ children: rejectSink })

    const validate = Validate({
      rules: { range: { amount: [0, 100] } },
      children: [wrapper, source],
    })

    expect(validate.component).toBe("Validate")
  })

  it("supports expression-only rules", () => {
    const source = KafkaSource({ topic: "orders", schema: OrderSchema })
    const rejectSink = KafkaSink({ topic: "invalid" })
    const wrapper = Validate.Reject({ children: rejectSink })

    const validate = Validate({
      rules: { expression: { has_email: "email IS NOT NULL AND email <> ''" } },
      children: [wrapper, source],
    })

    expect(validate.component).toBe("Validate")
  })
})

describe("Validate.Reject", () => {
  it("wraps a downstream sink", () => {
    const sink = KafkaSink({ topic: "rejected" })
    const wrapper = Validate.Reject({ children: sink })

    expect(wrapper.component).toBe("Validate.Reject")
    expect(wrapper.children).toHaveLength(1)
    expect(wrapper.children[0].component).toBe("KafkaSink")
  })
})

import { beforeEach, describe, expect, it } from "vitest"
import { RawSQL, UDF } from "@/components/escape-hatches.js"
import { createElement, resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { SynthContext } from "@/core/synth-context.js"

beforeEach(() => {
  resetNodeIdCounter()
})

function makeSource(name: string) {
  return createElement("KafkaSource", { topic: name })
}

// ── RawSQL ──────────────────────────────────────────────────────────

describe("RawSQL", () => {
  it("creates a RawSQL node with input streams as children", () => {
    const orders = makeSource("orders")
    const users = makeSource("users")

    const raw = RawSQL({
      sql: "SELECT o.*, u.name FROM orders o JOIN users u ON o.user_id = u.id",
      inputs: [orders, users],
      outputSchema: Schema({
        fields: {
          order_id: Field.BIGINT(),
          user_name: Field.STRING(),
        },
      }),
    })

    expect(raw.kind).toBe("RawSQL")
    expect(raw.component).toBe("RawSQL")
    expect(raw.props.sql).toBe(
      "SELECT o.*, u.name FROM orders o JOIN users u ON o.user_id = u.id",
    )
    expect(raw.props.inputIds).toEqual([orders.id, users.id])
    // Input streams are wired as children for DAG edges
    expect(raw.children).toHaveLength(2)
  })

  it("output schema matches the declared schema", () => {
    const source = makeSource("events")
    const outputSchema = Schema({
      fields: {
        event_id: Field.BIGINT(),
        computed_field: Field.STRING(),
      },
    })

    const raw = RawSQL({
      sql: "SELECT event_id, UPPER(payload) AS computed_field FROM events",
      inputs: [source],
      outputSchema,
    })

    expect(raw.props.outputSchema).toEqual(outputSchema)
    expect(
      (raw.props.outputSchema as typeof outputSchema).fields.event_id,
    ).toBe("BIGINT")
    expect(
      (raw.props.outputSchema as typeof outputSchema).fields.computed_field,
    ).toBe("STRING")
  })

  it("wires correctly into the DAG", () => {
    const source = makeSource("orders")

    const raw = RawSQL({
      sql: "SELECT * FROM orders WHERE amount > 100",
      inputs: [source],
      outputSchema: Schema({
        fields: { order_id: Field.BIGINT(), amount: Field.DECIMAL(10, 2) },
      }),
    })

    const ctx = new SynthContext()
    ctx.buildFromTree(raw)
    expect(ctx.getAllNodes()).toHaveLength(2)
    expect(ctx.getAllEdges()).toHaveLength(1)
  })

  it("constructs without inputs for self-contained SQL bodies", () => {
    // Self-contained SQL (e.g. VALUES literal, SELECT 1) has no upstream
    // streams. RawSQL accepts an absent or empty `inputs` prop and produces
    // a node with no DAG children — the SQL body stands on its own, and
    // sibling-chain wiring (or downstream consumers) connects it forward.
    const raw = RawSQL({
      sql: "SELECT 1 AS x",
      outputSchema: Schema({ fields: { x: Field.INT() } }),
    })
    expect(raw.kind).toBe("RawSQL")
    expect(raw.props.inputIds).toEqual([])
    expect(raw.children).toHaveLength(0)

    // Explicit empty array behaves identically.
    const rawEmpty = RawSQL({
      sql: "SELECT 1 AS x",
      inputs: [],
      outputSchema: Schema({ fields: { x: Field.INT() } }),
    })
    expect(rawEmpty.props.inputIds).toEqual([])
    expect(rawEmpty.children).toHaveLength(0)
  })
})

// ── UDF ─────────────────────────────────────────────────────────────

describe("UDF", () => {
  it("creates a UDF node with function registration metadata", () => {
    const udf = UDF({
      name: "my_hash",
      className: "com.mycompany.HashFunction",
      jarPath: "/path/to/udf.jar",
    })

    expect(udf.kind).toBe("UDF")
    expect(udf.component).toBe("UDF")
    expect(udf.props.name).toBe("my_hash")
    expect(udf.props.className).toBe("com.mycompany.HashFunction")
    expect(udf.props.jarPath).toBe("/path/to/udf.jar")
  })
})

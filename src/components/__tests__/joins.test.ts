import { beforeEach, describe, expect, it } from "vitest"
import {
  IntervalJoin,
  Join,
  LookupJoin,
  TemporalJoin,
} from "@/components/joins.js"
import { createElement, resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { SynthContext } from "@/core/synth-context.js"

beforeEach(() => {
  resetNodeIdCounter()
})

function makeSource(name: string) {
  return createElement("KafkaSource", { topic: name })
}

// ── 4.1 Inner join of two streams wires correctly ───────────────────

describe("Join", () => {
  it("creates a join node with edges from both inputs", () => {
    const left = makeSource("orders")
    const right = makeSource("users")

    const join = Join({
      left,
      right,
      on: "orders.user_id = users.id",
      type: "inner",
    })

    expect(join.kind).toBe("Join")
    expect(join.component).toBe("Join")
    expect(join.props.on).toBe("orders.user_id = users.id")
    expect(join.props.type).toBe("inner")
    expect(join.props.left).toBe(left.id)
    expect(join.props.right).toBe(right.id)

    // Both inputs are children for DAG edge creation
    expect(join.children).toHaveLength(2)

    const ctx = new SynthContext()
    ctx.buildFromTree(join)
    expect(ctx.getAllNodes()).toHaveLength(3)
    expect(ctx.getAllEdges()).toHaveLength(2)
  })

  it("supports downstream children after the join", () => {
    const left = makeSource("orders")
    const right = makeSource("users")
    const sink = createElement("KafkaSink", { topic: "enriched" })

    const join = Join({
      left,
      right,
      on: "a.id = b.id",
      children: sink,
    })

    // left, right, and downstream sink
    expect(join.children).toHaveLength(3)
  })

  // ── 4.2 Anti/semi join sets type on construct node ────────────────

  it("stores anti join type", () => {
    const left = makeSource("all-events")
    const right = makeSource("blocklist")

    const join = Join({
      left,
      right,
      on: "events.user_id = blocklist.user_id",
      type: "anti",
    })

    expect(join.props.type).toBe("anti")
  })

  it("stores semi join type", () => {
    const left = makeSource("orders")
    const right = makeSource("active-users")

    const join = Join({
      left,
      right,
      on: "orders.user_id = users.id",
      type: "semi",
    })

    expect(join.props.type).toBe("semi")
  })

  // ── 4.3 Join with broadcast hint stores hint metadata ─────────────

  it("stores broadcast hint", () => {
    const left = makeSource("large-stream")
    const right = makeSource("small-dim")

    const join = Join({
      left,
      right,
      on: "a.key = b.key",
      type: "inner",
      hints: { broadcast: "right" },
    })

    expect(join.props.hints).toEqual({ broadcast: "right" })
  })

  it("stores state TTL", () => {
    const left = makeSource("orders")
    const right = makeSource("payments")

    const join = Join({
      left,
      right,
      on: "o.id = p.order_id",
      stateTtl: "1h",
    })

    expect(join.props.stateTtl).toBe("1h")
  })

  it("throws when left or right is missing", () => {
    const source = makeSource("orders")

    expect(() =>
      Join({
        left: source,
        right: undefined as unknown as typeof source,
        on: "a = b",
      }),
    ).toThrow("Join requires both left and right inputs")
  })
})

// ── 4.5 TemporalJoin stores asOf column ─────────────────────────────

describe("TemporalJoin", () => {
  it("stores temporal join metadata with asOf column", () => {
    const orders = makeSource("orders")
    const rates = makeSource("exchange-rates")

    const join = TemporalJoin({
      stream: orders,
      temporal: rates,
      on: "orders.currency = rates.currency",
      asOf: "orders.order_time",
    })

    expect(join.kind).toBe("Join")
    expect(join.component).toBe("TemporalJoin")
    expect(join.props.asOf).toBe("orders.order_time")
    expect(join.props.on).toBe("orders.currency = rates.currency")
    expect(join.props.stream).toBe(orders.id)
    expect(join.props.temporal).toBe(rates.id)
    expect(join.children).toHaveLength(2)
  })
})

// ── 4.4 LookupJoin stores async and cache config ────────────────────

describe("LookupJoin", () => {
  it("stores async and cache configuration", () => {
    const input = makeSource("orders")

    const join = LookupJoin({
      input,
      table: "dim_products",
      url: "jdbc:postgresql://localhost:5433/db",
      on: "orders.product_id = dim_products.id",
      select: {
        product_name: "dim_products.name",
        price: "dim_products.price",
      },
      async: { enabled: true, capacity: 100, timeout: "30s" },
      cache: { type: "lru", maxRows: 10000, ttl: "1h" },
    })

    expect(join.kind).toBe("Join")
    expect(join.component).toBe("LookupJoin")
    expect(join.props.table).toBe("dim_products")
    expect(join.props.url).toBe("jdbc:postgresql://localhost:5433/db")
    expect(join.props.async).toEqual({
      enabled: true,
      capacity: 100,
      timeout: "30s",
    })
    expect(join.props.cache).toEqual({ type: "lru", maxRows: 10000, ttl: "1h" })
    expect(join.props.select).toEqual({
      product_name: "dim_products.name",
      price: "dim_products.price",
    })
  })

  it("auto-injects proc_time metadata flag", () => {
    const input = makeSource("events")

    const join = LookupJoin({
      input,
      table: "dim_users",
      url: "jdbc:mysql://localhost:3306/db",
      on: "events.user_id = dim_users.id",
    })

    expect(join.props.procTime).toBe(true)
    expect(join.props.input).toBe(input.id)
  })
})

// ── 4.6 IntervalJoin stores interval bounds ─────────────────────────

describe("IntervalJoin", () => {
  it("stores interval bounds on the construct node", () => {
    const orders = makeSource("orders")
    const shipments = makeSource("shipments")

    const join = IntervalJoin({
      left: orders,
      right: shipments,
      on: "orders.id = shipments.order_id",
      interval: {
        from: "orders.order_time",
        to: "orders.order_time + INTERVAL '7' DAY",
      },
      type: "inner",
    })

    expect(join.kind).toBe("Join")
    expect(join.component).toBe("IntervalJoin")
    expect(join.props.interval).toEqual({
      from: "orders.order_time",
      to: "orders.order_time + INTERVAL '7' DAY",
    })
    expect(join.props.type).toBe("inner")
    expect(join.props.left).toBe(orders.id)
    expect(join.props.right).toBe(shipments.id)
    expect(join.children).toHaveLength(2)
  })

  it("supports left outer interval join", () => {
    const left = makeSource("clicks")
    const right = makeSource("conversions")

    const join = IntervalJoin({
      left,
      right,
      on: "clicks.session = conversions.session",
      interval: { from: "clicks.ts", to: "clicks.ts + INTERVAL '30' MINUTE" },
      type: "left",
    })

    expect(join.props.type).toBe("left")
  })
})

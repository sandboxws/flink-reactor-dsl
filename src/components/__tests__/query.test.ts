import { beforeEach, describe, expect, it } from "vitest"
import type { WindowFunctionExpr } from "@/components/query.js"
import { Query } from "@/components/query.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"

const OutputSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    total: Field.DECIMAL(10, 2),
  },
})

beforeEach(() => {
  resetNodeIdCounter()
})

describe("Query component", () => {
  it("creates a Query node with Select + GroupBy + Having", () => {
    const select = Query.Select({
      columns: {
        user_id: "user_id",
        total: "SUM(amount)",
      },
    })

    const groupBy = Query.GroupBy({ columns: ["user_id"] })
    const having = Query.Having({ condition: "SUM(amount) > 1000" })

    const query = Query({
      outputSchema: OutputSchema,
      children: [select, groupBy, having],
    })

    expect(query.kind).toBe("Transform")
    expect(query.component).toBe("Query")
    expect(query.children).toHaveLength(3)
    expect(query.children[0].component).toBe("Query.Select")
    expect(query.children[1].component).toBe("Query.GroupBy")
    expect(query.children[2].component).toBe("Query.Having")
  })

  it("throws when Query has no Select child", () => {
    const groupBy = Query.GroupBy({ columns: ["user_id"] })

    expect(() =>
      Query({
        outputSchema: OutputSchema,
        children: [groupBy],
      }),
    ).toThrow("Query requires a Query.Select child")
  })

  it("throws when Having is used without GroupBy", () => {
    const select = Query.Select({
      columns: { user_id: "user_id" },
    })
    const having = Query.Having({ condition: "COUNT(*) > 5" })

    expect(() =>
      Query({
        outputSchema: OutputSchema,
        children: [select, having],
      }),
    ).toThrow("Query.Having requires a Query.GroupBy sibling")
  })

  it("throws on duplicate clause children", () => {
    const select1 = Query.Select({
      columns: { user_id: "user_id" },
    })
    const select2 = Query.Select({
      columns: { total: "SUM(amount)" },
    })

    expect(() =>
      Query({
        outputSchema: OutputSchema,
        children: [select1, select2],
      }),
    ).toThrow("Query must have at most one Query.Select child")
  })

  it("creates correct props for window function expressions", () => {
    const windowExpr: WindowFunctionExpr = {
      func: "LAG",
      args: ["amount", 1],
      over: {
        partitionBy: ["user_id"],
        orderBy: { event_time: "ASC" },
      },
    }

    const select = Query.Select({
      columns: {
        user_id: "user_id",
        prev_amount: windowExpr,
      },
    })

    const cols = select.props.columns as Record<string, unknown>
    expect(cols.user_id).toBe("user_id")
    expect(cols.prev_amount).toEqual({
      func: "LAG",
      args: ["amount", 1],
      over: {
        partitionBy: ["user_id"],
        orderBy: { event_time: "ASC" },
      },
    })
  })

  it("creates correct props for named window references", () => {
    const select = Query.Select({
      columns: {
        row_num: { func: "ROW_NUMBER", window: "w" },
      },
      windows: {
        w: {
          partitionBy: ["user_id"],
          orderBy: { event_time: "ASC" },
        },
      },
    })

    expect(select.props.windows).toEqual({
      w: {
        partitionBy: ["user_id"],
        orderBy: { event_time: "ASC" },
      },
    })
  })

  it("accepts all clause types together", () => {
    const select = Query.Select({
      columns: { user_id: "user_id", total: "SUM(amount)" },
    })
    const where = Query.Where({ condition: "status = 'active'" })
    const groupBy = Query.GroupBy({ columns: ["user_id"] })
    const having = Query.Having({ condition: "SUM(amount) > 100" })
    const orderBy = Query.OrderBy({ columns: { total: "DESC" } })

    const query = Query({
      outputSchema: OutputSchema,
      children: [select, where, groupBy, having, orderBy],
    })

    expect(query.children).toHaveLength(5)
    expect(query.children.map((c) => c.component)).toEqual([
      "Query.Select",
      "Query.Where",
      "Query.GroupBy",
      "Query.Having",
      "Query.OrderBy",
    ])
  })
})

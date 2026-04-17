import { beforeEach, describe, expect, it, vi } from "vitest"
import {
  introspectPostgresTables,
  mapPgTypeToFlink,
  resetIntrospectCache,
  splitQualifiedTable,
} from "@/cli/connectors/postgres-introspect.js"

// Mocked pg driver. `connectCount`/`queryCount` track invocations across
// the two queries (columns + primary keys).
const connectCount = vi.fn()
const queryCount = vi.fn()

class FakeClient {
  constructor(_config: { connectionString: string }) {}
  connect() {
    connectCount()
    return Promise.resolve()
  }
  query(text: string, _values?: unknown[]) {
    queryCount()
    if (text.includes("information_schema.columns")) {
      return Promise.resolve({
        rows: [
          {
            table_schema: "public",
            table_name: "orders",
            column_name: "order_id",
            data_type: "bigint",
            udt_name: "int8",
            character_maximum_length: null,
            numeric_precision: null,
            numeric_scale: null,
            ordinal_position: 1,
            is_nullable: "NO",
          },
          {
            table_schema: "public",
            table_name: "orders",
            column_name: "amount",
            data_type: "numeric",
            udt_name: "numeric",
            character_maximum_length: null,
            numeric_precision: 10,
            numeric_scale: 2,
            ordinal_position: 2,
            is_nullable: "YES",
          },
          {
            table_schema: "public",
            table_name: "orders",
            column_name: "product",
            data_type: "character varying",
            udt_name: "varchar",
            character_maximum_length: 200,
            numeric_precision: null,
            numeric_scale: null,
            ordinal_position: 3,
            is_nullable: "YES",
          },
        ],
      })
    }
    // PK query
    return Promise.resolve({
      rows: [
        {
          table_schema: "public",
          table_name: "orders",
          column_name: "order_id",
        },
      ],
    })
  }
  end() {
    return Promise.resolve()
  }
}

vi.mock("pg", () => ({ default: { Client: FakeClient }, Client: FakeClient }))

beforeEach(() => {
  connectCount.mockClear()
  queryCount.mockClear()
  resetIntrospectCache()
})

describe("mapPgTypeToFlink", () => {
  it("maps integer variants", () => {
    expect(mapPgTypeToFlink("integer", "int4")).toBe("INT")
    expect(mapPgTypeToFlink("bigint", "int8")).toBe("BIGINT")
    expect(mapPgTypeToFlink("smallint", "int2")).toBe("SMALLINT")
  })

  it("maps decimal with precision/scale", () => {
    expect(mapPgTypeToFlink("numeric", "numeric", null, 10, 2)).toBe(
      "DECIMAL(10, 2)",
    )
  })

  it("maps varchar with length", () => {
    expect(mapPgTypeToFlink("character varying", "varchar", 200)).toBe(
      "VARCHAR(200)",
    )
  })

  it("maps text/jsonb/uuid to STRING", () => {
    expect(mapPgTypeToFlink("text", "text")).toBe("STRING")
    expect(mapPgTypeToFlink("jsonb", "jsonb")).toBe("STRING")
    expect(mapPgTypeToFlink("uuid", "uuid")).toBe("STRING")
  })

  it("maps timestamp variants", () => {
    expect(mapPgTypeToFlink("timestamp", "timestamp")).toBe("TIMESTAMP(6)")
    expect(mapPgTypeToFlink("timestamptz", "timestamptz")).toBe(
      "TIMESTAMP_LTZ(6)",
    )
  })

  it("falls back to STRING on unknown types", () => {
    expect(mapPgTypeToFlink("weird_pg_type", "weird_pg_type")).toBe("STRING")
  })
})

describe("splitQualifiedTable", () => {
  it("splits schema.table", () => {
    expect(splitQualifiedTable("public.orders")).toEqual({
      schema: "public",
      table: "orders",
    })
  })

  it("defaults schema to public when absent", () => {
    expect(splitQualifiedTable("orders")).toEqual({
      schema: "public",
      table: "orders",
    })
  })
})

describe("introspectPostgresTables", () => {
  it("queries information_schema.columns + PK in one connection", async () => {
    const result = await introspectPostgresTables({
      connectionString: "postgres://user:pw@localhost:5432/shop",
      schemaList: ["public"],
      tableList: ["orders"],
    })

    expect(connectCount).toHaveBeenCalledTimes(1)
    // Exactly two queries: one for columns, one for primary keys.
    expect(queryCount).toHaveBeenCalledTimes(2)
    const cols = result.get("public.orders")
    expect(cols).toBeDefined()
    expect(cols?.map((c) => c.name)).toEqual(["order_id", "amount", "product"])
  })

  it("annotates PK columns with the PK constraint", async () => {
    const result = await introspectPostgresTables({
      connectionString: "postgres://user:pw@localhost:5432/shop",
      schemaList: ["public"],
      tableList: ["orders"],
    })

    const cols = result.get("public.orders") ?? []
    const orderId = cols.find((c) => c.name === "order_id")
    const amount = cols.find((c) => c.name === "amount")
    expect(orderId?.constraints).toEqual(["PK"])
    expect(amount?.constraints).toEqual([])
  })

  it("caches the result — second call makes no new queries", async () => {
    await introspectPostgresTables({
      connectionString: "postgres://user:pw@localhost:5432/shop",
      schemaList: ["public"],
      tableList: ["orders"],
    })
    expect(queryCount).toHaveBeenCalledTimes(2)

    await introspectPostgresTables({
      connectionString: "postgres://user:pw@localhost:5432/shop",
      schemaList: ["public"],
      tableList: ["orders"],
    })
    // Still 2 — the second call hits the cache.
    expect(queryCount).toHaveBeenCalledTimes(2)
    expect(connectCount).toHaveBeenCalledTimes(1)
  })

  it("maps PG → Flink types in the returned columns", async () => {
    const result = await introspectPostgresTables({
      connectionString: "postgres://user:pw@localhost:5432/shop",
      schemaList: ["public"],
      tableList: ["orders"],
    })

    const cols = result.get("public.orders") ?? []
    const byName = new Map(cols.map((c) => [c.name, c.type]))
    expect(byName.get("order_id")).toBe("BIGINT")
    expect(byName.get("amount")).toBe("DECIMAL(10, 2)")
    expect(byName.get("product")).toBe("VARCHAR(200)")
  })
})

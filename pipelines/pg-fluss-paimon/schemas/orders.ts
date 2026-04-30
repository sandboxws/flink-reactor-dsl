import { Field, Schema } from "@/core/schema.js"

export const OrdersSchema = Schema({
  fields: {
    o_orderkey: Field.BIGINT(),
    o_custkey: Field.BIGINT(),
    o_orderstatus: Field.STRING(),
    o_totalprice: Field.DECIMAL(15, 2),
    o_orderdate: Field.DATE(),
    o_orderpriority: Field.STRING(),
    o_clerk: Field.STRING(),
    o_shippriority: Field.INT(),
    o_comment: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["o_orderkey"] },
  watermark: {
    column: "event_time",
    expression: "event_time - INTERVAL '5' SECOND",
  },
})

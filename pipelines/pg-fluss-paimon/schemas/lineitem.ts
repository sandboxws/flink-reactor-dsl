import { Field, Schema } from "@/core/schema.js"

export const LineitemSchema = Schema({
  fields: {
    l_orderkey: Field.BIGINT(),
    l_partkey: Field.BIGINT(),
    l_suppkey: Field.BIGINT(),
    l_linenumber: Field.INT(),
    l_quantity: Field.DECIMAL(15, 2),
    l_extendedprice: Field.DECIMAL(15, 2),
    l_discount: Field.DECIMAL(15, 2),
    l_tax: Field.DECIMAL(15, 2),
    l_returnflag: Field.STRING(),
    l_linestatus: Field.STRING(),
    l_shipdate: Field.DATE(),
    l_commitdate: Field.DATE(),
    l_receiptdate: Field.DATE(),
    l_shipinstruct: Field.STRING(),
    l_shipmode: Field.STRING(),
    l_comment: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["l_orderkey", "l_linenumber"] },
  watermark: {
    column: "event_time",
    expression: "event_time - INTERVAL '5' SECOND",
  },
})

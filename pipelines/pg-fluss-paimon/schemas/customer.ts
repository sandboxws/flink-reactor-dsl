import { Field, Schema } from "@/core/schema.js"

export const CustomerSchema = Schema({
  fields: {
    c_custkey: Field.BIGINT(),
    c_name: Field.STRING(),
    c_address: Field.STRING(),
    c_nationkey: Field.BIGINT(),
    c_phone: Field.STRING(),
    c_acctbal: Field.DECIMAL(15, 2),
    c_mktsegment: Field.STRING(),
    c_comment: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["c_custkey"] },
  watermark: {
    column: "event_time",
    expression: "event_time - INTERVAL '5' SECOND",
  },
})

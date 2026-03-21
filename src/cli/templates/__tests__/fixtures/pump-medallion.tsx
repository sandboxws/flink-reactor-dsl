import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { DataGenSource } from "@/components/sources"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const OrderCdcSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    customerId: Field.STRING(),
    product: Field.STRING(),
    amount: Field.DOUBLE(),
    status: Field.STRING(),
    updatedAt: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["orderId"] },
  watermark: {
    column: "updatedAt",
    expression: "updatedAt - INTERVAL '5' SECOND",
  },
})

export default (
  <Pipeline name="pump-medallion" mode="streaming">
    <DataGenSource schema={OrderCdcSchema} rowsPerSecond={2000} />
    <KafkaSink topic="orders.cdc" bootstrapServers="kafka:9092" />
  </Pipeline>
)

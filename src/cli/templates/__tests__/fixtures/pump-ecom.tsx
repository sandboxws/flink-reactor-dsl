import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { DataGenSource } from "@/components/sources"
import { StatementSet } from "@/components/statement-set"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const OrderSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    customerId: Field.STRING(),
    amount: Field.DOUBLE(),
    currency: Field.STRING(),
    status: Field.STRING(),
    orderTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "orderTime",
    expression: "orderTime - INTERVAL '5' SECOND",
  },
})

const OrderItemSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    productId: Field.STRING(),
    quantity: Field.INT(),
    unitPrice: Field.DOUBLE(),
    itemTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "itemTime",
    expression: "itemTime - INTERVAL '5' SECOND",
  },
})

const ProductSchema = Schema({
  fields: {
    productId: Field.STRING(),
    name: Field.STRING(),
    category: Field.STRING(),
    price: Field.DOUBLE(),
    stock: Field.INT(),
    updateTime: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["productId"] },
})

const CustomerSchema = Schema({
  fields: {
    customerId: Field.STRING(),
    name: Field.STRING(),
    email: Field.STRING(),
    tier: Field.STRING(),
    updateTime: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["customerId"] },
})

export default (
  <Pipeline name="pump-ecom" mode="streaming">
    <StatementSet>
      <DataGenSource schema={OrderSchema} rowsPerSecond={2000} />
      <KafkaSink topic="ecom.orders" bootstrapServers="kafka:9092" />

      <DataGenSource schema={OrderItemSchema} rowsPerSecond={6000} />
      <KafkaSink topic="ecom.order-items" bootstrapServers="kafka:9092" />

      <DataGenSource schema={ProductSchema} rowsPerSecond={200} />
      <KafkaSink topic="ecom.products" bootstrapServers="kafka:9092" />

      <DataGenSource schema={CustomerSchema} rowsPerSecond={100} />
      <KafkaSink topic="ecom.customers" bootstrapServers="kafka:9092" />
    </StatementSet>
  </Pipeline>
)

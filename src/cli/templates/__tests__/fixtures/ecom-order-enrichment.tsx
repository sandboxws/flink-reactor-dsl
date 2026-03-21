import { IntervalJoin, TemporalJoin } from "@/components/joins"
import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
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

const orders = KafkaSource({
  topic: "ecom.orders",
  schema: OrderSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "ecom-enrichment-orders",
})

const items = KafkaSource({
  topic: "ecom.order-items",
  schema: OrderItemSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "ecom-enrichment-items",
})

const products = KafkaSource({
  topic: "ecom.products",
  schema: ProductSchema,
  format: "debezium-json",
  bootstrapServers: "kafka:9092",
  consumerGroup: "ecom-enrichment-products",
})

const ordersWithItems = IntervalJoin({
  left: orders,
  right: items,
  on: "orders.orderId = items.orderId",
  interval: {
    from: "orders.orderTime",
    to: "orders.orderTime + INTERVAL '30' SECOND",
  },
})

const enriched = TemporalJoin({
  stream: ordersWithItems,
  temporal: products,
  on: "productId = productId",
  asOf: "orderTime",
})

export default (
  <Pipeline name="ecom-order-enrichment" mode="streaming">
    {orders}
    {items}
    {products}
    {enriched}
    <KafkaSink topic="ecom.order-enriched" bootstrapServers="kafka:9092" />
  </Pipeline>
)

import { TemporalJoin } from "@/components/joins"
import { Pipeline } from "@/components/pipeline"
import { Route } from "@/components/route"
import { JdbcSink, KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const GroceryOrderSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    storeId: Field.STRING(),
    customerId: Field.STRING(),
    itemCount: Field.INT(),
    totalAmount: Field.DOUBLE(),
    orderTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "orderTime",
    expression: "orderTime - INTERVAL '5' SECOND",
  },
})

const StoreInventorySchema = Schema({
  fields: {
    storeId: Field.STRING(),
    productId: Field.STRING(),
    stockLevel: Field.INT(),
    substitutionId: Field.STRING(),
    updateTime: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["storeId", "productId"] },
})

const orders = KafkaSource({
  topic: "grocery.orders",
  schema: GroceryOrderSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "grocery-fulfillment",
})

const inventory = KafkaSource({
  topic: "grocery.store-inventory",
  schema: StoreInventorySchema,
  format: "debezium-json",
  bootstrapServers: "kafka:9092",
  consumerGroup: "grocery-inventory",
})

const enriched = TemporalJoin({
  stream: orders,
  temporal: inventory,
  on: "storeId = storeId",
  asOf: "orderTime",
})

export default (
  <Pipeline name="grocery-order-fulfillment" mode="streaming">
    {orders}
    {inventory}
    {enriched}
    <Route>
      <Route.Branch condition="stockLevel > 0">
        <JdbcSink
          table="fulfillment_queue"
          url="jdbc:postgresql://postgres:5432/flink_sink"
        />
      </Route.Branch>
      <Route.Branch condition="stockLevel = 0">
        <KafkaSink
          topic="grocery.substitution-alerts"
          bootstrapServers="kafka:9092"
        />
      </Route.Branch>
    </Route>
  </Pipeline>
)

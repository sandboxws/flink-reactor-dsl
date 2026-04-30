import { IntervalJoin } from "@/components/joins"
import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Map } from "@/components/transforms"
import { Field, Schema } from "@/core/schema"

const OrderSchema = Schema({
  fields: {
    order_id: Field.STRING(),
    user_id: Field.STRING(),
    product_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    order_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "order_time",
    expression: "order_time - INTERVAL '10' SECOND",
  },
})

const ShipmentSchema = Schema({
  fields: {
    shipment_id: Field.STRING(),
    order_id: Field.STRING(),
    carrier: Field.STRING(),
    ship_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "ship_time",
    expression: "ship_time - INTERVAL '10' SECOND",
  },
})

const orders = (
  <KafkaSource
    topic="orders"
    bootstrapServers="kafka:9092"
    schema={OrderSchema}
  />
)

const shipments = (
  <KafkaSource
    topic="shipments"
    bootstrapServers="kafka:9092"
    schema={ShipmentSchema}
  />
)

export default (
  <Pipeline name="order-fulfillment" parallelism={8}>
    <IntervalJoin
      left={orders}
      right={shipments}
      on="`orders`.order_id = `shipments`.order_id"
      interval={{
        from: "order_time",
        to: "order_time + INTERVAL '7' DAY",
      }}
    />
    <Map
      select={{
        order_id: "order_id",
        user_id: "user_id",
        amount: "amount",
        carrier: "carrier",
        fulfillment_time: "TIMESTAMPDIFF(SECOND, order_time, ship_time)",
      }}
    />
    <KafkaSink topic="order_fulfillment" />
  </Pipeline>
)

import { AddField } from "@/components/field-transforms"
import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const OrderSchema = Schema({
  fields: {
    order_id: Field.STRING(),
    product_id: Field.STRING(),
    quantity: Field.INT(),
    unit_price: Field.DECIMAL(10, 2),
    order_time: Field.TIMESTAMP(3),
  },
})

export default (
  <Pipeline name="enrich-orders" parallelism={8}>
    <KafkaSource
      topic="orders"
      bootstrapServers="kafka:9092"
      schema={OrderSchema}
    />
    <AddField
      columns={{
        total_price: "quantity * unit_price",
        is_high_value: "quantity * unit_price > 500",
      }}
    />
    <KafkaSink topic="enriched_orders" />
  </Pipeline>
)

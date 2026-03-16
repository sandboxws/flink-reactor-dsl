import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { FlatMap } from "@/components/transforms"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const OrderSchema = Schema({
  fields: {
    order_id: Field.STRING(),
    customer_id: Field.STRING(),
    line_items: Field.ARRAY(Field.STRING()),
    order_time: Field.TIMESTAMP(3),
  },
})

export default (
  <Pipeline name="unnest-line-items" parallelism={4}>
    <KafkaSource
      topic="orders"
      bootstrapServers="kafka:9092"
      schema={OrderSchema}
    />
    <FlatMap
      unnest="line_items"
      as={{
        product_id: "STRING",
        quantity: "INT",
        price: "DECIMAL(10, 2)",
      }}
    />
    <KafkaSink topic="order_line_items" />
  </Pipeline>
)

import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Filter } from "@/components/transforms"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const ProductSchema = Schema({
  fields: {
    id: Field.INT(),
    name: Field.STRING(),
    category: Field.STRING(),
    price: Field.DOUBLE(),
    quantity: Field.INT(),
  },
  primaryKey: { columns: ["id"] },
})

export default (
  <Pipeline name="hello-world">
    <KafkaSource
      topic="cdc.inventory.products"
      schema={ProductSchema}
      format="debezium-json"
      bootstrapServers="kafka:9092"
      consumerGroup="hello-world"
    />
    <Filter condition="quantity > 0" />
    <KafkaSink topic="in-stock-products" bootstrapServers="kafka:9092" />
  </Pipeline>
)

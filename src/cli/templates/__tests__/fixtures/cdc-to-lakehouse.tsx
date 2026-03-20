import { PaimonCatalog } from "@/components/catalogs"
import { Pipeline } from "@/components/pipeline"
import { PaimonSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const OrderSchema = Schema({
  fields: {
    orderId: Field.BIGINT(),
    customerId: Field.BIGINT(),
    product: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    status: Field.STRING(),
    createdAt: Field.TIMESTAMP(3),
    updatedAt: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["orderId"] },
})

const lakehouse = PaimonCatalog({
  name: "lakehouse",
  warehouse: "s3://my-bucket/warehouse",
})

export default (
  <Pipeline name="cdc-to-lakehouse">
    {lakehouse.node}
    <KafkaSource
      topic="dbserver1.inventory.orders"
      schema={OrderSchema}
      format="debezium-json"
      bootstrapServers="kafka:9092"
      consumerGroup="cdc-lakehouse"
    />
    <PaimonSink
      catalog={lakehouse.handle}
      database="inventory"
      table="orders"
      primaryKey={["orderId"]}
    />
  </Pipeline>
)

import { IcebergCatalog } from "@/components/catalogs"
import { Pipeline } from "@/components/pipeline"
import { IcebergSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Deduplicate } from "@/components/transforms"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const OrderCleanSchema = Schema({
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

const iceberg = IcebergCatalog({
  name: "lakehouse",
  catalogType: "rest",
  uri: "http://iceberg-rest:8181",
})

export default (
  <Pipeline name="medallion-silver" mode="streaming">
    {iceberg.node}
    <KafkaSource
      topic="orders.cdc"
      schema={OrderCleanSchema}
      format="debezium-json"
      bootstrapServers="kafka:9092"
      consumerGroup="medallion-silver"
    />
    <Deduplicate key={["orderId"]} order="updatedAt" keep="last" />
    <IcebergSink
      catalog={iceberg.handle}
      database="silver"
      table="orders_clean"
      primaryKey={["orderId"]}
      formatVersion={2}
      upsertEnabled
    />
  </Pipeline>
)

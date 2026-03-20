import { IcebergCatalog } from "@/components/catalogs"
import { Pipeline } from "@/components/pipeline"
import { IcebergSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate } from "@/components/transforms"
import { TumbleWindow } from "@/components/windows"
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

const iceberg = IcebergCatalog({
  name: "lakehouse",
  catalogType: "rest",
  uri: "http://iceberg-rest:8181",
})

export default (
  <Pipeline name="medallion-gold" mode="streaming">
    {iceberg.node}
    <KafkaSource
      topic="orders.cdc"
      schema={OrderCdcSchema}
      format="debezium-json"
      bootstrapServers="kafka:9092"
      consumerGroup="medallion-gold"
    />
    <TumbleWindow size="1 HOUR" on="updatedAt" />
    <Aggregate
      groupBy={["product"]}
      select={{
        product: "product",
        totalRevenue: "SUM(amount)",
        orderCount: "COUNT(*)",
        windowStart: "window_start",
        windowEnd: "window_end",
      }}
    />
    <IcebergSink
      catalog={iceberg.handle}
      database="gold"
      table="revenue_hourly"
    />
  </Pipeline>
)

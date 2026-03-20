import { Pipeline } from "@/components/pipeline"
import { Route } from "@/components/route"
import { JdbcSink, KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate } from "@/components/transforms"
import { SlideWindow } from "@/components/windows"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const OrderSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    customerId: Field.STRING(),
    amount: Field.DOUBLE(),
    currency: Field.STRING(),
    category: Field.STRING(),
    status: Field.STRING(),
    orderTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "orderTime",
    expression: "orderTime - INTERVAL '5' SECOND",
  },
})

export default (
  <Pipeline name="ecom-revenue-analytics" mode="streaming">
    <KafkaSource
      topic="ecom.order-enriched"
      schema={OrderSchema}
      bootstrapServers="kafka:9092"
      consumerGroup="ecom-revenue"
    />
    <Route>
      <Route.Branch condition="1 = 1">
        <SlideWindow size="5 MINUTE" slide="1 MINUTE" on="orderTime" />
        <Aggregate
          groupBy={["category"]}
          select={{
            category: "category",
            totalRevenue: "SUM(amount)",
            orderCount: "COUNT(*)",
            windowStart: "window_start",
            windowEnd: "window_end",
          }}
        />
        <JdbcSink
          table="revenue_by_category"
          url="jdbc:postgresql://postgres:5432/flink_sink"
        />
      </Route.Branch>
      <Route.Branch condition="amount > 500">
        <KafkaSink topic="ecom.revenue-alerts" bootstrapServers="kafka:9092" />
      </Route.Branch>
    </Route>
  </Pipeline>
)

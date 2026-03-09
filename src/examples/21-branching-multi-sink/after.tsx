import { Pipeline } from "@/components/pipeline"
import { Route } from "@/components/route"
import { JdbcSink, KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate, Map } from "@/components/transforms"
import { TumbleWindow } from "@/components/windows"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const OrderSchema = Schema({
  fields: {
    order_id: Field.STRING(),
    customer_id: Field.STRING(),
    product_id: Field.STRING(),
    quantity: Field.INT(),
    unit_price: Field.DECIMAL(10, 2),
    order_time: Field.TIMESTAMP(3),
    region: Field.STRING(),
    order_status: Field.STRING(),
  },
  watermark: {
    column: "order_time",
    expression: "order_time - INTERVAL '10' SECOND",
  },
})

export default (
  <Pipeline name="order-routing" parallelism={16}>
    <KafkaSource
      topic="raw_orders"
      bootstrapServers="kafka:9092"
      schema={OrderSchema}
    />
    <Map
      select={{
        order_id: "order_id",
        customer_id: "customer_id",
        product_id: "product_id",
        total_amount: "quantity * unit_price",
        order_time: "order_time",
        region: "region",
        order_status: "order_status",
      }}
    />
    <Route>
      <Route.Branch condition="total_amount >= 1000">
        <KafkaSink topic="high_value_orders" />
      </Route.Branch>
      <Route.Branch condition="order_status = 'FAILED'">
        <KafkaSink topic="failed_orders_alerts" />
      </Route.Branch>
      <Route.Default>
        <TumbleWindow size="1 minute" on="order_time">
          <Aggregate
            groupBy={["region"]}
            select={{
              region: "region",
              revenue: "SUM(total_amount)",
              order_count: "COUNT(*)",
            }}
          />
        </TumbleWindow>
        <JdbcSink
          url="jdbc:postgresql://db:5433/analytics"
          table="regional_metrics_per_minute"
        />
      </Route.Default>
    </Route>
  </Pipeline>
)

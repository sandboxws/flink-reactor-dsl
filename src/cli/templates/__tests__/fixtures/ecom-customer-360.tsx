import { LookupJoin } from "@/components/joins"
import { Pipeline } from "@/components/pipeline"
import { JdbcSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate } from "@/components/transforms"
import { SessionWindow } from "@/components/windows"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const OrderSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    customerId: Field.STRING(),
    amount: Field.DOUBLE(),
    currency: Field.STRING(),
    status: Field.STRING(),
    orderTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "orderTime",
    expression: "orderTime - INTERVAL '5' SECOND",
  },
})

const orders = KafkaSource({
  topic: "ecom.orders",
  schema: OrderSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "ecom-customer360",
})

export default (
  <Pipeline name="ecom-customer-360" mode="streaming">
    {orders}
    {LookupJoin({
      input: orders,
      table: "customers",
      url: "jdbc:postgresql://postgres:5432/flink_sink",
      on: "customerId = customerId",
    })}
    <SessionWindow gap="30 MINUTE" on="orderTime" />
    <Aggregate
      groupBy={["customerId", "name", "tier"]}
      select={{
        customerId: "customerId",
        customerName: "name",
        tier: "tier",
        sessionOrders: "COUNT(*)",
        sessionRevenue: "SUM(amount)",
        windowStart: "SESSION_START",
        windowEnd: "SESSION_END",
      }}
    />
    <JdbcSink
      table="customer_sessions"
      url="jdbc:postgresql://postgres:5432/flink_sink"
      upsertMode
      keyFields={["customerId"]}
    />
  </Pipeline>
)

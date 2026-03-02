import { TemporalJoin } from "../../components/joins"
import { Pipeline } from "../../components/pipeline"
import { KafkaSink } from "../../components/sinks"
import { KafkaSource } from "../../components/sources"
import { Map } from "../../components/transforms"
import { createElement } from "../../core/jsx-runtime"
import { Field, Schema } from "../../core/schema"

const CurrencyRateSchema = Schema({
  fields: {
    currency_pair: Field.STRING(),
    exchange_rate: Field.DECIMAL(12, 6),
    rate_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "rate_time",
    expression: "rate_time - INTERVAL '1' SECOND",
  },
  primaryKey: { columns: ["currency_pair"] },
})

const ForexOrderSchema = Schema({
  fields: {
    order_id: Field.STRING(),
    currency_pair: Field.STRING(),
    amount: Field.DECIMAL(12, 2),
    order_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "order_time",
    expression: "order_time - INTERVAL '1' SECOND",
  },
})

const rates = (
  <KafkaSource
    topic="currency_rates"
    bootstrapServers="kafka:9092"
    schema={CurrencyRateSchema}
  />
)

const orders = (
  <KafkaSource
    topic="forex_orders"
    bootstrapServers="kafka:9092"
    schema={ForexOrderSchema}
  />
)

export default (
  <Pipeline name="forex-conversion" parallelism={8}>
    <TemporalJoin
      stream={orders}
      temporal={rates}
      on="currency_pair = currency_pair"
      asOf="order_time"
    />
    <Map
      select={{
        order_id: "order_id",
        currency_pair: "currency_pair",
        amount: "amount",
        order_time: "order_time",
        exchange_rate: "exchange_rate",
        converted_amount: "amount * exchange_rate",
      }}
    />
    <KafkaSink topic="converted_orders" />
  </Pipeline>
)

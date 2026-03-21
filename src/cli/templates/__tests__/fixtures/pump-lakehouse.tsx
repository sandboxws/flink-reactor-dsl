import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { DataGenSource } from "@/components/sources"
import { StatementSet } from "@/components/statement-set"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const EventSchema = Schema({
  fields: {
    eventId: Field.STRING(),
    userId: Field.STRING(),
    eventType: Field.STRING(),
    payload: Field.STRING(),
    eventTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "eventTime",
    expression: "eventTime - INTERVAL '5' SECOND",
  },
})

const ClickstreamSchema = Schema({
  fields: {
    sessionId: Field.STRING(),
    userId: Field.STRING(),
    pageUrl: Field.STRING(),
    referrer: Field.STRING(),
    userAgent: Field.STRING(),
    clickTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "clickTime",
    expression: "clickTime - INTERVAL '5' SECOND",
  },
})

const TransactionSchema = Schema({
  fields: {
    txnId: Field.STRING(),
    accountId: Field.STRING(),
    amount: Field.DOUBLE(),
    currency: Field.STRING(),
    txnType: Field.STRING(),
    txnTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "txnTime",
    expression: "txnTime - INTERVAL '5' SECOND",
  },
})

export default (
  <Pipeline name="pump-lakehouse" mode="streaming">
    <StatementSet>
      <DataGenSource schema={EventSchema} rowsPerSecond={3000} />
      <KafkaSink topic="lake.events" bootstrapServers="kafka:9092" />

      <DataGenSource schema={ClickstreamSchema} rowsPerSecond={5000} />
      <KafkaSink topic="lake.clickstream" bootstrapServers="kafka:9092" />

      <DataGenSource schema={TransactionSchema} rowsPerSecond={1000} />
      <KafkaSink topic="lake.transactions" bootstrapServers="kafka:9092" />
    </StatementSet>
  </Pipeline>
)

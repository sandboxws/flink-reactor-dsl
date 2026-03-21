import { IcebergCatalog } from "@/components/catalogs"
import { Pipeline } from "@/components/pipeline"
import { IcebergSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
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

const iceberg = IcebergCatalog({
  name: "lakehouse",
  catalogType: "rest",
  uri: "http://iceberg-rest:8181",
})

export default (
  <Pipeline name="lakehouse-ingest" mode="streaming">
    {iceberg.node}

    <StatementSet>
      <KafkaSource
        topic="lake.events"
        schema={EventSchema}
        bootstrapServers="kafka:9092"
        consumerGroup="lake-ingest-events"
      />
      <IcebergSink catalog={iceberg.handle} database="raw" table="events" />

      <KafkaSource
        topic="lake.clickstream"
        schema={ClickstreamSchema}
        bootstrapServers="kafka:9092"
        consumerGroup="lake-ingest-clicks"
      />
      <IcebergSink
        catalog={iceberg.handle}
        database="raw"
        table="clickstream"
      />

      <KafkaSource
        topic="lake.transactions"
        schema={TransactionSchema}
        bootstrapServers="kafka:9092"
        consumerGroup="lake-ingest-txn"
      />
      <IcebergSink
        catalog={iceberg.handle}
        database="raw"
        table="transactions"
      />
    </StatementSet>
  </Pipeline>
)

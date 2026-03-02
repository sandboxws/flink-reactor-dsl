import { MatchRecognize } from "../../components/cep"
import { Pipeline } from "../../components/pipeline"
import { KafkaSink } from "../../components/sinks"
import { KafkaSource } from "../../components/sources"
import { Map } from "../../components/transforms"
import { createElement } from "../../core/jsx-runtime"
import { Field, Schema } from "../../core/schema"

const TransactionSchema = Schema({
  fields: {
    transaction_id: Field.STRING(),
    card_id: Field.STRING(),
    merchant_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    location: Field.STRING(),
    transaction_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "transaction_time",
    expression: "transaction_time - INTERVAL '30' SECOND",
  },
})

const transactions = (
  <KafkaSource
    topic="card_transactions"
    bootstrapServers="kafka:9092"
    schema={TransactionSchema}
  />
)

export default (
  <Pipeline name="fraud-detection" parallelism={16}>
    <MatchRecognize
      input={transactions}
      partitionBy={["card_id"]}
      orderBy="transaction_time"
      pattern="A B+ C"
      after="NEXT_ROW"
      define={{
        A: "A.amount > 1000",
        B: "B.location <> A.location",
        C: "C.amount > 500 AND C.location <> B.location",
      }}
      measures={{
        card_id: "A.card_id",
        first_txn: "A.transaction_id",
        last_txn: "C.transaction_id",
        total_amount: "A.amount + SUM(B.amount) + C.amount",
        txn_count: "COUNT(B.*) + 2",
      }}
    />
    <Map
      select={{
        card_id: "card_id",
        first_txn: "first_txn",
        last_txn: "last_txn",
        total_amount: "total_amount",
        txn_count: "txn_count",
        fraud_type: "'RAPID_GEO_CHANGE'",
      }}
    />
    <KafkaSink topic="fraud_alerts" />
  </Pipeline>
)

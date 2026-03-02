import { Pipeline } from "../../components/pipeline"
import { JdbcSink } from "../../components/sinks"
import { KafkaSource } from "../../components/sources"
import { Aggregate } from "../../components/transforms"
import { createElement } from "../../core/jsx-runtime"
import { Field, Schema } from "../../core/schema"

const TransactionSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    transaction_time: Field.TIMESTAMP(3),
    category: Field.STRING(),
  },
  watermark: {
    column: "transaction_time",
    expression: "transaction_time - INTERVAL '5' SECOND",
  },
})

export default (
  <Pipeline name="user-totals" parallelism={8}>
    <KafkaSource
      topic="transactions"
      bootstrapServers="kafka:9092"
      schema={TransactionSchema}
    />
    <Aggregate
      groupBy={["user_id"]}
      select={{
        user_id: "user_id",
        total_amount: "SUM(amount)",
        txn_count: "COUNT(*)",
      }}
    />
    <JdbcSink url="jdbc:postgresql://db:5432/analytics" table="user_totals" />
  </Pipeline>
)

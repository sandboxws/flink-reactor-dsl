import { Pipeline } from "@/components/pipeline"
import { Route } from "@/components/route"
import { JdbcSink, KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate } from "@/components/transforms"
import { TumbleWindow } from "@/components/windows"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const TransactionSchema = Schema({
  fields: {
    txnId: Field.STRING(),
    accountId: Field.STRING(),
    amount: Field.DOUBLE(),
    currency: Field.STRING(),
    merchant: Field.STRING(),
    country: Field.STRING(),
    txnTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "txnTime",
    expression: "txnTime - INTERVAL '5' SECOND",
  },
})

export default (
  <Pipeline name="bank-compliance-agg" mode="streaming">
    <KafkaSource
      topic="bank.transactions"
      schema={TransactionSchema}
      bootstrapServers="kafka:9092"
      consumerGroup="bank-compliance"
    />
    <TumbleWindow size="1 HOUR" on="txnTime" />
    <Route>
      <Route.Branch condition="1 = 1">
        <Aggregate
          groupBy={["accountId"]}
          select={{
            accountId: "accountId",
            totalAmount: "SUM(amount)",
            txnCount: "COUNT(*)",
            windowEnd: "window_end",
          }}
        />
        <JdbcSink
          table="large_txn_report"
          url="jdbc:postgresql://postgres:5432/flink_sink"
        />
      </Route.Branch>
      <Route.Branch condition="1 = 1">
        <Aggregate
          groupBy={["country"]}
          select={{
            country: "country",
            crossBorderCount: "COUNT(*)",
            totalVolume: "SUM(amount)",
            windowEnd: "window_end",
          }}
        />
        <JdbcSink
          table="cross_border_report"
          url="jdbc:postgresql://postgres:5432/flink_sink"
        />
      </Route.Branch>
      <Route.Branch condition="amount > 10000">
        <KafkaSink
          topic="bank.compliance-reports"
          bootstrapServers="kafka:9092"
        />
      </Route.Branch>
    </Route>
  </Pipeline>
)

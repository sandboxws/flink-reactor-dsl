import { MatchRecognize } from "@/components/cep"
import { TemporalJoin } from "@/components/joins"
import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
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

const AccountSchema = Schema({
  fields: {
    accountId: Field.STRING(),
    name: Field.STRING(),
    tier: Field.STRING(),
    status: Field.STRING(),
    balance: Field.DOUBLE(),
    updateTime: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["accountId"] },
})

const transactions = KafkaSource({
  topic: "bank.transactions",
  schema: TransactionSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "bank-fraud",
})

const accounts = KafkaSource({
  topic: "bank.accounts",
  schema: AccountSchema,
  format: "debezium-json",
  bootstrapServers: "kafka:9092",
  consumerGroup: "bank-fraud-accounts",
})

const fraudPatterns = MatchRecognize({
  input: transactions,
  pattern: "high{3,}",
  define: { high: "amount > 1000" },
  measures: {
    accountId: "FIRST(accountId)",
    txnCount: "COUNT(*)",
    totalAmount: "SUM(amount)",
    firstTxn: "FIRST(txnTime)",
    lastTxn: "LAST(txnTime)",
  },
})

const enriched = TemporalJoin({
  stream: fraudPatterns,
  temporal: accounts,
  on: "accountId = accountId",
  asOf: "lastTxn",
})

export default (
  <Pipeline name="bank-fraud-detection" mode="streaming">
    {transactions}
    {accounts}
    {enriched}
    <KafkaSink topic="bank.fraud-alerts" bootstrapServers="kafka:9092" />
  </Pipeline>
)

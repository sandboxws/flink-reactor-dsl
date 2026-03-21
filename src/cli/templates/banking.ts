import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getBankingTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "flink-reactor.config.ts",
      content: `import { defineConfig } from '@flink-reactor/dsl';

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  environments: {
    minikube: {
      cluster: { url: 'http://localhost:8081' },
      kafka: { bootstrapServers: 'kafka:9092' },
      sim: {
        init: {
          kafka: {
            topics: [
              'bank.transactions',
              'bank.accounts',
              'bank.fraud-alerts',
              'bank.compliance-reports',
            ],
          },
        },
      },
      pipelines: { '*': { parallelism: 2 } },
    },
    production: {
      cluster: { url: 'https://flink-prod:8081' },
      kubernetes: { namespace: 'flink-prod' },
      pipelines: { '*': { parallelism: 4 } },
    },
  },
});
`,
    },
    {
      path: "schemas/banking.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl';

export const TransactionSchema = Schema({
  fields: {
    txnId: Field.STRING(),
    accountId: Field.STRING(),
    amount: Field.DOUBLE(),
    currency: Field.STRING(),
    merchant: Field.STRING(),
    country: Field.STRING(),
    txnTime: Field.TIMESTAMP(3),
  },
  watermark: { column: 'txnTime', expression: "txnTime - INTERVAL '5' SECOND" },
});

export const AccountSchema = Schema({
  fields: {
    accountId: Field.STRING(),
    name: Field.STRING(),
    tier: Field.STRING(),
    status: Field.STRING(),
    balance: Field.DOUBLE(),
    updateTime: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ['accountId'] },
});
`,
    },
    {
      path: "pipelines/bank-fraud-detection/index.tsx",
      content: `import {
  Pipeline, KafkaSource, KafkaSink,
  MatchRecognize, TemporalJoin,
} from '@flink-reactor/dsl';
import { TransactionSchema, AccountSchema } from '@/schemas/banking';

const transactions = KafkaSource({
  topic: "bank.transactions",
  schema: TransactionSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "bank-fraud",
});

const accounts = KafkaSource({
  topic: "bank.accounts",
  schema: AccountSchema,
  format: "debezium-json",
  bootstrapServers: "kafka:9092",
  consumerGroup: "bank-fraud-accounts",
});

const fraudPatterns = MatchRecognize({
  input: transactions,
  pattern: "high{3,}",
  define: { high: "amount > 1000" },
  measures: {
    accountId: 'FIRST(accountId)',
    txnCount: 'COUNT(*)',
    totalAmount: 'SUM(amount)',
    firstTxn: 'FIRST(txnTime)',
    lastTxn: 'LAST(txnTime)',
  },
});

const enriched = TemporalJoin({
  stream: fraudPatterns,
  temporal: accounts,
  on: "accountId = accountId",
  asOf: "lastTxn",
});

export default (
  <Pipeline
    name="bank-fraud-detection"
    mode="streaming"
    parallelism={4}
    stateBackend="rocksdb"
    checkpoint={{ interval: "30s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/bank-fraud-detection",
      "state.savepoints.dir": "s3://flink-state/savepoints/bank-fraud-detection",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    {transactions}
    {accounts}
    {enriched}
    <KafkaSink topic="bank.fraud-alerts" bootstrapServers="kafka:9092" />
  </Pipeline>
);
`,
    },
    {
      path: "pipelines/bank-compliance-agg/index.tsx",
      content: `import {
  Pipeline, KafkaSource, KafkaSink, JdbcSink,
  TumbleWindow, Aggregate, Route,
} from '@flink-reactor/dsl';
import { TransactionSchema } from '@/schemas/banking';

export default (
  <Pipeline
    name="bank-compliance-agg"
    mode="streaming"
    parallelism={4}
    stateBackend="rocksdb"
    checkpoint={{ interval: "30s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/bank-compliance-agg",
      "state.savepoints.dir": "s3://flink-state/savepoints/bank-compliance-agg",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    <KafkaSource topic="bank.transactions" schema={TransactionSchema} bootstrapServers="kafka:9092" consumerGroup="bank-compliance" />
    <TumbleWindow size="1 HOUR" on="txnTime" />
    <Route>
      <Route.Branch condition="1 = 1">
        <Aggregate groupBy={['accountId']} select={{ accountId: 'accountId', totalAmount: 'SUM(amount)', txnCount: 'COUNT(*)', windowEnd: 'WINDOW_END' }} />
        <JdbcSink table="large_txn_report" url="jdbc:postgresql://postgres:5432/flink_sink" />
      </Route.Branch>
      <Route.Branch condition="1 = 1">
        <Aggregate groupBy={['country']} select={{ country: 'country', crossBorderCount: 'COUNT(*)', totalVolume: 'SUM(amount)', windowEnd: 'WINDOW_END' }} />
        <JdbcSink table="cross_border_report" url="jdbc:postgresql://postgres:5432/flink_sink" />
      </Route.Branch>
      <Route.Branch condition="amount > 10000">
        <KafkaSink topic="bank.compliance-reports" bootstrapServers="kafka:9092" />
      </Route.Branch>
    </Route>
  </Pipeline>
);
`,
    },
  ]
}

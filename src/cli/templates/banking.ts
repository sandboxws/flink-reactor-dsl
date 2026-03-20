import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getBankingTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "schemas/banking.ts",
      content: `import { Schema, Field } from 'flink-reactor';

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
} from 'flink-reactor';
import { TransactionSchema, AccountSchema } from '@/schemas/banking';

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
    <KafkaSource topic="bank.transactions" schema={TransactionSchema} bootstrapServers="kafka:9092" consumerGroup="bank-fraud" />
    <MatchRecognize
      pattern="high{3,}"
      within="5 MINUTE"
      define={{ high: "amount > 1000" }}
      measures={{
        accountId: 'FIRST(accountId)',
        txnCount: 'COUNT(*)',
        totalAmount: 'SUM(amount)',
        firstTxn: 'FIRST(txnTime)',
        lastTxn: 'LAST(txnTime)',
      }}
    />
    <TemporalJoin
      rightSource={<KafkaSource topic="bank.accounts" schema={AccountSchema} format="debezium-json" bootstrapServers="kafka:9092" consumerGroup="bank-fraud-accounts" />}
      on="accountId"
    />
    <KafkaSink topic="bank.fraud-alerts" bootstrapServers="kafka:9092" />
  </Pipeline>
);
`,
    },
    {
      path: "pipelines/bank-compliance-agg/index.tsx",
      content: `import {
  Pipeline, KafkaSource, KafkaSink, JdbcSink,
  TumbleWindow, Aggregate, Filter, StatementSet,
} from 'flink-reactor';
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
    <StatementSet>
      <Aggregate groupBy={['accountId']} select={{ accountId: 'accountId', totalAmount: 'SUM(amount)', txnCount: 'COUNT(*)', windowEnd: 'WINDOW_END' }} />
      <JdbcSink table="large_txn_report" url="jdbc:postgresql://postgres:5432/flink_sink" />

      <Aggregate groupBy={['country']} select={{ country: 'country', crossBorderCount: 'COUNT(*)', totalVolume: 'SUM(amount)', windowEnd: 'WINDOW_END' }} />
      <JdbcSink table="cross_border_report" url="jdbc:postgresql://postgres:5432/flink_sink" />

      <Filter condition="amount > 10000" />
      <KafkaSink topic="bank.compliance-reports" bootstrapServers="kafka:9092" />
    </StatementSet>
  </Pipeline>
);
`,
    },
  ]
}

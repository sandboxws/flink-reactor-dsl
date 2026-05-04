import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import {
  pipelineReadme,
  sharedFiles,
  templatePipelineTestStub,
  templateReadme,
} from "./shared.js"

export function getBankingTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "flink-reactor.config.ts",
      content: `import { defineConfig } from '@flink-reactor/dsl';

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  // Kafka for transaction streams; Postgres for the JDBC sinks (fraud + compliance).
  services: { kafka: { bootstrapServers: 'kafka:9092' }, postgres: {} },

  environments: {
    minikube: {
      cluster: { url: 'http://localhost:8081' },
      sim: {
        init: {
          kafka: {
            topics: ['bank.fraud-alerts', 'bank.compliance-reports'],
            catalogs: [{
              name: 'banking',
              tables: [
                {
                  table: 'transactions',
                  topic: 'bank.transactions',
                  format: 'json',
                  columns: {
                    txnId: 'STRING',
                    accountId: 'STRING',
                    amount: 'DOUBLE',
                    currency: 'STRING',
                    merchant: 'STRING',
                    country: 'STRING',
                    txnTime: 'TIMESTAMP(3)',
                  },
                  watermark: { column: 'txnTime', expression: "txnTime - INTERVAL '5' SECOND" },
                },
                {
                  table: 'accounts',
                  topic: 'bank.accounts',
                  format: 'debezium-json',
                  columns: {
                    accountId: 'STRING',
                    name: 'STRING',
                    tier: 'STRING',
                    status: 'STRING',
                    balance: 'DOUBLE',
                    updateTime: 'TIMESTAMP(3)',
                  },
                  primaryKey: ['accountId'],
                },
              ],
            }],
          },
          jdbc: {
            catalogs: [{
              name: 'flink_sink',
              baseUrl: 'jdbc:postgresql://postgres:5432/',
              defaultDatabase: 'flink_sink',
            }],
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
  watermark: { column: 'updateTime', expression: "updateTime - INTERVAL '5' SECOND" },
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
  orderBy: "txnTime",
  pattern: "high{3,}?",
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
      <Route.Branch condition="true">
        <Aggregate groupBy={['accountId']} select={{ accountId: 'accountId', totalAmount: 'SUM(amount)', txnCount: 'COUNT(*)', windowEnd: 'window_end' }} />
        <JdbcSink table="large_txn_report" url="jdbc:postgresql://postgres:5432/flink_sink" />
      </Route.Branch>
      <Route.Branch condition="true">
        <Aggregate groupBy={['country']} select={{ country: 'country', crossBorderCount: 'COUNT(*)', totalVolume: 'SUM(amount)', windowEnd: 'window_end' }} />
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

    // ── Per-pipeline READMEs ──────────────────────────────────────────

    pipelineReadme({
      pipelineName: "bank-fraud-detection",
      tagline:
        "Per-account 3+ high-value transactions detected via `<MatchRecognize>`, enriched with current account state via temporal join.",
      demonstrates: [
        "`<MatchRecognize>` with `pattern: 'high{3,}?'` and `define: { high: 'amount > 1000' }` — three or more consecutive high-value transactions per account.",
        "`<TemporalJoin>` enriching each match with the current account state (`debezium-json` versioned source).",
        "`<KafkaSink>` writing the alerts to `bank.fraud-alerts`.",
      ],
      topology: `KafkaSource (transactions)             ─► MatchRecognize (pattern=high{3,}?, define amount>1000)     ─┐
                                                                                                            ├─► TemporalJoin (accountId AS OF lastTxn) ─► KafkaSink (bank.fraud-alerts)
KafkaSource (accounts, debezium-json)  ──────────────────────────────────────────────────────────────────────┘`,
      schemas: [
        "`schemas/banking.ts` — `TransactionSchema` (with `txnTime` watermark), `AccountSchema` (with `accountId` PK and `updateTime` watermark)",
      ],
      runCommand: `pnpm synth
pnpm test`,
    }),
    pipelineReadme({
      pipelineName: "bank-compliance-agg",
      tagline:
        "Hourly tumbling-window compliance fan-out: per-account aggregates, per-country aggregates, plus a Kafka alert branch for transactions over $10k.",
      demonstrates: [
        '`<TumbleWindow size="1 HOUR" on="txnTime">` for hourly windows.',
        "`<Route>` fan-out with three branches: per-account aggregate to `large_txn_report`, per-country aggregate to `cross_border_report`, individual large transactions to `bank.compliance-reports`.",
        "Two `<JdbcSink>` and one `<KafkaSink>` consuming the same windowed input via the route.",
      ],
      topology: `KafkaSource (transactions)
  └── TumbleWindow (1 HOUR, on=txnTime)
        └── Route
              ├── Branch ─► Aggregate (GROUP BY accountId — SUM, COUNT) ─► JdbcSink (large_txn_report)
              ├── Branch ─► Aggregate (GROUP BY country — COUNT, SUM)   ─► JdbcSink (cross_border_report)
              └── Branch (amount > 10000) ─► KafkaSink (bank.compliance-reports)`,
      schemas: [
        "`schemas/banking.ts` — `TransactionSchema` (with `txnTime` watermark)",
      ],
      runCommand: `pnpm synth
pnpm test`,
    }),

    // ── Tests ─────────────────────────────────────────────────────────

    templatePipelineTestStub({
      pipelineName: "bank-fraud-detection",
      loadBearingPatterns: [
        /MATCH_RECOGNIZE/i,
        /FOR SYSTEM_TIME AS OF/i,
        /bank\.fraud-alerts/,
      ],
    }),
    templatePipelineTestStub({
      pipelineName: "bank-compliance-agg",
      loadBearingPatterns: [
        /TUMBLE\(/i,
        /GROUP BY/i,
        /bank\.compliance-reports/,
      ],
    }),

    // ── Project-root README ───────────────────────────────────────────

    templateReadme({
      templateName: "banking",
      tagline:
        "Two banking pipelines: `bank-fraud-detection` (CEP via `<MatchRecognize>` over high-value transactions, enriched with current account state via `<TemporalJoin>`) and `bank-compliance-agg` (hourly tumbling-window fan-out to per-account, per-country, and per-transaction reports).",
      pipelines: [
        {
          name: "bank-fraud-detection",
          pitch:
            "MATCH_RECOGNIZE detects 3+ consecutive high-value transactions; temporal-joined to current account state.",
        },
        {
          name: "bank-compliance-agg",
          pitch:
            "Hourly tumbling-window compliance fan-out via `<Route>` → per-account, per-country, and per-transaction sinks.",
        },
      ],
      gettingStarted: ["pnpm install", "pnpm synth", "pnpm test"],
    }),
  ]
}

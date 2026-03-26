import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getLakehouseIngestionTemplates(
  opts: ScaffoldOptions,
): TemplateFile[] {
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
          iceberg: { databases: ['raw'] },
          kafka: {
            catalogs: [
              {
                name: 'lake',
                tables: [
                  {
                    table: 'events',
                    topic: 'lake.events',
                    columns: {
                      eventId: 'STRING',
                      userId: 'STRING',
                      eventType: 'STRING',
                      payload: 'STRING',
                      eventTime: 'TIMESTAMP(3)',
                    },
                    format: 'json',
                    watermark: { column: 'eventTime', expression: "eventTime - INTERVAL '5' SECOND" },
                  },
                  {
                    table: 'clickstream',
                    topic: 'lake.clickstream',
                    columns: {
                      sessionId: 'STRING',
                      userId: 'STRING',
                      pageUrl: 'STRING',
                      referrer: 'STRING',
                      userAgent: 'STRING',
                      clickTime: 'TIMESTAMP(3)',
                    },
                    format: 'json',
                    watermark: { column: 'clickTime', expression: "clickTime - INTERVAL '5' SECOND" },
                  },
                  {
                    table: 'transactions',
                    topic: 'lake.transactions',
                    columns: {
                      txnId: 'STRING',
                      accountId: 'STRING',
                      amount: 'DOUBLE',
                      currency: 'STRING',
                      txnType: 'STRING',
                      txnTime: 'TIMESTAMP(3)',
                    },
                    format: 'json',
                    watermark: { column: 'txnTime', expression: "txnTime - INTERVAL '5' SECOND" },
                  },
                ],
              },
            ],
          },
        },
      },
      pipelines: { '*': { parallelism: 4 } },
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

    // ── Schemas ──────────────────────────────────────────────────────

    {
      path: "schemas/lakehouse.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl';

export const EventSchema = Schema({
  fields: {
    eventId: Field.STRING(),
    userId: Field.STRING(),
    eventType: Field.STRING(),
    payload: Field.STRING(),
    eventTime: Field.TIMESTAMP(3),
  },
  watermark: { column: 'eventTime', expression: "eventTime - INTERVAL '5' SECOND" },
});

export const ClickstreamSchema = Schema({
  fields: {
    sessionId: Field.STRING(),
    userId: Field.STRING(),
    pageUrl: Field.STRING(),
    referrer: Field.STRING(),
    userAgent: Field.STRING(),
    clickTime: Field.TIMESTAMP(3),
  },
  watermark: { column: 'clickTime', expression: "clickTime - INTERVAL '5' SECOND" },
});

export const TransactionSchema = Schema({
  fields: {
    txnId: Field.STRING(),
    accountId: Field.STRING(),
    amount: Field.DOUBLE(),
    currency: Field.STRING(),
    txnType: Field.STRING(),
    txnTime: Field.TIMESTAMP(3),
  },
  watermark: { column: 'txnTime', expression: "txnTime - INTERVAL '5' SECOND" },
});
`,
    },

    // ── Pipeline: Multi-topic Kafka → Iceberg raw landing ───────────

    {
      path: "pipelines/lakehouse-ingest/index.tsx",
      content: `import {
  Pipeline,
  KafkaSource,
  IcebergCatalog,
  IcebergSink,
  StatementSet,
} from '@flink-reactor/dsl';
import { EventSchema, ClickstreamSchema, TransactionSchema } from '@/schemas/lakehouse';

// Iceberg REST catalog backed by SeaweedFS S3 storage
const iceberg = IcebergCatalog({
  name: "lakehouse",
  catalogType: "rest",
  uri: "http://iceberg-rest:8181",
});

export default (
  <Pipeline
    name="lakehouse-ingest"
    mode="streaming"
    parallelism={4}
    stateBackend="rocksdb"
    checkpoint={{ interval: "60s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/lakehouse-ingest",
      "state.savepoints.dir": "s3://flink-state/savepoints/lakehouse-ingest",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    {iceberg.node}

    <StatementSet>
      {/* Events → Iceberg raw_events (append-only) */}
      <KafkaSource
        topic="lake.events"
        schema={EventSchema}
        bootstrapServers="kafka:9092"
        consumerGroup="lake-ingest-events"
      />
      <IcebergSink
        catalog={iceberg.handle}
        database="raw"
        table="events"
      />

      {/* Clickstream → Iceberg raw_clickstream (append-only) */}
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

      {/* Transactions → Iceberg raw_transactions (append-only) */}
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
);
`,
    },

    // ── Data Pump: DataGen → Kafka for all lakehouse topics ─────────

    {
      path: "pipelines/pump-lakehouse/index.tsx",
      content: `import {
  Pipeline,
  DataGenSource,
  KafkaSink,
  StatementSet,
} from '@flink-reactor/dsl';
import { EventSchema, ClickstreamSchema, TransactionSchema } from '@/schemas/lakehouse';

export default (
  <Pipeline
    name="pump-lakehouse"
    mode="streaming"
    parallelism={2}
    stateBackend="rocksdb"
    checkpoint={{ interval: "60s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/pump-lakehouse",
      "state.savepoints.dir": "s3://flink-state/savepoints/pump-lakehouse",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    <StatementSet>
      <DataGenSource schema={EventSchema} rowsPerSecond={3000} />
      <KafkaSink topic="lake.events" bootstrapServers="kafka:9092" />

      <DataGenSource schema={ClickstreamSchema} rowsPerSecond={5000} />
      <KafkaSink topic="lake.clickstream" bootstrapServers="kafka:9092" />

      <DataGenSource schema={TransactionSchema} rowsPerSecond={1000} />
      <KafkaSink topic="lake.transactions" bootstrapServers="kafka:9092" />
    </StatementSet>
  </Pipeline>
);
`,
    },

    // ── README ───────────────────────────────────────────────────────

    {
      path: "README.md",
      content: `# Lakehouse Ingestion

Multi-topic Kafka → Iceberg raw landing tables.

## Pipelines

- **lakehouse-ingest**: 3 Kafka topics (events, clickstream, transactions) → 3 Iceberg tables
- **pump-lakehouse**: DataGen data pump for all 3 topics

## Prerequisites

1. Deploy the Iceberg REST catalog:
   \`\`\`bash
   kubectl apply -f deploy/minikube/07-iceberg-rest.yaml
   \`\`\`

2. Create the Iceberg database (via Flink SQL Gateway):
   \`\`\`sql
   CREATE CATALOG lakehouse WITH (
     'type' = 'iceberg',
     'catalog-type' = 'rest',
     'uri' = 'http://iceberg-rest:8181'
   );
   USE CATALOG lakehouse;
   CREATE DATABASE IF NOT EXISTS raw;
   \`\`\`

## Getting Started

\`\`\`bash
pnpm install
flink-reactor synth          # Preview generated SQL
flink-reactor deploy --env dev
\`\`\`
`,
    },

    // ── Tests ────────────────────────────────────────────────────────

    {
      path: "tests/pipelines/lakehouse-ingest.test.ts",
      content: `import { describe, it } from 'vitest';

describe('lakehouse-ingest pipeline', () => {
  it.todo('synthesizes valid Flink SQL with Iceberg catalog and 3 sink tables');
});
`,
    },
  ]
}

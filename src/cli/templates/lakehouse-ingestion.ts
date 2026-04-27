import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import {
  pipelineReadme,
  sharedFiles,
  templatePipelineTestStub,
  templateReadme,
} from "./shared.js"

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
  uri: "http://lakekeeper.localtest.me:8181/catalog",
  warehouse: "flink-warehouse",
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

    // ── Per-pipeline READMEs ──────────────────────────────────────────

    pipelineReadme({
      pipelineName: "lakehouse-ingest",
      tagline:
        "Three Kafka topics (events, clickstream, transactions) ingested into three Iceberg raw-landing tables under one shared catalog.",
      demonstrates: [
        "`<IcebergCatalog>` declaration referencing a REST catalog backed by SeaweedFS S3 storage.",
        "`<StatementSet>` running three `<KafkaSource>` → `<IcebergSink>` flows in a single Flink job — efficient when many lightweight ingest jobs would otherwise be deployed separately.",
        "All three sinks land in `raw.<table>` (default Iceberg format v1, append-only — bronze layer).",
      ],
      topology: `IcebergCatalog (REST → SeaweedFS S3 warehouse)
  └── StatementSet
        ├── KafkaSource (lake.events)        ─► IcebergSink (raw.events)
        ├── KafkaSource (lake.clickstream)   ─► IcebergSink (raw.clickstream)
        └── KafkaSource (lake.transactions)  ─► IcebergSink (raw.transactions)`,
      schemas: [
        "`schemas/lakehouse.ts` — `EventSchema`, `ClickstreamSchema`, `TransactionSchema` (each with their own watermark)",
      ],
      runCommand: `pnpm synth
pnpm test`,
    }),
    pipelineReadme({
      pipelineName: "pump-lakehouse",
      tagline:
        "Internal data-generator pipeline that pumps synthetic events, clickstream, and transactions into the corresponding Kafka topics.",
      demonstrates: [
        "Three `<DataGenSource>` driving three `<KafkaSink>` inside a single `<StatementSet>`.",
        "Bundle-internal pump pattern (no upstream Apache Flink source — exists only to feed `lakehouse-ingest` on the local sim).",
      ],
      topology: `DataGenSource (Event)        ─► KafkaSink (lake.events)
DataGenSource (Clickstream)  ─► KafkaSink (lake.clickstream)
DataGenSource (Transaction)  ─► KafkaSink (lake.transactions)`,
      schemas: ["`schemas/lakehouse.ts` — same schemas the consumer reads"],
      runCommand: `pnpm synth
pnpm test`,
    }),

    // ── Tests ─────────────────────────────────────────────────────────

    templatePipelineTestStub({
      pipelineName: "lakehouse-ingest",
      loadBearingPatterns: [/iceberg/i, /INSERT INTO/i, /raw/i],
    }),
    templatePipelineTestStub({
      pipelineName: "pump-lakehouse",
      loadBearingPatterns: [/INSERT INTO/i, /datagen/i],
    }),

    // ── Project-root README ───────────────────────────────────────────

    templateReadme({
      templateName: "lakehouse-ingestion",
      tagline:
        "Multi-topic Kafka → Iceberg raw-landing pipeline plus an internal data pump. Three append-only ingest flows fan into one Flink job via `<StatementSet>`, all landing in the `raw` Iceberg database under a REST catalog backed by SeaweedFS S3 storage.",
      pipelines: [
        {
          name: "lakehouse-ingest",
          pitch:
            "Three Kafka topics → three Iceberg `raw.<table>` sinks in one `<StatementSet>` Flink job.",
        },
        {
          name: "pump-lakehouse",
          pitch:
            "Internal DataGen → Kafka pump for events, clickstream, and transactions topics.",
        },
      ],
      prerequisites: [
        "Deploy the Iceberg REST catalog: `kubectl apply -f deploy/minikube/07-iceberg-rest.yaml`",
        "Create the Iceberg `raw` database via the Flink SQL Gateway (the README's Prerequisites section in the scaffolded project shows the exact `CREATE CATALOG ... CREATE DATABASE` block).",
      ],
      gettingStarted: [
        "pnpm install",
        "flink-reactor synth          # Preview generated SQL",
        "flink-reactor deploy --env dev",
      ],
    }),
  ]
}

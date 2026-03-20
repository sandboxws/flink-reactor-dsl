import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getLakehouseAnalyticsTemplates(
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
          iceberg: { databases: ['bronze', 'silver', 'gold'] },
          kafka: { topics: ['orders.cdc'] },
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
      path: "schemas/medallion.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl';

// Bronze: raw CDC events from source systems
export const OrderCdcSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    customerId: Field.STRING(),
    product: Field.STRING(),
    amount: Field.DOUBLE(),
    status: Field.STRING(),
    updatedAt: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ['orderId'] },
  watermark: { column: 'updatedAt', expression: "updatedAt - INTERVAL '5' SECOND" },
});

// Silver: deduplicated, cleaned records
export const OrderCleanSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    customerId: Field.STRING(),
    product: Field.STRING(),
    amount: Field.DOUBLE(),
    status: Field.STRING(),
    updatedAt: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ['orderId'] },
  watermark: { column: 'updatedAt', expression: "updatedAt - INTERVAL '5' SECOND" },
});

// Gold: aggregated summaries
export const RevenueSummarySchema = Schema({
  fields: {
    product: Field.STRING(),
    totalRevenue: Field.DOUBLE(),
    orderCount: Field.BIGINT(),
    windowStart: Field.TIMESTAMP(3),
    windowEnd: Field.TIMESTAMP(3),
  },
});
`,
    },

    // ── Pipeline M1: Bronze — Kafka CDC → Iceberg append (raw) ──────

    {
      path: "pipelines/medallion-bronze/index.tsx",
      content: `import {
  Pipeline,
  KafkaSource,
  IcebergCatalog,
  IcebergSink,
} from '@flink-reactor/dsl';
import { OrderCdcSchema } from '@/schemas/medallion';

const iceberg = IcebergCatalog({
  name: "lakehouse",
  catalogType: "rest",
  uri: "http://iceberg-rest:8181",
});

export default (
  <Pipeline
    name="medallion-bronze"
    mode="streaming"
    parallelism={4}
    stateBackend="rocksdb"
    checkpoint={{ interval: "60s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/medallion-bronze",
      "state.savepoints.dir": "s3://flink-state/savepoints/medallion-bronze",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    {iceberg.node}
    <KafkaSource
      topic="orders.cdc"
      schema={OrderCdcSchema}
      format="debezium-json"
      bootstrapServers="kafka:9092"
      consumerGroup="medallion-bronze"
    />
    {/* Bronze: append-only raw landing, Iceberg format v1 */}
    <IcebergSink
      catalog={iceberg.handle}
      database="bronze"
      table="orders_raw"
      formatVersion={1}
    />
  </Pipeline>
);
`,
    },

    // ── Pipeline M2: Silver — Kafka → Deduplicate → Iceberg upsert ──

    {
      path: "pipelines/medallion-silver/index.tsx",
      content: `import {
  Pipeline,
  KafkaSource,
  IcebergCatalog,
  IcebergSink,
  Deduplicate,
} from '@flink-reactor/dsl';
import { OrderCleanSchema } from '@/schemas/medallion';

const iceberg = IcebergCatalog({
  name: "lakehouse",
  catalogType: "rest",
  uri: "http://iceberg-rest:8181",
});

export default (
  <Pipeline
    name="medallion-silver"
    mode="streaming"
    parallelism={4}
    stateBackend="rocksdb"
    checkpoint={{ interval: "60s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/medallion-silver",
      "state.savepoints.dir": "s3://flink-state/savepoints/medallion-silver",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    {iceberg.node}
    <KafkaSource
      topic="orders.cdc"
      schema={OrderCleanSchema}
      format="debezium-json"
      bootstrapServers="kafka:9092"
      consumerGroup="medallion-silver"
    />
    <Deduplicate key={['orderId']} order="updatedAt" keep="last" />
    {/* Silver: upsert with format v2 for row-level deletes */}
    <IcebergSink
      catalog={iceberg.handle}
      database="silver"
      table="orders_clean"
      primaryKey={['orderId']}
      formatVersion={2}
      upsertEnabled
    />
  </Pipeline>
);
`,
    },

    // ── Pipeline M3: Gold — Kafka → Aggregate → Iceberg summary ─────

    {
      path: "pipelines/medallion-gold/index.tsx",
      content: `import {
  Pipeline,
  KafkaSource,
  IcebergCatalog,
  IcebergSink,
  TumbleWindow,
  Aggregate,
} from '@flink-reactor/dsl';
import { OrderCdcSchema } from '@/schemas/medallion';

const iceberg = IcebergCatalog({
  name: "lakehouse",
  catalogType: "rest",
  uri: "http://iceberg-rest:8181",
});

export default (
  <Pipeline
    name="medallion-gold"
    mode="streaming"
    parallelism={2}
    stateBackend="rocksdb"
    checkpoint={{ interval: "60s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/medallion-gold",
      "state.savepoints.dir": "s3://flink-state/savepoints/medallion-gold",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    {iceberg.node}
    <KafkaSource
      topic="orders.cdc"
      schema={OrderCdcSchema}
      format="debezium-json"
      bootstrapServers="kafka:9092"
      consumerGroup="medallion-gold"
    />
    <TumbleWindow size="1 HOUR" on="updatedAt" />
    <Aggregate
      groupBy={['product']}
      select={{
        product: 'product',
        totalRevenue: 'SUM(amount)',
        orderCount: 'COUNT(*)',
        windowStart: 'WINDOW_START',
        windowEnd: 'WINDOW_END',
      }}
    />
    {/* Gold: append-only summary tables */}
    <IcebergSink
      catalog={iceberg.handle}
      database="gold"
      table="revenue_hourly"
    />
  </Pipeline>
);
`,
    },

    // ── Data Pump ───────────────────────────────────────────────────

    {
      path: "pipelines/pump-medallion/index.tsx",
      content: `import {
  Pipeline,
  DataGenSource,
  KafkaSink,
} from '@flink-reactor/dsl';
import { OrderCdcSchema } from '@/schemas/medallion';

export default (
  <Pipeline
    name="pump-medallion"
    mode="streaming"
    parallelism={2}
    stateBackend="rocksdb"
    checkpoint={{ interval: "60s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/pump-medallion",
      "state.savepoints.dir": "s3://flink-state/savepoints/pump-medallion",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    <DataGenSource schema={OrderCdcSchema} rowsPerSecond={2000} />
    <KafkaSink topic="orders.cdc" bootstrapServers="kafka:9092" />
  </Pipeline>
);
`,
    },

    // ── README ───────────────────────────────────────────────────────

    {
      path: "README.md",
      content: `# Lakehouse Analytics — Medallion Architecture

Bronze → Silver → Gold data pipeline using Apache Iceberg.

## Pipelines

| Pipeline | Layer | Pattern | Iceberg Format |
|----------|-------|---------|----------------|
| **medallion-bronze** | Bronze | Kafka CDC → Iceberg append | v1 (append-only) |
| **medallion-silver** | Silver | Kafka → Deduplicate → Iceberg upsert | v2 (row-level deletes) |
| **medallion-gold** | Gold | Kafka → Aggregate → Iceberg append | v1 (append-only) |
| **pump-medallion** | — | DataGen → Kafka data pump | — |

## Key Concepts

- **Format v1 vs v2**: v1 supports append-only writes; v2 adds equality deletes
  enabling upsert/delete operations via primary keys.
- **Append vs Upsert**: Bronze/Gold use append (immutable audit trail);
  Silver uses upsert (deduplicated current state).

## Prerequisites

1. Deploy the Iceberg REST catalog:
   \`\`\`bash
   kubectl apply -f deploy/minikube/07-iceberg-rest.yaml
   \`\`\`

2. Create Iceberg databases (via Flink SQL Gateway):
   \`\`\`sql
   CREATE CATALOG lakehouse WITH (
     'type' = 'iceberg',
     'catalog-type' = 'rest',
     'uri' = 'http://iceberg-rest:8181'
   );
   USE CATALOG lakehouse;
   CREATE DATABASE IF NOT EXISTS bronze;
   CREATE DATABASE IF NOT EXISTS silver;
   CREATE DATABASE IF NOT EXISTS gold;
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
      path: "tests/pipelines/medallion-bronze.test.ts",
      content: `import { describe, it } from 'vitest';

describe('medallion-bronze pipeline', () => {
  it.todo('synthesizes valid Flink SQL with Iceberg format v1 append sink');
});
`,
    },
  ]
}

import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import {
  pipelineReadme,
  sharedFiles,
  templatePipelineTestStub,
  templateReadme,
} from "./shared.js"

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

  // Bronze/silver/gold lakehouse analytics on Iceberg, fed from Kafka CDC.
  services: { kafka: { bootstrapServers: 'kafka:9092' }, iceberg: {} },

  environments: {
    minikube: {
      cluster: { url: 'http://localhost:8081' },
      sim: {
        init: {
          iceberg: { databases: ['bronze', 'silver', 'gold'] },
          kafka: {
            catalogs: [
              {
                name: 'orders',
                tables: [
                  {
                    table: 'cdc',
                    topic: 'orders.cdc',
                    columns: {
                      orderId: 'STRING',
                      customerId: 'STRING',
                      product: 'STRING',
                      amount: 'DOUBLE',
                      status: 'STRING',
                      updatedAt: 'TIMESTAMP(3)',
                    },
                    format: 'debezium-json',
                    primaryKey: ['orderId'],
                    watermark: { column: 'updatedAt', expression: "updatedAt - INTERVAL '5' SECOND" },
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
  uri: "http://lakekeeper.localtest.me:8181/catalog",
  warehouse: "flink-warehouse",
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
  uri: "http://lakekeeper.localtest.me:8181/catalog",
  warehouse: "flink-warehouse",
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
  uri: "http://lakekeeper.localtest.me:8181/catalog",
  warehouse: "flink-warehouse",
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
        windowStart: 'window_start',
        windowEnd: 'window_end',
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

    // ── Per-pipeline READMEs ──────────────────────────────────────────

    pipelineReadme({
      pipelineName: "medallion-bronze",
      tagline:
        "Bronze layer: raw CDC events landed append-only into Iceberg format v1 (immutable audit trail).",
      demonstrates: [
        '`<KafkaSource format="debezium-json">` consuming CDC retract changelog.',
        "`<IcebergCatalog>` with `catalogType: 'rest'` (Lakekeeper REST catalog).",
        "`<IcebergSink formatVersion={1}>` for append-only landing — every CDC event is preserved as a row, no upserts.",
      ],
      topology: `IcebergCatalog (REST → SeaweedFS)
  └── KafkaSource (orders.cdc, debezium-json)
        └── IcebergSink (bronze.orders_raw, format v1 append)`,
      schemas: [
        "`schemas/medallion.ts` — `OrderCdcSchema` (with `orderId` PK)",
      ],
      runCommand: `pnpm synth
pnpm test`,
    }),
    pipelineReadme({
      pipelineName: "medallion-silver",
      tagline:
        'Silver layer: deduplicated current-state mirror via `<Deduplicate keep="last">` → Iceberg format v2 upsert.',
      demonstrates: [
        '`<Deduplicate key={[\'orderId\']} order="updatedAt" keep="last">` — `ROW_NUMBER()` last-row-per-key pattern.',
        "`<IcebergSink formatVersion={2} upsertEnabled>` with `primaryKey={['orderId']}` — equality deletes give per-key upsert semantics.",
        "Versioned/CDC source replayed into Silver guarantees the table converges to the source's current state.",
      ],
      topology: `IcebergCatalog
  └── KafkaSource (orders.cdc, debezium-json)
        └── Deduplicate (key=orderId, order=updatedAt, keep=last)
              └── IcebergSink (silver.orders_clean, format v2 upsert, PK=orderId)`,
      schemas: ["`schemas/medallion.ts` — `OrderCleanSchema`"],
      runCommand: `pnpm synth
pnpm test`,
    }),
    pipelineReadme({
      pipelineName: "medallion-gold",
      tagline:
        "Gold layer: hourly tumbling-window per-product revenue summaries → Iceberg format v1 (append-only summary table).",
      demonstrates: [
        '`<TumbleWindow size="1 HOUR" on="updatedAt">` for hourly revenue buckets.',
        "`<Aggregate>` computing `SUM(amount)` and `COUNT(*)` per `product` plus `window_start` / `window_end`.",
        "`<IcebergSink>` (default v1 append) — Gold stays append-only because each window is immutable once closed.",
      ],
      topology: `IcebergCatalog
  └── KafkaSource (orders.cdc, debezium-json)
        └── TumbleWindow (1 HOUR, on=updatedAt)
              └── Aggregate (GROUP BY product — SUM, COUNT, window metadata)
                    └── IcebergSink (gold.revenue_hourly, format v1 append)`,
      schemas: [
        "`schemas/medallion.ts` — `OrderCdcSchema` (input), `RevenueSummarySchema` (output shape)",
      ],
      runCommand: `pnpm synth
pnpm test`,
    }),
    pipelineReadme({
      pipelineName: "pump-medallion",
      tagline:
        "Internal data-generator pipeline that pumps synthetic CDC orders onto the `orders.cdc` Kafka topic so the medallion pipelines have data to consume.",
      demonstrates: [
        "`<DataGenSource>` driving a `<KafkaSink>` directly — single-topic pump.",
        "Bundle-internal pump pattern (no upstream Apache Flink source — exists only to feed the three medallion pipelines on the local sim).",
      ],
      topology: `DataGenSource (OrderCdc) ─► KafkaSink (orders.cdc)`,
      schemas: [
        "`schemas/medallion.ts` — same schema all three medallion pipelines read",
      ],
      runCommand: `pnpm synth
pnpm test`,
    }),

    // ── Tests ─────────────────────────────────────────────────────────

    templatePipelineTestStub({
      pipelineName: "medallion-bronze",
      loadBearingPatterns: [
        /iceberg/i,
        /debezium-json/i,
        /'format-version'\s*=\s*'1'/i,
      ],
    }),
    templatePipelineTestStub({
      pipelineName: "medallion-silver",
      loadBearingPatterns: [
        /ROW_NUMBER\(\)/i,
        /iceberg/i,
        /'format-version'\s*=\s*'2'/i,
      ],
    }),
    templatePipelineTestStub({
      pipelineName: "medallion-gold",
      loadBearingPatterns: [/TUMBLE\(/i, /SUM\(/i, /iceberg/i],
    }),
    templatePipelineTestStub({
      pipelineName: "pump-medallion",
      loadBearingPatterns: [/INSERT INTO/i, /datagen/i, /orders\.cdc/],
    }),

    // ── Project-root README ───────────────────────────────────────────

    templateReadme({
      templateName: "lakehouse-analytics",
      tagline:
        "Bronze → Silver → Gold medallion-architecture data pipeline using Apache Iceberg. Bronze (append-only raw CDC), Silver (deduplicated upsert via Iceberg format v2), and Gold (append-only hourly aggregate summaries) demonstrate the canonical lakehouse pattern with one consumer source per layer.",
      pipelines: [
        {
          name: "medallion-bronze",
          pitch: "Kafka CDC → Iceberg format v1 append-only landing.",
        },
        {
          name: "medallion-silver",
          pitch:
            'Kafka CDC → `<Deduplicate keep="last">` → Iceberg format v2 upsert.',
        },
        {
          name: "medallion-gold",
          pitch:
            "Kafka CDC → 1-hour tumbling aggregate → Iceberg format v1 append (revenue_hourly).",
        },
        {
          name: "pump-medallion",
          pitch: "Internal DataGen → Kafka pump for the `orders.cdc` topic.",
        },
      ],
      prerequisites: [
        "Deploy the Iceberg REST catalog: `kubectl apply -f deploy/minikube/07-iceberg-rest.yaml`",
        "Create the `bronze`, `silver`, and `gold` databases via the Flink SQL Gateway (CREATE CATALOG ... CREATE DATABASE block).",
        "Format-v2 vs v1 distinction: v1 = append-only; v2 = adds equality deletes for upsert/delete via primary keys. Bronze and Gold use v1 (immutable history); Silver uses v2 (current-state mirror).",
      ],
      gettingStarted: [
        "pnpm install",
        "flink-reactor synth          # Preview generated SQL",
        "flink-reactor deploy --env dev",
      ],
    }),
  ]
}

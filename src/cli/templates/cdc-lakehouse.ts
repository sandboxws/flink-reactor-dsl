import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import {
  pipelineReadme,
  sharedFiles,
  templatePipelineTestStub,
  templateReadme,
} from "./shared.js"

export function getCdcLakehouseTemplates(
  opts: ScaffoldOptions,
): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "flink-reactor.config.ts",
      content: `import { defineConfig } from '@flink-reactor/dsl';

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  // CDC stream lands in Kafka, lakehouse stores in Iceberg
  // (Lakekeeper-backed Iceberg REST → Postgres pulls in implicitly).
  services: { kafka: {}, iceberg: {} },

  environments: {
    minikube: {
      cluster: { url: 'http://localhost:8081' },
      kafka: { bootstrapServers: 'kafka:9092' },
      sim: {
        init: {
          iceberg: { databases: ['inventory'] },
          kafka: {
            catalogs: [
              {
                name: 'cdc',
                tables: [
                  {
                    table: 'inventory_orders',
                    topic: 'dbserver1.inventory.orders',
                    columns: {
                      orderId: 'BIGINT',
                      customerId: 'BIGINT',
                      product: 'STRING',
                      amount: 'DECIMAL(10, 2)',
                      status: 'STRING',
                      createdAt: 'TIMESTAMP(3)',
                      updatedAt: 'TIMESTAMP(3)',
                    },
                    format: 'debezium-json',
                    primaryKey: ['orderId'],
                  },
                ],
              },
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
      path: "schemas/orders.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl';

export const OrderSchema = Schema({
  fields: {
    orderId: Field.BIGINT(),
    customerId: Field.BIGINT(),
    product: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    status: Field.STRING(),
    createdAt: Field.TIMESTAMP(3),
    updatedAt: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ['orderId'] },
});
`,
    },
    {
      path: "pipelines/cdc-to-lakehouse/index.tsx",
      content: `import { Pipeline, KafkaSource, IcebergCatalog, IcebergSink } from '@flink-reactor/dsl';
import { OrderSchema } from '@/schemas/orders';

// Iceberg REST catalog backed by SeaweedFS S3 storage
const iceberg = IcebergCatalog({
  name: 'lakehouse',
  catalogType: 'rest',
  uri: 'http://lakekeeper.localtest.me:8181/catalog',
  warehouse: 'flink-warehouse',
});

export default (
  <Pipeline
    name="cdc-to-lakehouse"
    mode="streaming"
    parallelism={2}
    stateBackend="rocksdb"
    checkpoint={{ interval: '30s', mode: 'exactly-once' }}
    flinkConfig={{
      's3.endpoint': 'http://seaweedfs.flink-demo.svc:8333',
      's3.path.style.access': 'true',
      's3.access-key': 'admin',
      's3.secret-key': 'admin',
      'state.checkpoints.dir': 's3://flink-state/checkpoints/cdc-to-lakehouse',
      'state.savepoints.dir': 's3://flink-state/savepoints/cdc-to-lakehouse',
    }}
  >
    {iceberg.node}
    <KafkaSource
      topic="dbserver1.inventory.orders"
      schema={OrderSchema}
      format="debezium-json"
      bootstrapServers="kafka:9092"
      consumerGroup="cdc-lakehouse"
    />
    <IcebergSink
      catalog={iceberg.handle}
      database="inventory"
      table="orders"
      primaryKey={['orderId']}
      formatVersion={2}
      upsertEnabled
    />
  </Pipeline>
);
`,
    },
    pipelineReadme({
      pipelineName: "cdc-to-lakehouse",
      tagline:
        "Stream a Debezium order CDC topic into an Iceberg upsert sink (format v2) on a SeaweedFS-backed REST catalog.",
      demonstrates: [
        '`<KafkaSource format="debezium-json">` consuming a CDC retract changelog.',
        "`<IcebergCatalog>` declaration referencing a REST catalog with S3 (SeaweedFS) warehouse storage.",
        "`<IcebergSink>` with `formatVersion={2}` and `upsertEnabled` so primary-key updates produce equality deletes.",
      ],
      topology: `IcebergCatalog (REST → SeaweedFS S3 warehouse)
  └── KafkaSource (dbserver1.inventory.orders, debezium-json)
        └── IcebergSink (lakehouse.inventory.orders, format v2 upsert)`,
      schemas: [
        "`schemas/orders.ts` — `{ orderId, customerId, product, amount, status, createdAt, updatedAt }` with `orderId` as primary key",
      ],
      runCommand: `pnpm synth
pnpm test`,
    }),
    templatePipelineTestStub({
      pipelineName: "cdc-to-lakehouse",
      loadBearingPatterns: [
        /debezium-json/,
        /iceberg/i,
        /'format-version'\s*=\s*'2'/i,
      ],
    }),
    templateReadme({
      templateName: "cdc-lakehouse",
      tagline:
        "End-to-end CDC pipeline: Debezium-formatted Kafka topic → Iceberg upsert sink (format v2). Demonstrates the canonical streaming-CDC-to-lakehouse pattern with row-level deletes via primary-key equality.",
      pipelines: [
        {
          name: "cdc-to-lakehouse",
          pitch:
            "Order CDC stream (debezium-json) → Iceberg upsert table on a SeaweedFS-backed REST catalog.",
        },
      ],
      gettingStarted: ["pnpm install", "pnpm synth", "pnpm test"],
    }),
  ]
}

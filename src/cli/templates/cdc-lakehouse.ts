import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

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

  environments: {
    minikube: {
      cluster: { url: 'http://localhost:8081' },
      kafka: { bootstrapServers: 'kafka:9092' },
      sim: {
        init: {
          iceberg: { databases: ['inventory'] },
          kafka: { topics: ['dbserver1.inventory.orders'] },
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
  uri: 'http://iceberg-rest:8181',
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
    {
      path: "tests/pipelines/cdc-to-lakehouse.test.ts",
      content: `import { describe, it, expect } from 'vitest';
// import { synth } from '@flink-reactor/dsl/testing';

describe('cdc-to-lakehouse pipeline', () => {
  it.todo('synthesizes valid Flink SQL with Debezium source');
});
`,
    },
  ]
}

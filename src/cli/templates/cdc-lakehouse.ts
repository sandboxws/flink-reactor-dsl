import type { ScaffoldOptions, TemplateFile } from '../commands/new.js';
import { sharedFiles } from './shared.js';

export function getCdcLakehouseTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: 'schemas/orders.ts',
      content: `import { Schema, Field } from 'flink-reactor';

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
      path: 'pipelines/cdc-to-lakehouse/index.tsx',
      content: `import { createElement, Pipeline, KafkaSource, PaimonCatalog, PaimonSink } from 'flink-reactor';
import { OrderSchema } from '../../schemas/orders';

const lakehouse = PaimonCatalog({ name: 'lakehouse', warehouse: 's3://my-bucket/warehouse' });

export default (
  <Pipeline name="cdc-to-lakehouse">
    {lakehouse.node}
    <KafkaSource
      topic="dbserver1.inventory.orders"
      schema={OrderSchema}
      format="debezium-json"
      bootstrapServers="localhost:9092"
      consumerGroup="cdc-lakehouse"
    />
    <PaimonSink
      catalog={lakehouse.handle}
      database="inventory"
      table="orders"
      primaryKey={['orderId']}
    />
  </Pipeline>
);
`,
    },
    {
      path: 'tests/pipelines/cdc-to-lakehouse.test.ts',
      content: `import { describe, it, expect } from 'vitest';
// import { synth } from 'flink-reactor/testing';

describe('cdc-to-lakehouse pipeline', () => {
  it.todo('synthesizes valid Flink SQL with Debezium source');
});
`,
    },
  ];
}

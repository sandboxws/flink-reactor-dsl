import type { ScaffoldOptions, TemplateFile } from '../commands/new.js';
import { sharedFiles } from './shared.js';

export function getCdcLakehouseTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: 'schemas/orders.ts',
      content: `import { Schema, Field } from 'flink-reactor';

export const OrderSchema = Schema('orders', {
  orderId: Field.BIGINT(),
  customerId: Field.BIGINT(),
  product: Field.STRING(),
  amount: Field.DECIMAL(10, 2),
  status: Field.STRING(),
  createdAt: Field.TIMESTAMP(3),
  updatedAt: Field.TIMESTAMP(3),
});
`,
    },
    {
      path: 'pipelines/cdc-to-lakehouse/index.tsx',
      content: `import { createElement, Pipeline, KafkaSource, PaimonSink } from 'flink-reactor';
import { OrderSchema } from '../../schemas/orders';

export default (
  <Pipeline name="cdc-to-lakehouse">
    <KafkaSource
      topic="dbserver1.inventory.orders"
      schema={OrderSchema}
      format="debezium-json"
      properties={{ 'bootstrap.servers': 'localhost:9092', 'group.id': 'cdc-lakehouse' }}
    />
    <PaimonSink
      table="lakehouse.orders"
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

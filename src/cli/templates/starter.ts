import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getStarterTemplates(opts: ScaffoldOptions): TemplateFile[] {
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
          kafka: {
            topics: ['in-stock-products'],
            catalogs: [{
              name: 'cdc',
              tables: [{
                table: 'inventory_products',
                topic: 'cdc.inventory.products',
                format: 'debezium-json',
                columns: {
                  id: 'INT',
                  name: 'STRING',
                  category: 'STRING',
                  price: 'DOUBLE',
                  quantity: 'INT',
                },
                primaryKey: ['id'],
              }],
            }],
          },
        },
      },
      pipelines: { '*': { parallelism: 1 } },
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
      path: "schemas/products.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl';

export const ProductSchema = Schema({
  fields: {
    id: Field.INT(),
    name: Field.STRING(),
    category: Field.STRING(),
    price: Field.DOUBLE(),
    quantity: Field.INT(),
  },
  primaryKey: { columns: ['id'] },
});
`,
    },
    {
      path: "pipelines/hello-world/index.tsx",
      content: `import { Pipeline, KafkaSource, KafkaSink, Filter } from '@flink-reactor/dsl';
import { ProductSchema } from '@/schemas/products';

export default (
  <Pipeline name="hello-world">
    <KafkaSource
      topic="cdc.inventory.products"
      schema={ProductSchema}
      format="debezium-json"
      bootstrapServers="kafka:9092"
      consumerGroup="hello-world"
    />
    <Filter condition="quantity > 0" />
    <KafkaSink
      topic="in-stock-products"
      bootstrapServers="kafka:9092"
    />
  </Pipeline>
);
`,
    },
    {
      path: "tests/pipelines/hello-world.test.ts",
      content: `import { describe, it, expect } from 'vitest';
// import { synth } from '@flink-reactor/dsl/testing';

describe('hello-world pipeline', () => {
  it.todo('synthesizes valid Flink SQL');
});
`,
    },
  ]
}

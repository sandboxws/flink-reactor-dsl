import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getStarterTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
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

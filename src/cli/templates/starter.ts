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
    // Docker-compose by default — matches the platform docs' recommended
    // local lane. Override with \`fr up --runtime=minikube\` to exercise the
    // Kubernetes lane without editing config.
    development: {
      runtime: 'docker',
      supportedRuntimes: ['docker', 'minikube'],
      cluster:    { url: 'http://localhost:8081' },
      kafka:      { bootstrapServers: 'localhost:9092' },
      sqlGateway: { url: 'http://localhost:8083' },   // used when runtime=docker
      kubectl:    { context: 'minikube' },             // used when runtime=minikube
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

    // CI / integration tests — full K8s lane on minikube.
    test: {
      runtime: 'minikube',
      kubectl:    { context: 'minikube' },
      kubernetes: { namespace: 'flink-test' },
      kafka:      { bootstrapServers: 'kafka:9092' },
      pipelines:  { '*': { parallelism: 1 } },
    },

    // Pre-prod validation on a remote cluster. \`kubectl.context\` is required —
    // no sensible default exists.
    staging: {
      runtime: 'kubernetes',
      kubectl:    { context: 'staging' },
      kubernetes: { namespace: 'flink-staging' },
      pipelines:  { '*': { parallelism: 2 } },
    },

    production: {
      runtime: 'kubernetes',
      kubectl:    { context: 'production' },
      kubernetes: { namespace: 'flink-prod' },
      pipelines:  { '*': { parallelism: 4 } },
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
